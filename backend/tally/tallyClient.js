/**
 * tallyClient.js
 * ──────────────
 * Connects to TallyPrime HTTP server (default port 9000).
 *
 * Fetches ALL master data visible in Chart of Accounts:
 *   Accounting Masters : Companies, Groups, Ledgers, Voucher Types,
 *                        Cost Categories, Cost Centres, Currencies, Budgets
 *   Inventory Masters  : Stock Groups, Stock Items, Stock Categories,
 *                        Units, Godowns
 *   Transactions       : Vouchers (with date range)
 *
 * All data is normalised into clean JS objects for middleware inspection.
 */

import axios from "axios";
import Agent from "agentkeepalive";
import { parseStringPromise } from "xml2js";
import { config } from "../config/config.js";
import { logger } from "../logs/logger.js";

// ── HTTP agent with keep-alive for localhost Tally ────────────────────────────
const httpAgent = new Agent({
  maxSockets: 10,
  maxFreeSockets: 5,
  timeout: 600_000,          // 10 min — matches agent.cjs
  freeSocketTimeout: 30_000,
});

const tallyAxios = axios.create({
  baseURL: config.tally.url,
  headers: { "Content-Type": "text/xml" },
  timeout: 600_000,          // 10 min — large voucher sets can take several minutes
  httpAgent,
});

// ── XML / value helpers ───────────────────────────────────────────────────────
function escapeXml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function val(v) {
  if (v === undefined || v === null) return null;
  if (Array.isArray(v)) return val(v[0]);
  if (typeof v === "object" && v._) return cleanVal(v._);
  if (typeof v === "string" || typeof v === "number") return cleanVal(String(v));
  return null;
}
// Strip Tally control characters (EOT and other non-printable chars) that
// survive xml2js parsing and break string comparisons (e.g. parent=" Primary").
function cleanVal(s) {
  if (!s) return s;
  // Filter chars using charCodeAt — avoids regex Unicode escape encoding issues.
  // Removes: ASCII control chars (0-31, 127), zero-width spaces (8203-8205), BOM (65279).
  let out = "";
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c <= 31 || c === 127 || c === 8203 || c === 8204 || c === 8205 || c === 65279) continue;
    out += s[i];
  }
  return out.trim();
}

function extractAddress(addr) {
  if (!addr) return null;
  if (Array.isArray(addr))
    return addr.map((a) => (typeof a === "object" ? a._ : a)).filter(Boolean).join(", ");
  if (typeof addr === "object" && addr._) return addr._;
  return String(addr);
}

function extractPhone(blob) {
  if (!blob) return null;
  const m = String(blob).match(/\b[6-9]\d{9}\b/g);
  return m ? m[0] : null;
}

function extractEmail(blob) {
  if (!blob) return null;
  const m = String(blob).match(/[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}/i);
  return m ? m[0] : null;
}

function parseTallyAmount(raw) {
  if (!raw) return 0;
  const str = String(raw).replace(/[, ]/g, "");
  const n = parseFloat(str);
  return isNaN(n) ? 0 : Math.abs(n);
}

// FIX: Signed variant — preserves Dr/Cr sign from Tally balance fields.
// Tally exports OPENINGBALANCE/CLOSINGBALANCE as XML nodes that can have two forms:
//   Form A: plain string  "-4274515.00"  (negative = Dr/asset)
//   Form B: object with _ = "4274515.00" and $ = { TYPE: "Dr" } or { TYPE: "Cr" }
// parseTallyAmountSigned handles both forms:
//   • Form A: parseFloat preserves the minus sign directly.
//   • Form B: read the absolute value from _, then negate if TYPE="Dr" (Dr = asset = debit = negative in Tally).
// The OB sync uses bal < 0 to decide debit vs credit — the sign MUST be correct.
// Use ONLY for balance fields (openingBalance, closingBalance on ledgers).
// Strip UoM suffixes from Tally quantity/rate fields.
// Tally exports: "1 Nos", "79.00 /Nos" — parseFloat stops at the first non-numeric char.
function parseTallyQty(raw) {
  if (!raw) return 1;
  const n = parseFloat(String(raw));
  return isNaN(n) || n === 0 ? 1 : Math.abs(n);
}
function parseTallyRate(raw) {
  if (!raw) return 0;
  const n = parseFloat(String(raw));
  return isNaN(n) ? 0 : Math.abs(n);
}

// All money/amount fields on vouchers remain unsigned (use parseTallyAmount).
function parseTallyAmountSigned(raw) {
  if (!raw) return 0;
  // Form B: Tally exports CLOSINGBALANCE as { _: "4274515.00", $: { TYPE: "Dr" } }
  // The TYPE attribute tells us whether it is a Debit (asset) or Credit (liability).
  // Debit (Dr) = asset/debtor → negative in our convention (bal < 0 = debit).
  // Credit (Cr) = liability/creditor → positive in our convention (bal > 0 = credit).
  if (typeof raw === "object" && raw !== null && raw._) {
    const str = String(raw._).replace(/[, ]/g, "");
    const n = parseFloat(str);
    if (isNaN(n) || n === 0) return 0;
    // Determine sign from $ attributes — try TYPE, then ISDEEMEDPOSITIVE, then DR flag
    const attrs = raw.$ || {};
    const type  = (attrs.TYPE || attrs.type || "").trim().toLowerCase();
    const isDr  = type === "dr" || type === "debit" ||
                  (attrs.ISDEEMEDPOSITIVE || "").toLowerCase() === "no";
    // If no attribute found, fall back to the sign of n (Form A inside object)
    if (type || attrs.ISDEEMEDPOSITIVE) {
      return isDr ? -Math.abs(n) : Math.abs(n);
    }
    return n; // no attribute — trust the raw sign (should be already signed)
  }
  // Form A: plain string or number, possibly already signed ("-4274515.00" = Dr)
  const str = String(raw).replace(/[, ]/g, "");
  const n = parseFloat(str);
  return isNaN(n) ? 0 : n;
}

function tallyDateToISO(raw) {
  if (!raw || String(raw).length < 8) return null;
  const s = String(raw).replace(/-/g, "");
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
}

// ── Reusable collection XML builder ──────────────────────────────────────────
function buildCollectionXml(collectionName, type, fetch, companyName, extra = "") {
  const companyTag = companyName
    ? `<SVCURRENTCOMPANY>${escapeXml(companyName)}</SVCURRENTCOMPANY>`
    : "";
  return `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>${collectionName}</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    ${companyTag}
    ${extra}
   </STATICVARIABLES>
   <TDL>
    <TDLMESSAGE>
     <COLLECTION NAME="${collectionName}">
      <TYPE>${type}</TYPE>
      <FETCH>${fetch}</FETCH>
     </COLLECTION>
    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>`;
}

// ── XML POST ──────────────────────────────────────────────────────────────────
async function postXml(xml) {
  const res = await tallyAxios.post("", xml);
  return res.data;
}

async function parseXml(raw, explicitArray = true) {
  return parseStringPromise(raw, { explicitArray });
}

// ── Generic list extractor (handles both array and non-array parse modes) ─────
function extractList(parsed, ...keys) {
  // Walk the key path; the last key is the item tag name
  let node = parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION;
  if (!node) {
    // Try explicitArray path
    node = parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0];
  }
  if (!node) return [];
  const raw = node[keys[0]] ?? node[keys[1]] ?? [];
  return Array.isArray(raw) ? raw : [raw];
}

// ═════════════════════════════════════════════════════════════════════════════
// 1. PING / CONNECTION CHECK
// ═════════════════════════════════════════════════════════════════════════════
export async function pingTally() {
  const xml = buildCollectionXml("Company Collection", "Company", "NAME");
  const start = Date.now();
  try {
    const raw = await postXml(xml);
    
    const latencyMs = Date.now() - start;
    if (!raw || typeof raw !== "string") throw new Error("Empty response from Tally");
    return { connected: true, latencyMs, url: config.tally.url };
  } catch (err) {
    return {
      connected: false,
      error:
        err.code === "ECONNREFUSED"
          ? `Tally not running on ${config.tally.url}. Open TallyPrime → Gateway of Tally → Enable Tally.NET → HTTP Port 9000.`
          : err.message,
      url: config.tally.url,
    };
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// 2. COMPANIES
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyCompanies() {
  logger.info("Fetching companies from Tally");
  const xml = buildCollectionXml(
    "Company Collection",
    "Company",
    "NAME,GUID,STARTINGFROM,BOOKSBEGINNINGFROM,COUNTRYNAME,STATENAME,GSTIN,INCOMETAXNUMBER"
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, true);
  const companies =
    parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.COMPANY || [];

  return companies
    .map((c) => ({
      guid: val(c.GUID),
      name: val(c.NAME),
      startingFrom: tallyDateToISO(val(c.STARTINGFROM)),
      booksFrom: tallyDateToISO(val(c.BOOKSBEGINNINGFROM)),
      country: val(c.COUNTRYNAME),
      state: val(c.STATENAME),
      gstin: val(c.GSTIN),
      pan: val(c.INCOMETAXNUMBER),
    }))
    .filter((c) => c.name);
}

// ═════════════════════════════════════════════════════════════════════════════
// 3. ACCOUNTING GROUPS  (Accounting Masters → Groups)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyGroups(companyName) {
  logger.info("Fetching groups from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "GroupCollection",
    "Group",
    "NAME,GUID,PARENT,ALTERID,ISREVENUE,ISBILLWISEON,AFFECTSSTOCK,ISSUBLEDGER,BASICGROUPISCALCULABLE",
    companyName
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, false);
  const rawGroups =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.GROUP || [];
  const arr = Array.isArray(rawGroups) ? rawGroups : [rawGroups];

  const groups = arr
    .map((g) => {
      const name = g.$?.NAME || val(g.NAME) || null;
      if (!name) return null;
      return {
        guid: val(g.GUID),
        name,
        alterId: val(g.ALTERID) || null,
        parent: val(g.PARENT) || "Primary",
        isRevenue: val(g.ISREVENUE) === "Yes",
        isBillwise: val(g.ISBILLWISEON) === "Yes",
        affectsStock: val(g.AFFECTSSTOCK) === "Yes",
        isSubledger: val(g.ISSUBLEDGER) === "Yes",
      };
    })
    .filter(Boolean);

  logger.success(`Fetched ${groups.length} groups`, { company: companyName });
  return groups;
}

// ═════════════════════════════════════════════════════════════════════════════
// 4. LEDGERS  (Accounting Masters → Ledgers)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyLedgers(companyName) {
  logger.info("Fetching ledgers from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "Ledger Collection",
    "Ledger",
    "NAME,GUID,PARENT,ALTERID,ADDRESS,ADDRESS.LISTADDRESS.LIST.STATENAME,ADDRESS.LIST.PINCODE,MAILINGNAME,MAILINGDETAILS.LIST,MAILINGDETAILS.LIST.STATENAME,MAILINGDETAILS.LIST.PINCODE,MAILINGDETAILS.LIST.COUNTRYNAME,STATE,STATENAME,OPENINGBALANCE,CLOSINGBALANCE,ISBILLWISEON,LEDGERPHONE,LEDGERMOBILE,EMAIL,INCOMETAXNUMBER,PARTYGSTIN,LEDGSTREGDETAILS.LIST,COUNTRYNAME,PINCODE,BANKDETAILS,BANKDETAILS.LIST,IFSCODE,SWIFTCODE,BANKINGCONFIGBANK,BANKACCHOLDERNAME,BRANCHNAME,BANKBSRCODE,MSMEREGISTRATIONDETAILS.LIST.MSMEENTERPRISETYPE,MSMEREGISTRATIONDETAILS.LIST.UDYAMREGISTRATIONNUMBER,MSMEREGISTRATIONDETAILS.LIST.MSMEACTIVITYTYPE,CREDITLIMIT,ISCREDITCHECK,CREDITDAYS,ISOVERRIDECREDITLIMIT,LANGUAGENAME.LIST,LANGUAGENAME.LIST.LANGUAGEALIASLABEL,UPIID,ACCOUNTNUMBER,BANKNAME,TRANSACTIONTYPE",
    companyName
  );
  const raw = await postXml(xml);



  const parsed = await parseXml(raw, false);

  const rawLedgers =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.LEDGER || [];
  const ledgersArr = Array.isArray(rawLedgers) ? rawLedgers : [rawLedgers];

  const ledgers = [];
  for (const l of ledgersArr) {

   


    const name = l.$?.NAME || val(l.NAME) || null;
    
    const guid = val(l.GUID) || null;
    if (!name || !guid) continue;

    // Extract phone/email from their dedicated Tally fields only.
    // Do NOT mix address text into a blob — address strings can accidentally
    // match phone/email regex patterns and produce false contact data.
    const rawPhone = val(l.LEDGERMOBILE) || val(l.LEDGERPHONE) || null;
    const rawEmail = val(l.EMAIL) || null;

    // ── Capture any extra fields Tally sends that we don't explicitly map ──────
    // If a client's Tally has custom fields (e.g. CREDITLIMIT, TRANSPORTERNAME),
    // they arrive as extra XML keys. We collect them here so they are not silently
    // lost — Erpnextclient.js will map them to custom_* fields in ERPNext.
const KNOWN_LEDGER_KEYS = new Set([
      "$", "GUID", "NAME", "PARENT", "ALTERID", "ADDRESS", "MAILINGNAME",
      "MAILINGDETAILS.LIST", "MAILINGDETAILS", "OPENINGBALANCE", "CLOSINGBALANCE",
      "ISBILLWISEON", "LEDGERPHONE", "LEDGERMOBILE", "EMAIL", "INCOMETAXNUMBER",
      "GSTIN.LIST", "PARTYGSTIN", "LEDGSTREGDETAILS.LIST", "COUNTRYNAME", "STATENAME", "PINCODE", "BANKDETAILS",
      "IFSCODE", "SWIFTCODE", "BANKINGCONFIGBANK", "BANKACCHOLDERNAME", "BRANCHNAME", "BANKBSRCODE",
      "MSMEREGISTRATIONDETAILS.LIST",
      "MSMEREGISTRATIONDETAILS.LIST.ENTERPRISETYPE",
      "MSMEREGISTRATIONDETAILS.LIST.UDYAMREGNUMBER",
      "MSMEREGISTRATIONDETAILS.LIST.MSMEACTIVITYTYPE",
     "CREDITLIMIT",
"CREDITPERIOD",
"ISCREDITCHECK",
"ISOVERRIDECREDITLIMIT", 
 "ALIAS", 
 "LANGUAGEINFOLIST.LIST",
"LANGUAGEINFOLIST.LIST.LANGUAGEALIASLABEL",
"ADDRESS.LIST",
"LANGUAGENAME.LIST",   
"BANKDETAILS.LIST",
    
    ]);
    const customFields = {};
    for (const key of Object.keys(l)) {
      if (!KNOWN_LEDGER_KEYS.has(key)) {
        const v2 = val(l[key]);
        if (v2 !== null && v2 !== undefined && v2 !== "") {
          customFields[key] = v2;
        }
      }
    }
    
  



    ledgers.push({
      guid,
      name,
      alterId: val(l.ALTERID) || null,
      parentGroup: val(l.PARENT) || "Sundry Debtors",
      openingBalance: parseTallyAmountSigned(l.OPENINGBALANCE?._ || l.OPENINGBALANCE),
      closingBalance: parseTallyAmountSigned(l.CLOSINGBALANCE?._ || l.CLOSINGBALANCE),
      type: val(l.ISBILLWISEON) === "Yes" ? "Party" : "General",
      phone: extractPhone(rawPhone),
      email: extractEmail(rawEmail),
      pan: val(l.INCOMETAXNUMBER) || null,
      gstin: (() => {
        // Try 1: PARTYGSTIN — simplest direct field, works on most Tally versions
        const direct = val(l.PARTYGSTIN);
        if (direct && direct.trim().length > 5) return direct.trim();

        // Try 2: LEDGSTREGDETAILS.LIST — Tax Registration Details list (Tally Prime)
        const reg = l["LEDGSTREGDETAILS.LIST"];
        if (reg) {
          const regArr = Array.isArray(reg) ? reg : [reg];
          for (const item of regArr) {
            if (!item) continue;
            const list = item.LIST;
            if (list) {
              const listArr = Array.isArray(list) ? list : [list];
              for (const li of listArr) {
                const v = val(li?.GSTIN) || val(li?.GSTREGISTRATIONNUMBER) || null;
                if (v && v.trim().length > 5) return v.trim();
              }
            }
            const v = val(item.GSTIN) || val(item.GSTREGISTRATIONNUMBER) || null;
            if (v && v.trim().length > 5) return v.trim();
          }
        }

        // Try 3: Legacy GSTIN.LIST (older Tally versions)
        const g = l["GSTIN.LIST"];
        if (g) {
          const arr = Array.isArray(g) ? g : [g];
          for (const item of arr) {
            if (!item) continue;
            if (typeof item === "string" && item.trim().length > 5) return item.trim();
            const list = item.LIST;
            if (list) {
              const listArr = Array.isArray(list) ? list : [list];
              for (const li of listArr) {
                const v = val(li?.GSTIN) || val(li?._) || null;
                if (v && v.trim().length > 5) return v.trim();
              }
            }
            const v = val(item.GSTIN) || val(item._) || null;
            if (v && v.trim().length > 5) return v.trim();
          }
        }

        return null;
      })(),

registrationType: (() => {
  const reg = l["LEDGSTREGDETAILS.LIST"];
  if (!reg) return null;

  const arr = Array.isArray(reg)
    ? reg
    : [reg];

  for (const item of arr) {
    if (!item) continue;

    const v =
      val(item.GSTREGISTRATIONTYPE) ||
      val(item.REGISTRATIONTYPE);

    if (v && v.trim()) {
      return v.trim();
    }
  }

  return null;
})(),

placeOfSupply: (() => {
  const reg = l["LEDGSTREGDETAILS.LIST"];
  if (!reg) return null;

  const arr = Array.isArray(reg)
    ? reg
    : [reg];

  for (const item of arr) {
    if (!item) continue;

    const v =
      val(item.PLACEOFSUPPLY) ||
      val(item.PPLACEOFSUPPLY);

    if (v && v.trim()) {
      return v.trim();
    }
  }

  return null;
})(),


placeOfSupply: (() => {
  const gst = l["LEDGSTREGDETAILS.LIST"];
  if (!gst) return null;

  const arr = Array.isArray(gst)
    ? gst
    : [gst];

  for (const item of arr) {
    if (!item) continue;

    const place =
      val(item.PLACEOFSUPPLY) ||
      val(item.PPLACEOFSUPPLY);

    if (place && place.trim()) {
      return place.trim();
    }
  }

  return null;
})(),



isTransporter: (() => {
  const gst = l["LEDGSTREGDETAILS.LIST"];
  if (!gst) return "Undefined";

  const arr = Array.isArray(gst)
    ? gst
    : [gst];

  for (const item of arr) {
    if (!item) continue;

    const v = val(item.ISTRANSPORTER);

    if (v && v.trim()) {
      return v.trim();
    }
  }

  return "Undefined";
})(),

transporterId: (() => {
  const gst = l["LEDGSTREGDETAILS.LIST"];
  if (!gst) return "Undefined";

  const arr = Array.isArray(gst)
    ? gst
    : [gst];

  for (const item of arr) {
    if (!item) continue;

    const v = val(item.TRANSPORTERID);

    if (v && v.trim()) {
      return v.trim();
    }
  }

  return "Undefined";
})(),

bankTransactionType: (() => {
  const b =
    l["BANKDETAILS.LIST"] ||
    l.BANKDETAILS ||
    {};

  return (
    val(b.TRANSACTIONTYPE) ||
    null
  );
})(),

bankDetailsRaw: (() => {
  return l["BANKDETAILS.LIST"] || null;
})(),

upiId: (() => {
  const b =
    l["BANKDETAILS.LIST"] ||
    l.BANKDETAILS ||
    {};

  return (
    val(b.UPIID) ||
    null
  );
})(),

popupBankAccountNo: (() => {
  const b =
    l["BANKDETAILS.LIST"] ||
    l.BANKDETAILS ||
    {};

  return (
    val(b.ACCOUNTNUMBER) ||
    val(b.ACNO) ||
    null
  );
})(),

popupIfscCode: (() => {
  const b =
    l["BANKDETAILS.LIST"] ||
    l.BANKDETAILS ||
    {};

  return (
    val(b.IFSCODE) ||
    null
  );
})(),

popupBankName: (() => {
  const b =
    l["BANKDETAILS.LIST"] ||
    l.BANKDETAILS ||
    {};

  return (
    val(b.BANKNAME) ||
    null
  );
})(),

  state: (() => {
  const possible = [];

  // 1. Direct top-level fields (usually company state, but try anyway)
  possible.push(val(l.STATENAME), val(l.STATE), val(l.LEDSTATENAME));

  // 2. MAILINGDETAILS.LIST — this is where TallyPrime stores ledger mailing state
  const mailing = l["MAILINGDETAILS.LIST"] || l.MAILINGDETAILS;
  if (mailing) {
    const arr = Array.isArray(mailing) ? mailing : [mailing];
    for (const m of arr) {
      if (!m || typeof m !== "object") continue;
      // Try all known state field names TallyPrime uses
      possible.push(
        val(m.STATENAME),
        val(m["MAILINGDETAILS.LIST.STATENAME"]),
        val(m.STATE),
        val(m.LEDSTATENAME)
      );
    }
  }

  // 3. ADDRESS.LIST
  const addrList = l["ADDRESS.LIST"];
  if (addrList) {
    const arr = Array.isArray(addrList) ? addrList : [addrList];
    for (const a of arr) {
      if (!a || typeof a !== "object") continue;
      possible.push(val(a.STATENAME), val(a.STATE));
    }
  }

  // 4. Filter and return first valid value
  for (const s of possible) {
    const clean = String(s || "")
      .replace(/[♦◆\u0004\u2297\u2666\u25c6*]/g, "")
      .trim();
    if (clean && clean.toLowerCase() !== "not applicable" && clean.length > 1) {
      return clean;
    }
  }
  return null;
})(),

pincode: (() => {
  // Try direct field first
  const direct = val(l.PINCODE);
  if (direct && direct.trim()) return direct.trim();
  
  // Then check MAILINGDETAILS.LIST
  const mailing = l["MAILINGDETAILS.LIST"] || l.MAILINGDETAILS;
  if (mailing) {
    const arr = Array.isArray(mailing) ? mailing : [mailing];
    for (const m of arr) {
      if (!m) continue;
      const v = val(m.PINCODE) || val(m["MAILINGDETAILS.LIST.PINCODE"]);
      if (v && v.trim()) return v.trim();
    }
  }
  return null;
})(),
      bankAccount: val
      (l.BANKDETAILS)       || null,   // Tally XML: BANKDETAILS (was BANKACCNO)
      ifsc:        val(l.IFSCODE)           || null,   // Tally XML: IFSCODE ✓
      swiftCode:   val(l.SWIFTCODE)         || null,   // Tally XML: SWIFTCODE ✓
      bankName:    val(l.BANKINGCONFIGBANK) || null,   // Tally XML: BANKINGCONFIGBANK (was BANKNAME)
      holderName:  val(l.BANKACCHOLDERNAME) || null,   // Tally XML: BANKACCHOLDERNAME (was ACHOLDERNAM)
      bankBranch:  val(l.BRANCHNAME)        || null,   // Tally XML: BRANCHNAME (was BANKBRANCH)
      bsrCode:     val(l.BANKBSRCODE)       || null,   // Tally XML: BANKBSRCODE (was BSRCODE)
      msmeType: (() => {
  const m = l["MSMEREGISTRATIONDETAILS.LIST"];
  if (!m) return null;
  const arr = Array.isArray(m) ? m : [m];
  for (const item of arr) {
    if (!item) continue;
    const v = val(item.ENTERPRISETYPE);
    if (v && v.trim().toLowerCase() !== "not applicable") return v.trim();
  }
  return null;
})(),
udyamNo: (() => {
  const m = l["MSMEREGISTRATIONDETAILS.LIST"];
  if (!m) return null;
  const arr = Array.isArray(m) ? m : [m];
  for (const item of arr) {
    if (!item) continue;
    const v = val(item.UDYAMREGNUMBER);
    if (v && v.trim()) return v.trim();
  }
  return null;
})(),
msmeActivity: (() => {
  const m = l["MSMEREGISTRATIONDETAILS.LIST"];
  if (!m) return null;
  const arr = Array.isArray(m) ? m : [m];
  for (const item of arr) {
    if (!item) continue;
    const v = val(item.MSMEACTIVITYTYPE);
    if (v && v.trim().toLowerCase() !== "not applicable") return v.trim();
  }
  return null;
})(),

creditLimit: parseTallyAmount(l.CREDITLIMIT?._ || l.CREDITLIMIT) || 0,
creditDays:
  val(l.DEFAULTCREDITPERIOD) ||
  val(l.CREDITPERIOD) ||
  val(l.CREDITDAYS) ||
  "",
isCreditCheck:
  String(
    val(l.CHECKFORCREDITDAYS) ||
    val(l.ISCREDITCHECK) ||
    "No"
  ).trim(),
isOverrideCreditLimit:
  String(
    val(l.OVERRIDECREDITLIMIT) ||
    val(l.ISOVERRIDECREDITLIMIT) ||
    "No"
  ).trim(),
      country: (() => {

  const possible = [
    l.COUNTRYNAME,
    l.COUNTRY,
    l.TERRITORYNAME,
    l.TERRITORY
  ];

  for (let c of possible) {

    if (typeof c === "object" && c._) {
      c = c._;
    }

    c = String(c || "").trim();

    if (
      c &&
      c.toLowerCase() !== "not applicable"
    ) {
      return c;
    }
  }

  return null;

})(),


      address: (() => {
  // Try MAILINGDETAILS.LIST first (multi-line address)
  const md = l["MAILINGDETAILS.LIST"] || l.MAILINGDETAILS;
  if (md) {
    const arr = Array.isArray(md) ? md : [md];
    const lines = arr.flatMap((m) => {
      if (!m || typeof m !== "object") return [];
      const list = m.LIST || m.ADDRESSLIST || m["MAILINGDETAILS.LIST"];
      if (list) {
        const la = Array.isArray(list) ? list : [list];
        return la.map((x) => val(x.ADDRESS || x) || "").filter(Boolean);
      }
      return [val(m.ADDRESS || m.NAME) || ""].filter(Boolean);
    });
    if (lines.length > 0) return lines.join(", ");
  }

  // FIX: Try ADDRESS.LIST (Tally Prime structure)
  // XML: <ADDRESS.LIST><ADDRESS TYPE="String">123 Test Street</ADDRESS></ADDRESS.LIST>
  // xml2js parses this as: { "$": { TYPE: "String" }, ADDRESS: { "_": "123 Test Street", "$": {...} } }
  const addrList = l["ADDRESS.LIST"];
  if (addrList) {
    const arr = Array.isArray(addrList) ? addrList : [addrList];
    const lines = arr.flatMap((item) => {
      if (!item || typeof item !== "object") return [];
      // item.ADDRESS can be: string, { _: "value" }, or array of those
      const addrField = item.ADDRESS;
      if (!addrField) return [];
      if (Array.isArray(addrField)) {
        return addrField.map((a) => (typeof a === "object" ? a._ : a)).filter(Boolean);
      }
      if (typeof addrField === "object" && addrField._) return [addrField._];
      if (typeof addrField === "string") return [addrField];
      return [];
    }).filter(Boolean);
    if (lines.length > 0) return lines.join(", ");
  }

  // Fallback: flat ADDRESS field
  return extractAddress(l.ADDRESS) || null;
})(),

hsnCode: (() => {
  const gst = l["LEDGSTREGDETAILS.LIST"];
  if (!gst) return null;

  const arr = Array.isArray(gst) ? gst : [gst];

  for (const item of arr) {
    if (!item) continue;

    const code =
      val(item.HSNCODE) ||
      val(item.GSTHSNNAME) ||
      val(item.SACCODE);

    if (code && code.trim()) {
      return code.trim();
    }
  }

  return null;
})(),

gstRate: (() => {
  const gst = l["LEDGSTREGDETAILS.LIST"];
  if (!gst) return null;

  const arr = Array.isArray(gst) ? gst : [gst];

  for (const item of arr) {
    if (!item) continue;

    const rate =
      val(item.GSTRATE) ||
      val(item.GSTREGRATE);

    if (rate) {
      const parsed = parseFloat(
        String(rate).replace(/[^0-9.]/g, "")
      );

      if (!isNaN(parsed)) {
        return parsed;
      }
    }
  }

  return null;
})(),

taxability: (() => {
  const gst = l["LEDGSTREGDETAILS.LIST"];
  if (!gst) return null;

  const arr = Array.isArray(gst) ? gst : [gst];

  for (const item of arr) {
    if (!item) continue;

    const tax =
      val(item.TAXABILITY);

    if (tax && tax.trim()) {
      return tax.trim();
    }
  }

  return null;
})(),
////////////////////
gstRegistrationType: (() => {
  const gst = l["LEDGSTREGDETAILS.LIST"];
  if (!gst) return null;

  const arr = Array.isArray(gst)
    ? gst
    : [gst];

  for (const item of arr) {
    if (!item) continue;

    const reg =
      val(item.GSTREGISTRATIONTYPE) ||
      val(item.REGISTRATIONTYPE);

    if (reg && reg.trim()) {
      return reg.trim();
    }
  }

  return null;
})(),
/////////////

placeOfSupply: (() => {
  const gst = l["LEDGSTREGDETAILS.LIST"];
  if (!gst) return null;

  const arr = Array.isArray(gst)
    ? gst
    : [gst];

  for (const item of arr) {
    if (!item) continue;

    const place =
      val(item.PLACEOFSUPPLY) ||
      val(item.PPLACEOFSUPPLY);

    if (place && place.trim()) {
      return place.trim();
    }
  }

  return null;
})(),
////////////

typeOfSupply: (() => {
  const gst = l["LEDGSTREGDETAILS.LIST"];
  if (!gst) return null;

  const arr = Array.isArray(gst) ? gst : [gst];

  for (const item of arr) {
    if (!item) continue;

    const supply =
      val(item.GSTTYPEOFSUPPLY) ||
      val(item.TYPEOFSUPPLY);

    if (supply && supply.trim()) {
      return supply.trim();
    }
  }

  return null;
})(),
      
      alias: (() => {
  const langList = l["LANGUAGENAME.LIST"];
  if (!langList) return null;
  const nameList = langList["NAME.LIST"];
  if (!nameList) return null;
  const nameField = nameList.NAME;
  // NAME is an array when alias exists: ["TestCustomer4", "Project"]
  if (Array.isArray(nameField) && nameField.length >= 2) {
    const a = nameField[1];
    if (a && String(a).trim()) return String(a).trim();
  }
  return null;
})(),
      customFields: Object.keys(customFields).length > 0 ? customFields : undefined,
    });
  }

   


  logger.success(`Fetched ${ledgers.length} ledgers`, { company: companyName });
  return ledgers;
}

// ═════════════════════════════════════════════════════════════════════════════
// 5. VOUCHER TYPES  (Accounting Masters → Voucher Types)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyVoucherTypes(companyName) {
  logger.info("Fetching voucher types from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "VoucherTypeCollection",
    "VoucherType",
    "NAME,GUID,PARENT,BASEVOUCHERTYPE,NUMBERINGMETHOD,ISOPTIONAL,ISACTIVE,COMMONNARRATION,AFFECTSSTOCK",
    companyName
  );
  const raw = await postXml(xml);
  
  const parsed = await parseXml(raw, false);
  const rawVT =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.VOUCHERTYPE ||
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.["VOUCHER-TYPE"] || [];
  const arr = Array.isArray(rawVT) ? rawVT : [rawVT];

  const types = arr
    .map((v) => {
      const name = v.$?.NAME || val(v.NAME) || null;
      if (!name) return null;
      return {
        guid: val(v.GUID),
        name,
        parent:    val(v.PARENT),
        baseType:  val(v.BASEVOUCHERTYPE) || val(v.PARENT) || name, // base standard type
        numberingMethod: val(v.NUMBERINGMETHOD),
        isOptional: val(v.ISOPTIONAL) === "Yes",
        isActive: val(v.ISACTIVE) !== "No",
        commonNarration: val(v.COMMONNARRATION) === "Yes",
        affectsStock: val(v.AFFECTSSTOCK) === "Yes",
      };
    })
    .filter(Boolean);

  logger.success(`Fetched ${types.length} voucher types`, { company: companyName });
  return types;
}

// ═════════════════════════════════════════════════════════════════════════════
// 6. COST CATEGORIES  (Accounting Masters → Cost Categories)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyCostCategories(companyName) {
  logger.info("Fetching cost categories from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "CostCategoryCollection",
    "Cost Category",
    "NAME,GUID,ALLOCATEREVENUE,ALLOCATENONREVENUE",
    companyName
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, false);
  const rawCC =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.["COST-CATEGORY"] ||
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.COSTCATEGORY || [];
  const arr = Array.isArray(rawCC) ? rawCC : [rawCC];

  const cats = arr
    .map((c) => {
      const name = c.$?.NAME || val(c.NAME) || null;
      if (!name) return null;
      return {
        guid: val(c.GUID),
        name,
        allocateRevenue: val(c.ALLOCATEREVENUE) === "Yes",
        allocateNonRevenue: val(c.ALLOCATENONREVENUE) === "Yes",
      };
    })
    .filter(Boolean);

  logger.success(`Fetched ${cats.length} cost categories`, { company: companyName });
  return cats;
}

// ═════════════════════════════════════════════════════════════════════════════
// 7. COST CENTRES  (Accounting Masters → Cost Centres)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyCostCentres(companyName) {
  logger.info("Fetching cost centres from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "CostCentreCollection",
    "Cost Centre",
    "NAME,GUID,PARENT,ALTERID,CATEGORY",
    companyName
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, false);
  const rawCentre =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.["COST-CENTRE"] ||
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.COSTCENTRE || [];
  const arr = Array.isArray(rawCentre) ? rawCentre : [rawCentre];

  const centres = arr
    .map((c) => {
      const name = c.$?.NAME || val(c.NAME) || null;
      if (!name) return null;
      return {
        guid:     val(c.GUID),
        name,
        parent:   (val(c.PARENT) || "Primary").trim(),
        category: val(c.CATEGORY) || null,
        alterId:  val(c.ALTERID)  || null,
      };
    })
    .filter(Boolean);

  logger.success(`Fetched ${centres.length} cost centres`, { company: companyName });
  return centres;
}

// ═════════════════════════════════════════════════════════════════════════════
// 8. CURRENCIES  (Accounting Masters → Currencies)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyCurrencies(companyName) {
  logger.info("Fetching currencies from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "CurrencyCollection",
    "Currency",
    "NAME,GUID,MAILINGNAME,ISSUFFIX,INWORDSSUBUNITS,DECIMALPLACES,EXPANSSYMBOL",
    companyName
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, false);
  const rawCur =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.CURRENCY || [];
  const arr = Array.isArray(rawCur) ? rawCur : [rawCur];

  const currencies = arr
    .map((c) => {
      const name = c.$?.NAME || val(c.NAME) || null;
      if (!name) return null;
      return {
        guid: val(c.GUID),
        name,
        mailingName: val(c.MAILINGNAME),
        symbol: val(c.EXPANSSYMBOL),
        isSuffix: val(c.ISSUFFIX) === "Yes",
        decimalPlaces: parseInt(val(c.DECIMALPLACES) || "2", 10),
        inWordsSubUnits: val(c.INWORDSSUBUNITS),
      };
    })
    .filter(Boolean);

  logger.success(`Fetched ${currencies.length} currencies`, { company: companyName });
  return currencies;
}

// ═════════════════════════════════════════════════════════════════════════════
// 9. BUDGETS  (Accounting Masters → Budgets)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyBudgets(companyName) {
  logger.info("Fetching budgets from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "BudgetCollection",
    "Budget",
    "NAME,GUID,PARENT,STARTDATE,ENDDATE",
    companyName
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, false);
  const rawBudget =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.BUDGET || [];
  const arr = Array.isArray(rawBudget) ? rawBudget : [rawBudget];

  const budgets = arr
    .map((b) => {
      const name = b.$?.NAME || val(b.NAME) || null;
      if (!name) return null;
      return {
        guid: val(b.GUID),
        name,
        parent: val(b.PARENT) || "Primary",
        startDate: tallyDateToISO(val(b.STARTDATE)),
        endDate: tallyDateToISO(val(b.ENDDATE)),
      };
    })
    .filter(Boolean);

  logger.success(`Fetched ${budgets.length} budgets`, { company: companyName });
  return budgets;
}

// ═════════════════════════════════════════════════════════════════════════════
// 10. STOCK GROUPS  (Inventory Masters → Stock Groups)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyStockGroups(companyName) {
  logger.info("Fetching stock groups from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "StockGroupCollection",
    "Stock Group",
    "NAME,GUID,PARENT,ISADDABLE",
    companyName
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, false);
  const rawSG =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.["STOCK-GROUP"] ||
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.STOCKGROUP || [];
  const arr = Array.isArray(rawSG) ? rawSG : [rawSG];

  const groups = arr
    .map((g) => {
      const name = g.$?.NAME || val(g.NAME) || null;
      if (!name) return null;
      return {
        guid: val(g.GUID),
        name,
        parent: val(g.PARENT) || "Primary",
        isAddable: val(g.ISADDABLE) === "Yes",
      };
    })
    .filter(Boolean);

  logger.success(`Fetched ${groups.length} stock groups`, { company: companyName });
  return groups;
}

// ═════════════════════════════════════════════════════════════════════════════
// 11. STOCK ITEMS  (Inventory Masters → Stock Items)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyStockItems(companyName) {
  logger.info("Fetching stock items from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "StockItemCollection",
    "Stock Item",
    // FIX: Use HSNDETAILS.LIST.HSNCODE (nested list path) instead of flat HSNDETAILS.HSNCODE.
    // TallyPrime stores HSN data inside a HSNDETAILS.LIST sub-list; the flat key returns empty.
    // Also fetch GSTRATE, GSTTYPEOFSUPPLY, and TAXABILITY for complete GST info.
    "NAME,GUID,PARENT,ALTERID,CATEGORY,BASEUNITS,HSNDETAILS.LIST.HSNCODE,HSNDETAILS.LIST.GSTRATE,GSTAPPLICABLE,GSTTYPEOFSUPPLY,TAXABILITY,OPENINGBALANCE,CLOSINGBALANCE,OPENINGVALUE,CLOSINGVALUE",
    companyName
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, false);
  const rawItems =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.["STOCK-ITEM"] ||
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.STOCKITEM || [];
  const itemsArr = Array.isArray(rawItems) ? rawItems : [rawItems];

  // Helper: extract HSN code from the nested HSNDETAILS.LIST structure.
  // Tally may return it as an array of objects, a single object, or (older versions)
  // as a flat HSNCODE field directly on the item. We try all paths in priority order.
  // Extract both HSN code AND GST rate from the HSNDETAILS.LIST structure.
  // Returns { hsnCode, gstRate } — gstRate is the numeric % (e.g. 18, 5, 28).
  function extractHsnDetails(s) {
    let hsnCode = null;
    let gstRate = null;

    // Path 1: xml2js explicitArray=false
    const detailsList = s["HSNDETAILS.LIST"];
    if (detailsList) {
      const arr = Array.isArray(detailsList) ? detailsList : [detailsList];
      for (const entry of arr) {
        if (!hsnCode) {
          const code = val(entry.HSNCODE) || val(entry["HSNDETAILS.LIST.HSNCODE"]);
          if (code && code.trim()) hsnCode = code.trim();
        }
        if (gstRate === null) {
          const rate = val(entry.GSTRATE) || val(entry["HSNDETAILS.LIST.GSTRATE"]);
          if (rate !== null && rate !== undefined && rate !== "") {
            const parsed = parseFloat(String(rate).replace(/[^0-9.]/g, ""));
            if (!isNaN(parsed)) gstRate = parsed;
          }
        }
        if (hsnCode && gstRate !== null) break;
      }
    }

    // Path 2: Some TallyPrime versions expose HSNDETAILS directly
    if (!hsnCode || gstRate === null) {
      const hsnDetails = s.HSNDETAILS;
      if (hsnDetails) {
        const arr = Array.isArray(hsnDetails) ? hsnDetails : [hsnDetails];
        for (const entry of arr) {
          const list = entry?.LIST || entry?.["HSNDETAILS.LIST"];
          if (list) {
            const listArr = Array.isArray(list) ? list : [list];
            for (const le of listArr) {
              if (!hsnCode) {
                const code = val(le.HSNCODE);
                if (code && code.trim()) hsnCode = code.trim();
              }
              if (gstRate === null) {
                const rate = val(le.GSTRATE);
                if (rate !== null && rate !== undefined && rate !== "") {
                  const parsed = parseFloat(String(rate).replace(/[^0-9.]/g, ""));
                  if (!isNaN(parsed)) gstRate = parsed;
                }
              }
            }
          }
          if (!hsnCode) {
            const code = val(entry.HSNCODE);
            if (code && code.trim()) hsnCode = code.trim();
          }
          if (gstRate === null) {
            const rate = val(entry.GSTRATE);
            if (rate !== null && rate !== undefined && rate !== "") {
              const parsed = parseFloat(String(rate).replace(/[^0-9.]/g, ""));
              if (!isNaN(parsed)) gstRate = parsed;
            }
          }
        }
      }
    }

    // Path 3: Older Tally flat export
    if (!hsnCode) {
      const flat = val(s["HSNDETAILS.HSNCODE"]) || val(s.HSNCODE);
      if (flat && flat.trim()) hsnCode = flat.trim();
    }
    if (gstRate === null) {
      const rate = val(s["HSNDETAILS.GSTRATE"]) || val(s.GSTRATE);
      if (rate !== null && rate !== undefined && rate !== "") {
        const parsed = parseFloat(String(rate).replace(/[^0-9.]/g, ""));
        if (!isNaN(parsed)) gstRate = parsed;
      }
    }

    return { hsnCode, gstRate };
  }

  const KNOWN_STOCK_KEYS = new Set([
    "$", "GUID", "NAME", "PARENT", "ALTERID", "CATEGORY", "BASEUNITS",
    "HSNDETAILS.LIST", "HSNDETAILS", "HSNDETAILS.LIST.HSNCODE", "HSNDETAILS.LIST.GSTRATE",
    "GSTAPPLICABLE", "GSTTYPEOFSUPPLY", "TAXABILITY",
    "OPENINGBALANCE", "CLOSINGBALANCE", "OPENINGVALUE", "CLOSINGVALUE",
  ]);

  const items = itemsArr
    .map((s) => {
      const name = s.$?.NAME || val(s.NAME) || null;
      if (!name) return null;
      const { hsnCode, gstRate } = extractHsnDetails(s);

      // Capture any extra fields Tally sends beyond what we explicitly map
      const customFields = {};
      for (const key of Object.keys(s)) {
        if (!KNOWN_STOCK_KEYS.has(key)) {
          const v2 = val(s[key]);
          if (v2 !== null && v2 !== undefined && v2 !== "") {
            customFields[key] = v2;
          }
        }
      }
      if (Object.keys(customFields).length > 0) {
        logger.human
          ? logger.human.headsUp(`Stock item "${name}" has ${Object.keys(customFields).length} extra field(s) from Tally: ${Object.keys(customFields).join(", ")}. These will be synced as custom fields in ERPNext.`)
          : logger.info(`Stock item "${name}" has extra Tally fields: ${Object.keys(customFields).join(", ")}`);
      }

      return {
        guid:            val(s.GUID),
        name,
        alterId:         val(s.ALTERID) || null,
        group:           val(s.PARENT) || "Primary",
        category:        val(s.CATEGORY) || null,
        baseUnit:        val(s.BASEUNITS),
        hsnCode,
        gstRate,         // numeric GST % from Tally (e.g. 18, 5, 28) — used for Item Tax Template
        gstApplicable:   val(s.GSTAPPLICABLE),
        gstTypeOfSupply: val(s.GSTTYPEOFSUPPLY),
        taxability:      val(s.TAXABILITY),       // "Taxable" / "Non-GST" / "Exempt" / "Nil-Rated"
        openingQty:      parseTallyAmount(s.OPENINGBALANCE?._ || s.OPENINGBALANCE),
        closingQty:      parseTallyAmount(s.CLOSINGBALANCE?._ || s.CLOSINGBALANCE),
        openingValue:    parseTallyAmount(s.OPENINGVALUE?._ || s.OPENINGVALUE),
        closingValue:    parseTallyAmount(s.CLOSINGVALUE?._ || s.CLOSINGVALUE),
        customFields:    Object.keys(customFields).length > 0 ? customFields : undefined,
      };
    })
    .filter(Boolean);

  const withHsn     = items.filter((i) => i.hsnCode).length;
  const withGstRate = items.filter((i) => i.gstRate !== null && i.gstRate !== undefined).length;
  logger.success(`Fetched ${items.length} stock items (${withHsn} with HSN/SAC, ${withGstRate} with GST rate)`, { company: companyName });
  return items;
}
// ═════════════════════════════════════════════════════════════════════════════
// 12. STOCK CATEGORIES  (Inventory Masters → Stock Categories)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyStockCategories(companyName) {
  logger.info("Fetching stock categories from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "StockCategoryCollection",
    "Stock Category",
    "NAME,GUID,PARENT",
    companyName
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, false);
  const rawSC =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.["STOCK-CATEGORY"] ||
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.STOCKCATEGORY || [];
  const arr = Array.isArray(rawSC) ? rawSC : [rawSC];

  const cats = arr
    .map((c) => {
      const name = c.$?.NAME || val(c.NAME) || null;
      if (!name) return null;
      return {
        guid: val(c.GUID),
        name,
        parent: val(c.PARENT) || "Primary",
      };
    })
    .filter(Boolean);

  logger.success(`Fetched ${cats.length} stock categories`, { company: companyName });
  return cats;
}

// ═════════════════════════════════════════════════════════════════════════════
// 13. UNITS  (Inventory Masters → Units)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyUnits(companyName) {
  logger.info("Fetching units from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "UnitCollection",
    "Unit",
    "NAME,GUID,ISSIMPLEUNIT,BASEUNITS,ADDITIONALUNITS,CONVERSIONFACTOR,DECIMALPLACES",
    companyName
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, false);
  const rawUnits =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.UNIT || [];
  const arr = Array.isArray(rawUnits) ? rawUnits : [rawUnits];

  const units = arr
    .map((u) => {
      const name = u.$?.NAME || val(u.NAME) || null;
      if (!name) return null;
      return {
        guid: val(u.GUID),
        name,
        isSimple: val(u.ISSIMPLEUNIT) !== "No",
        baseUnit: val(u.BASEUNITS) || null,
        additionalUnit: val(u.ADDITIONALUNITS) || null,
        conversionFactor: parseTallyAmount(u.CONVERSIONFACTOR),
        decimalPlaces: parseInt(val(u.DECIMALPLACES) || "0", 10),
      };
    })
    .filter(Boolean);

  logger.success(`Fetched ${units.length} units`, { company: companyName });
  return units;
}

// ═════════════════════════════════════════════════════════════════════════════
// 14. GODOWNS  (Inventory Masters → Godowns)
// ═════════════════════════════════════════════════════════════════════════════
export async function fetchTallyGodowns(companyName) {
  logger.info("Fetching godowns from Tally", { company: companyName });
  const xml = buildCollectionXml(
    "GodownCollection",
    "Godown",
    "NAME,GUID,PARENT,ALTERID,ADDRESS,HASSPACE,ISINTERNAL,ISEXTERNAL",
    companyName
  );
  const raw = await postXml(xml);
  const parsed = await parseXml(raw, false);
  const rawGD =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.GODOWN || [];
  const arr = Array.isArray(rawGD) ? rawGD : [rawGD];

  const godowns = arr
    .map((g) => {
      const name = g.$?.NAME || val(g.NAME) || null;
      if (!name) return null;
      return {
        guid:       val(g.GUID),
        name,
        parent:     val(g.PARENT) || "Primary",
        address:    extractAddress(g.ADDRESS) || null,
        hasSpace:   val(g.HASSPACE) === "Yes",
        isInternal: val(g.ISINTERNAL) !== "No",
        alterId:    val(g.ALTERID)   || null,
      };
    })
    .filter(Boolean);

  logger.success(`Fetched ${godowns.length} godowns`, { company: companyName });
  return godowns;
}

// ═════════════════════════════════════════════════════════════════════════════
// 15. VOUCHERS / TRANSACTIONS  (with date range)
//
// FIXES APPLIED:
//  1. $$SysName  (was $SysName)  — missing $$ caused malformed export format
//  2. $$InRange  (was $InRange)  — missing $$ meant the date filter was
//     silently ignored; Tally returned ALL vouchers since company creation
//     (2016+), causing the 120 000 ms timeout on full-year queries.
//  3. Added SVFROMDATE / SVTODATE static variables as a primary date filter
//     (more reliable; both set so older and newer TallyPrime builds are covered).
//  4. Monthly chunking: ranges > 31 days are split into monthly batches so
//     each request stays well within the 120 s timeout even for large books.
// ═════════════════════════════════════════════════════════════════════════════

// ── Internal: single-month (≤ 31 day) chunk ───────────────────────────────────
// ── Internal: single-month (≤ 31 day) chunk ───────────────────────────────────
// ── Global Tally mutex ───────────────────────────────────────────────────────
// Tally is single-threaded with a global "active company" state.
// Parallel requests race to switch the active company and corrupt each other's
// responses. This mutex ensures only ONE request hits Tally at a time.
let _tallyBusy = false;
const _tallyQueue = [];
function tallyLock() {
  return new Promise((resolve) => {
    if (!_tallyBusy) { _tallyBusy = true; resolve(); }
    else _tallyQueue.push(resolve);
  });
}
function tallyUnlock() {
  if (_tallyQueue.length > 0) {
    const next = _tallyQueue.shift();
    next();
  } else {
    _tallyBusy = false;
  }
}

// ── Internal: fetch ALL vouchers for this company in ONE request ──────────────
// Tally's TDL Collection date-filtering ($$InRange, SVFROMDATE/SVTODATE) is
// unreliable across TallyPrime versions:
//   - Without filter  → returns every voucher (all 129, all chunks identical)
//   - With $$InRange  → returns 0 on many builds (TDL ignored or unsupported)
//
// CORRECT STRATEGY:
//   1. Fetch ALL vouchers in one single request (no date filter in XML)
//   2. Filter by date in JavaScript — reliable, fast, zero Tally API calls
//   3. Deduplicate by GUID — prevents any double-count if Tally returns dupes
//   4. Only chunk if the company has a huge number of vouchers (>2000) to
//      avoid a single massive XML response hanging Tally. Each chunk requests
//      the full set but we deduplicate at the end, so the result is correct.
async function fetchTallyVouchersChunk(companyName, fromDate, toDate) {
  // STRATEGY: Use <TYPE>Object</TYPE> with OBJECTTYPE=Voucher — the most reliable
  // method across all TallyPrime versions. SVFROMDATE/SVTODATE are set as static
  // variables so Tally applies its own built-in date filter server-side.
  // We do NOT use $$InRange in a TDL filter — it returns 0 on most builds.
  // FIX: No hardcoded fallback date — caller must always pass fromDate.
  // If fromDate is null/undefined, use today (safe fallback for incremental syncs).
  const tallyFrom = fromDate
    ? String(fromDate).replace(/-/g, "")
    : new Date().toISOString().slice(0, 10).replace(/-/g, "");
  const tallyTo   = toDate   ? String(toDate).replace(/-/g, "")   : new Date().toISOString().slice(0,10).replace(/-/g,"");

  const xml = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>VoucherCollection</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <SVCURRENTCOMPANY>${escapeXml(companyName)}</SVCURRENTCOMPANY>
    <SVFROMDATE>${tallyFrom}</SVFROMDATE>
    <SVTODATE>${tallyTo}</SVTODATE>
   </STATICVARIABLES>
   <TDL>
    <TDLMESSAGE>
     <COLLECTION NAME="VoucherCollection" ISMODIFY="No">
      <TYPE>Voucher</TYPE>
      <FETCH>GUID,DATE,VOUCHERNUMBER,VOUCHERTYPENAME,PARTYLEDGERNAME,NARRATION,REFERENCE,ISINVOICE,ISOPTIONAL,ISPOSTDATED,ALLLEDGERENTRIES.LIST,ALLLEDGERENTRIES.LIST.COSTCENTREDETAILS.LIST,ALLINVENTORYENTRIES.LIST,ALLINVENTORYENTRIES.LIST.BATCHALLOCATIONS.LIST,INVENTORYENTRIES.LIST,BILLALLOCATIONS.LIST</FETCH>
     </COLLECTION>
    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>`;

  await tallyLock();
  let raw;
  try {
    raw = await postXml(xml);
  } finally {
    tallyUnlock();
  }

  const parsed = await parseXml(raw, false);

  // Response path for inline TDL Collection: ENVELOPE > BODY > DATA > COLLECTION > VOUCHER
  const body = parsed?.ENVELOPE?.BODY;
  const rawVouchers =
    body?.DATA?.COLLECTION?.VOUCHER   ||
    body?.DATA?.TALLYMESSAGE?.VOUCHER ||
    body?.DATA?.VOUCHER               ||
    body?.TALLYMESSAGE?.VOUCHER       ||
    [];

  const vArr = Array.isArray(rawVouchers) ? rawVouchers : (rawVouchers ? [rawVouchers] : []);
  logger.info(`Raw vouchers received from Tally: ${vArr.length}`, { company: companyName });

  const vouchers = [];
  for (const v of vArr) {
    const guid =
      v.$?.GUID ||
      val(v.GUID) ||
      `${val(v.DATE)}-${val(v.VOUCHERNUMBER)}-${val(v.VOUCHERTYPENAME)}`;

    const allEntries = v["ALLLEDGERENTRIES.LIST"] || v["LEDGERENTRIES.LIST"] || [];
    const entryArr   = Array.isArray(allEntries) ? allEntries : [allEntries];
    let netAmount    = 0;
    const entries    = [];

    for (const e of entryArr) {
      if (!e || typeof e !== "object") continue;
      const ledgerName       = val(e.LEDGERNAME);
      const isDeemedPositive = val(e.ISDEEMEDPOSITIVE) === "Yes";
      const amount           = parseTallyAmount(val(e.AMOUNT));
      if (isDeemedPositive) netAmount += amount;

      // Cost centre: Tally stores it in COSTCENTREDETAILS.LIST on the ledger entry.
      // Take the first cost centre name if present (most vouchers have only one).
      const ccDetails  = e["COSTCENTREDETAILS.LIST"] || e["COSTCENTREDETAILS"] || [];
      const ccArr      = Array.isArray(ccDetails) ? ccDetails : (ccDetails ? [ccDetails] : []);
      const costCentre = val(ccArr[0]?.COSTCENTRENAME) || null;

      if (ledgerName) entries.push({
        ledger:      ledgerName,
        amount,
        isDebit:     isDeemedPositive,
        costCentre,                        // ← Tally cost centre name (null if not set)
        isDeemedPositive,                  // ← raw flag kept for downstream logic
      });
    }

    // ── Parse inventory line items ───────────────────────────────────────────────
    // Tally stores inventory items in different sub-lists depending on voucher type:
    //   "Sales Invoice" / "Purchase Invoice" → ALLINVENTORYENTRIES.LIST
    //   "Sales" / "Purchase" (simple voucher) → INVENTORYENTRIES.LIST
    //   Some builds export both; we merge both lists so nothing is missed.
    //
    // Field quirks (explicitArray:false mode):
    //   STOCKITEMNAME → plain string (item name)  e.g. "Red Pen"
    //   ACTUALQTY     → string with UoM suffix    e.g. "1 Nos"  — strip non-numeric
    //   BILLEDQTY     → same format               e.g. "1 Nos"
    //   RATE          → string with UoM suffix    e.g. "79.00 /Nos" — strip non-numeric
    //   AMOUNT        → plain number string       e.g. "-79.00"
    //
    // Merge both inventory entry lists — order: ALLINVENTORYENTRIES first
    const rawInvAll  = v["ALLINVENTORYENTRIES.LIST"] || [];
    const rawInvSimp = v["INVENTORYENTRIES.LIST"]    || [];
    const rawInvEntries = [
      ...(Array.isArray(rawInvAll)  ? rawInvAll  : (rawInvAll  ? [rawInvAll]  : [])),
      ...(Array.isArray(rawInvSimp) ? rawInvSimp : (rawInvSimp ? [rawInvSimp] : [])),
    ];

    const inventoryItems = [];
    for (const ie of rawInvEntries) {
      if (!ie || typeof ie !== "object") continue;

      // Item name: try STOCKITEMNAME first, then NAME (some Tally versions use NAME)
      const itemName = val(ie.STOCKITEMNAME) || val(ie.NAME);
      if (!itemName) continue;

      // Qty: prefer BILLEDQTY over ACTUALQTY (matches what appears on the invoice)
      const qty    = parseTallyQty(val(ie.BILLEDQTY) || val(ie.ACTUALQTY));
      const rate   = parseTallyRate(val(ie.RATE));
      const amount = parseTallyAmount(val(ie.AMOUNT));

      // Derive missing rate/amount from the other field
      const finalRate   = rate   || (qty ? amount / qty : 0);
      const finalAmount = amount || (rate * qty);

      // Godown (warehouse) and batch from the first BATCHALLOCATIONS sub-entry
      const batchAllocs  = ie["BATCHALLOCATIONS.LIST"] || ie["BATCHALLOCATIONS"] || [];
      const batchArr     = Array.isArray(batchAllocs) ? batchAllocs : (batchAllocs ? [batchAllocs] : []);
      const godownName   = val(batchArr[0]?.GODOWNNAME)  || val(ie.GODOWNNAME)  || null;
      const batchName    = val(batchArr[0]?.BATCHNAME)   || val(ie.BATCHNAME)   || null;

      inventoryItems.push({
        itemName,
        qty:        qty,
        rate:       Math.abs(finalRate),
        amount:     Math.abs(finalAmount),
        godownName,   // ← Tally godown / warehouse name
        batchName,    // ← Tally batch / lot name
      });
    }

    // Extract due date from Tally bill allocations (set by credit terms on the voucher)
    const billAllocs = v["BILLALLOCATIONS.LIST"] || [];
    const billArr    = Array.isArray(billAllocs) ? billAllocs : (billAllocs ? [billAllocs] : []);
    const firstBill  = billArr.find((b) => b && val(b.BILLDATE));
    const dueDate    = firstBill ? tallyDateToISO(val(firstBill.BILLDATE)) : null;
    // Bill reference name (used as supplier invoice number for Purchase vouchers)
    const billRefName = firstBill ? (val(firstBill.NAME) || val(firstBill.BILLNAME)) : null;

    // Identify the sales/purchase ledger entry — the entry that is NOT the party ledger.
    // For a Sales voucher: party entry is credit (isDebit=false), sales entry is debit.
    // For a Purchase voucher: party entry is debit (isDebit=true), purchase entry is credit.
    const partyLedgerName = val(v.PARTYLEDGERNAME);
    const salesLedgerEntry = entries.find(
      (e) => e.ledger && e.ledger !== partyLedgerName
    );
    const salesLedgerName   = salesLedgerEntry?.ledger   || null;
    const salesCostCentre   = salesLedgerEntry?.costCentre || null;
    // First cost centre across all entries (fallback if salesLedgerEntry has none)
    const firstCostCentre   = entries.find((e) => e.costCentre)?.costCentre || null;

    vouchers.push({
      guid: guid || `voucher-${vouchers.length}`,
      voucherDate:    tallyDateToISO(val(v.DATE)),
      voucherType:    val(v.VOUCHERTYPENAME) || val(v.VOUCHERTYPE),
      voucherNumber:  val(v.VOUCHERNUMBER),
      referenceNo:    val(v.REFERENCE) || billRefName,  // bill ref name as fallback
      partyName:      val(v.PARTYLEDGERNAME),
      narration:      val(v.NARRATION),
      dueDate,        // ← bill due date from Tally credit terms (null if not set)
      netAmount,
      entries,
      inventoryItems,                          // ← real item rows from Tally
      salesLedgerName,                         // ← Tally sales/purchase ledger name
      salesCostCentre,                         // ← cost centre on the sales ledger entry
      firstCostCentre,                         // ← first cost centre found on any entry
      lineItemCount:  entries.length,
      isInvoice:    val(v.ISINVOICE)   === "Yes",
      isOptional:   val(v.ISOPTIONAL)  === "Yes",
      isPostDated:  val(v.ISPOSTDATED) === "Yes",
    });
  }

  logger.info(`Vouchers parsed successfully: ${vouchers.length}`, { company: companyName });
  return vouchers;
}

export async function fetchTallyVouchers(companyName, fromDate = null, toDate = null) {
  logger.info(`Fetching ALL vouchers from Tally`, { company: companyName, fromDate, toDate });

  // Pass dates to Tally so it applies SVFROMDATE/SVTODATE server-side.
  // JS-side filtering below is kept as a safety net.
  const allVouchers = await fetchTallyVouchersChunk(companyName, fromDate, toDate);

  // Deduplicate by GUID
  const seen = new Set();
  const unique = allVouchers.filter((v) => {
    if (seen.has(v.guid)) return false;
    seen.add(v.guid);
    return true;
  });

  // Apply JS-side date filter only if dates were explicitly passed
  if (fromDate || toDate) {
    const filtered = unique.filter((v) => {
      if (!v.voucherDate) return true;
      const d = v.voucherDate.slice(0, 10); // normalise to YYYY-MM-DD for safe string compare
      if (fromDate && d < fromDate) return false;
      if (toDate   && d > toDate)   return false;
      return true;
    });
    logger.success(`Fetched ${filtered.length} vouchers (filtered: ${fromDate} → ${toDate}) from ${unique.length} total`, { company: companyName });
    // Diagnostic: when all vouchers are filtered out, log the actual date range in Tally
    // so it's immediately obvious whether the fromDate window is too narrow.
    if (filtered.length === 0 && unique.length > 0) {
      const dates = unique.map((v) => v.voucherDate).filter(Boolean).sort();
      const earliest = dates[0] || "unknown";
      const latest   = dates[dates.length - 1] || "unknown";
      logger.warn(
        `All ${unique.length} Tally vouchers are outside the requested window (${fromDate} → ${toDate}). ` +
        `Actual voucher dates in Tally: ${earliest} → ${latest}. ` +
        `If this is unexpected, reset the sync state so the next run uses the company start date as fromDate.`,
        { company: companyName }
      );
    }
    return filtered;
  }

  logger.success(`Fetched ${unique.length} total vouchers (all dates)`, { company: companyName });
  return unique;
}

// ═════════════════════════════════════════════════════════════════════════════
// 16. FULL MIDDLEWARE CHECK — fetch ALL masters + transactions and validate
// ═════════════════════════════════════════════════════════════════════════════
export async function runMiddlewareCheck(companyName, options = {}) {
  const result = {
    company: companyName,
    startedAt: new Date().toISOString(),
    status: "running",
    checks: {
      // ── Connection ──────────────────────────────────────────────────────────
      ping: { status: "pending" },
      companies: { status: "pending" },

      // ── Accounting Masters ──────────────────────────────────────────────────
      groups: { status: "pending" },
      ledgers: { status: "pending" },
      voucherTypes: { status: "pending" },
      costCategories: { status: "pending" },
      costCentres: { status: "pending" },
      currencies: { status: "pending" },
      budgets: { status: "pending" },

      // ── Inventory Masters ───────────────────────────────────────────────────
      stockGroups: { status: "pending" },
      stockItems: { status: "pending" },
      stockCategories: { status: "pending" },
      units: { status: "pending" },
      godowns: { status: "pending" },

      // ── Transactions ────────────────────────────────────────────────────────
      vouchers: { status: "pending" },
    },
    summary: null,
    readyToSync: false,
    errors: [],
  };

  // ── Helper: run a non-critical (warn-on-fail) check ──────────────────────
  async function softCheck(key, fn) {
    try {
      return await fn();
    } catch (e) {
      result.checks[key] = { status: "warn", error: e.message, count: 0 };
      return null;
    }
  }

  // ── 1. Ping ───────────────────────────────────────────────────────────────
  try {
    const ping = await pingTally();
    result.checks.ping = {
      status: ping.connected ? "ok" : "fail",
      latencyMs: ping.latencyMs,
      url: ping.url,
      error: ping.error || null,
    };
    if (!ping.connected) {
      result.status = "failed";
      result.errors.push(`Connection failed: ${ping.error}`);
      result.readyToSync = false;
      result.finishedAt = new Date().toISOString();
      return result;
    }
  } catch (e) {
    result.checks.ping = { status: "fail", error: e.message };
    result.status = "failed";
    result.readyToSync = false;
    result.finishedAt = new Date().toISOString();
    return result;
  }

  // ── 2. Companies (critical) ───────────────────────────────────────────────
  try {
    const companies = await fetchTallyCompanies();
    result.checks.companies = {
      status: companies.length > 0 ? "ok" : "warn",
      count: companies.length,
      data: companies,
    };
    if (companies.length === 0) result.errors.push("No companies found in Tally");
  } catch (e) {
    result.checks.companies = { status: "fail", error: e.message };
    result.errors.push(`Companies: ${e.message}`);
  }

  // ── 3. Accounting Groups ──────────────────────────────────────────────────
  await softCheck("groups", async () => {
    const groups = await fetchTallyGroups(companyName);
    result.checks.groups = {
      status: groups.length > 0 ? "ok" : "warn",
      count: groups.length,
      sample: groups.slice(0, 5),
    };
  });

  // ── 4. Ledgers (critical for sync) ───────────────────────────────────────
  try {
    const ledgers = await fetchTallyLedgers(companyName);
    const partyLedgers = ledgers.filter((l) => l.type === "Party");
    result.checks.ledgers = {
      status: ledgers.length > 0 ? "ok" : "warn",
      count: ledgers.length,
      partyCount: partyLedgers.length,
      withGstin: ledgers.filter((l) => l.gstin).length,
      withEmail: ledgers.filter((l) => l.email).length,
      withPhone: ledgers.filter((l) => l.phone).length,
      sample: ledgers.slice(0, 5),
    };
    if (ledgers.length === 0) result.errors.push("No ledgers found");
  } catch (e) {
    result.checks.ledgers = { status: "fail", error: e.message };
    result.errors.push(`Ledgers: ${e.message}`);
  }

  // ── 5. Voucher Types ──────────────────────────────────────────────────────
  await softCheck("voucherTypes", async () => {
    const vt = await fetchTallyVoucherTypes(companyName);
    result.checks.voucherTypes = {
      status: vt.length > 0 ? "ok" : "warn",
      count: vt.length,
      sample: vt.slice(0, 8),
    };
  });

  // ── 6. Cost Categories ────────────────────────────────────────────────────
  await softCheck("costCategories", async () => {
    const cc = await fetchTallyCostCategories(companyName);
    result.checks.costCategories = {
      status: "ok",
      count: cc.length,
      sample: cc.slice(0, 5),
    };
  });

  // ── 7. Cost Centres ───────────────────────────────────────────────────────
  await softCheck("costCentres", async () => {
    const centres = await fetchTallyCostCentres(companyName);
    result.checks.costCentres = {
      status: "ok",
      count: centres.length,
      sample: centres.slice(0, 5),
    };
  });

  // ── 8. Currencies ─────────────────────────────────────────────────────────
  await softCheck("currencies", async () => {
    const cur = await fetchTallyCurrencies(companyName);
    result.checks.currencies = {
      status: cur.length > 0 ? "ok" : "warn",
      count: cur.length,
      sample: cur,
    };
  });

  // ── 9. Budgets ────────────────────────────────────────────────────────────
  await softCheck("budgets", async () => {
    const bud = await fetchTallyBudgets(companyName);
    result.checks.budgets = {
      status: "ok",
      count: bud.length,
      sample: bud.slice(0, 5),
    };
  });

  // ── 10. Stock Groups ──────────────────────────────────────────────────────
  await softCheck("stockGroups", async () => {
    const sg = await fetchTallyStockGroups(companyName);
    result.checks.stockGroups = {
      status: "ok",
      count: sg.length,
      sample: sg.slice(0, 5),
    };
  });

  // ── 11. Stock Items ───────────────────────────────────────────────────────
  await softCheck("stockItems", async () => {
    const items = await fetchTallyStockItems(companyName);
    const totalClosingValue = items.reduce((s, i) => s + i.closingValue, 0);
    result.checks.stockItems = {
      status: items.length > 0 ? "ok" : "warn",
      count: items.length,
      totalClosingValue,
      sample: items.slice(0, 5),
    };
  });

  // ── 12. Stock Categories ──────────────────────────────────────────────────
  await softCheck("stockCategories", async () => {
    const sc = await fetchTallyStockCategories(companyName);
    result.checks.stockCategories = {
      status: "ok",
      count: sc.length,
      sample: sc.slice(0, 5),
    };
  });

  // ── 13. Units ─────────────────────────────────────────────────────────────
  await softCheck("units", async () => {
    const units = await fetchTallyUnits(companyName);
    result.checks.units = {
      status: units.length > 0 ? "ok" : "warn",
      count: units.length,
      sample: units.slice(0, 8),
    };
  });

  // ── 14. Godowns ───────────────────────────────────────────────────────────
  await softCheck("godowns", async () => {
    const gd = await fetchTallyGodowns(companyName);
    result.checks.godowns = {
      status: "ok",
      count: gd.length,
      sample: gd.slice(0, 5),
    };
  });

  // ── 15. Vouchers (transactions) ───────────────────────────────────────────
  // Fetch ALL vouchers — no date filter at all.
  // ── 15. Vouchers (transactions) ───────────────────────────────────────────
try {
  const today   = new Date().toISOString().slice(0, 10);
  const from365 = new Date(Date.now() - 365 * 864e5).toISOString().slice(0, 10);

  // Use user-supplied dates from the Data Check form if provided; fall back to last 365 days
  const voucherFrom = options.fromDate || from365;
  const voucherTo   = options.toDate   || today;

  logger.info(`Voucher check: fetching vouchers from ${voucherFrom} to ${voucherTo}`, { company: companyName });
  const vouchers = await fetchTallyVouchers(companyName, voucherFrom, voucherTo);
    const byType = {};

    vouchers.forEach((v) => {
      byType[v.voucherType] = (byType[v.voucherType] || 0) + 1;
    });
    const totalAmount = vouchers.reduce((s, v) => s + v.netAmount, 0);
    result.checks.vouchers = {
      status: vouchers.length > 0 ? "ok" : "warn",
      count: vouchers.length,
      byType,
      totalAmount,
      sample: vouchers.slice(0, 5),
    };
  } catch (e) {
    logger.error(`Voucher check failed: ${e.message}`, { company: companyName });
    result.checks.vouchers = {
      status: "warn",
      error: e.message,
      count: 0,
    };
    result.errors.push(`Vouchers fetch failed: ${e.message}`);
  }

  // ── Final status ──────────────────────────────────────────────────────────
  // Only ping + companies + ledgers are hard failures; everything else is warn
  const criticalFail = ["ping", "companies", "ledgers"].some(
    (k) => result.checks[k]?.status === "fail"
  );
  const hasWarn = Object.values(result.checks).some((c) => c.status === "warn");

  result.status = criticalFail ? "failed" : hasWarn ? "warning" : "ok";
  result.readyToSync = !criticalFail;
  result.finishedAt = new Date().toISOString();
  result.summary = {
    companies: result.checks.companies?.count || 0,
    // Accounting masters
    groups: result.checks.groups?.count || 0,
    ledgers: result.checks.ledgers?.count || 0,
    voucherTypes: result.checks.voucherTypes?.count || 0,
    costCategories: result.checks.costCategories?.count || 0,
    costCentres: result.checks.costCentres?.count || 0,
    currencies: result.checks.currencies?.count || 0,
    budgets: result.checks.budgets?.count || 0,
    // Inventory masters
    stockGroups: result.checks.stockGroups?.count || 0,
    stockItems: result.checks.stockItems?.count || 0,
    stockCategories: result.checks.stockCategories?.count || 0,
    units: result.checks.units?.count || 0,
    godowns: result.checks.godowns?.count || 0,
    // Transactions
    vouchers: result.checks.vouchers?.count || 0,
  };

  logger.info(`Middleware check complete: ${result.status}`, result.summary);
  return result;
}
// ═════════════════════════════════════════════════════════════════════════════
// 17. CHUNKED VOUCHER FETCH — monthly chunks with resume support
//     Used for first-time full sync of large Tally books.
//     Multi-tenant: chunk cache keyed by (company + erpnextUrl).
// ═════════════════════════════════════════════════════════════════════════════

import {
  isChunkDone,
  markChunkDone,
} from "../syncChunkState.js";

/**
 * generateMonthlyChunks(fromDate, toDate)
 * Splits a date range into monthly chunks.
 * Returns array of { id: "2016-04", from: "2016-04-01", to: "2016-04-30" }
 */
function generateMonthlyChunks(fromDate, toDate) {
  const chunks  = [];
  let   current = new Date(fromDate + "T00:00:00Z");
  const end     = new Date(toDate   + "T00:00:00Z");

  while (current <= end) {
    const year  = current.getUTCFullYear();
    const month = current.getUTCMonth(); // 0-indexed

    // Last day of this month
    const lastDay  = new Date(Date.UTC(year, month + 1, 0));
    const chunkEnd = lastDay > end ? end : lastDay;

    const fromStr = current.toISOString().slice(0, 10);
    const toStr   = chunkEnd.toISOString().slice(0, 10);
    const id      = `${year}-${String(month + 1).padStart(2, "0")}`;

    chunks.push({ id, from: fromStr, to: toStr });

    // Move to first day of next month
    current = new Date(Date.UTC(year, month + 1, 1));
  }
  return chunks;
}

/**
 * fetchTallyVouchersChunked(companyName, fromDate, toDate, erpnextUrl, onProgress?)
 * ─────────────────────────────────────────────────────────────────────────────
 * Fetches vouchers in monthly chunks with resume support.
 * If a chunk was already fetched and cached (isChunkDone), it is skipped.
 * If interrupted, next call resumes from the last incomplete chunk.
 *
 * @param companyName  - Tally company name
 * @param fromDate     - YYYY-MM-DD (company books start date for first sync)
 * @param toDate       - YYYY-MM-DD
 * @param erpnextUrl   - ERPNext instance URL (for cache key, multi-tenant)
 * @param onProgress   - optional callback(chunkId, count, totalChunks, idx, skipped)
 */
export async function fetchTallyVouchersChunked(
  companyName,
  fromDate,
  toDate,
  erpnextUrl  = "default",
  onProgress  = null
) {
  if (!fromDate) {
    throw new Error(
      `fetchTallyVouchersChunked: fromDate is required. ` +
      `Ensure the Tally company has a valid books beginning date.`
    );
  }

  const chunks = generateMonthlyChunks(fromDate, toDate);
  const total  = chunks.length;

  logger.info(
    `Voucher chunked fetch: ${total} monthly chunks — ${fromDate} → ${toDate}`,
    { company: companyName, erpnextUrl }
  );

  const allVouchers = [];
  const seen        = new Set();

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];

    // ── Resume: skip already-done chunks ────────────────────────────────────
    if (isChunkDone(companyName, erpnextUrl, "vouchers", chunk.id)) {
      logger.info(
        `Voucher chunk ${chunk.id} (${i + 1}/${total}): already done — skipping`,
        { company: companyName }
      );
      if (onProgress) onProgress(chunk.id, 0, total, i + 1, true /* skipped */);
      continue;
    }

    logger.info(
      `Voucher chunk ${chunk.id} (${i + 1}/${total}): fetching ${chunk.from} → ${chunk.to}`,
      { company: companyName }
    );

    try {
      const raw = await fetchTallyVouchersChunk(companyName, chunk.from, chunk.to);

      // Deduplicate across chunks
      const unique = raw.filter((v) => {
        if (seen.has(v.guid)) return false;
        seen.add(v.guid);
        return true;
      });

      allVouchers.push(...unique);

      // ── Save chunk progress immediately after successful fetch ────────────
      markChunkDone(companyName, erpnextUrl, "vouchers", chunk.id, {
        from:  chunk.from,
        to:    chunk.to,
        count: unique.length,
      });

      logger.info(
        `Voucher chunk ${chunk.id}: ${unique.length} vouchers — ${i + 1}/${total} done`,
        { company: companyName }
      );

      if (onProgress) onProgress(chunk.id, unique.length, total, i + 1, false);

    } catch (err) {
      // Do NOT mark as done — will retry this chunk on next run
      logger.error(
        `Voucher chunk ${chunk.id} failed: ${err.message} — will retry on next run`,
        { company: companyName }
      );
      throw err; // bubble up — sync job fails cleanly, resumes next time
    }
  }

  logger.info(
    `All ${total} voucher chunks complete: ${allVouchers.length} total vouchers`,
    { company: companyName }
  );
  return allVouchers;
}

/**
 * fetchTallyLedgersChunked(companyName, erpnextUrl, batchSize?, onProgress?)
 * ─────────────────────────────────────────────────────────────────────────────
 * Fetches all ledgers from Tally then splits into batches for ERPNext push.
 * Returns { batches, total, batchCount } — caller pushes each batch and marks done.
 *
 * @param companyName - Tally company name
 * @param erpnextUrl  - ERPNext instance URL (for cache key, multi-tenant)
 * @param batchSize   - ledgers per batch (default from env or 500)
 * @param onProgress  - optional callback(batchId, count, totalBatches, idx, skipped)
 */
export async function fetchTallyLedgersChunked(
  companyName,
  erpnextUrl  = "default",
  batchSize   = parseInt(process.env.SYNC_LEDGER_BATCH_SIZE, 10) || 500,
  onProgress  = null
) {
  logger.info(
    `Ledger chunked fetch: fetching all ledgers then batching by ${batchSize}`,
    { company: companyName }
  );

  // Fetch all at once from Tally (Tally doesn't support offset/pagination)
  const allLedgers = await fetchTallyLedgers(companyName);
  const total      = allLedgers.length;
  const batches    = [];

  // Split into batches
  for (let i = 0; i < total; i += batchSize) {
    batches.push({
      id:      `batch-${Math.floor(i / batchSize)}`,
      from:    i,
      to:      Math.min(i + batchSize, total),
      ledgers: allLedgers.slice(i, i + batchSize),
    });
  }

  logger.info(
    `Ledger chunked fetch: ${total} ledgers → ${batches.length} batches of ${batchSize}`,
    { company: companyName }
  );

  // Return only batches that are NOT already done (resume support)
  const pendingBatches = [];

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];

    if (isChunkDone(companyName, erpnextUrl, "ledgers", batch.id)) {
      logger.info(
        `Ledger batch ${batch.id} (${i + 1}/${batches.length}): already done — skipping`,
        { company: companyName }
      );
      if (onProgress) onProgress(batch.id, 0, batches.length, i + 1, true);
      continue;
    }

    pendingBatches.push({ ...batch, index: i });
    if (onProgress) onProgress(batch.id, batch.ledgers.length, batches.length, i + 1, false);
  }

  return { batches: pendingBatches, total, batchCount: batches.length, allLedgers };
}