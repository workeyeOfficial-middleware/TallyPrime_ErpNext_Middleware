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
    "NAME,GUID,PARENT,ALTERID,ADDRESS,MAILINGNAME,MAILINGDETAILS.LIST,OPENINGBALANCE,CLOSINGBALANCE,ISBILLWISEON,LEDGERPHONE,LEDGERMOBILE,EMAIL,INCOMETAXNUMBER,GSTIN.LIST,COUNTRYNAME,STATENAME,PINCODE,BANKACCNO,IFSCODE,SWIFTCODE,BANKNAME",
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

    const blob = [
      val(l.LEDGERMOBILE),
      val(l.LEDGERPHONE),
      val(l.EMAIL),
      extractAddress(l.ADDRESS),
    ]
      .filter(Boolean)
      .join(" ");

    ledgers.push({
      guid,
      name,
      alterId: val(l.ALTERID) || null,
      parentGroup: val(l.PARENT) || "Sundry Debtors",
      openingBalance: parseTallyAmount(l.OPENINGBALANCE?._ || l.OPENINGBALANCE),
      closingBalance: parseTallyAmount(l.CLOSINGBALANCE?._ || l.CLOSINGBALANCE),
      type: val(l.ISBILLWISEON) === "Yes" ? "Party" : "General",
      phone: extractPhone(blob),
      email: extractEmail(blob),
      pan: val(l.INCOMETAXNUMBER) || null,
      gstin: (() => {
        const g = l["GSTIN.LIST"];
        if (!g) return null;
        const a = Array.isArray(g) ? g : [g];
        return a[0]?.GSTIN || null;
      })(),
      state: (() => {
        const s = val(l.STATENAME) || "";
        const clean = s.replace(/[♦◆\u0004\u2297\u2666\u25c6*]/g, "").trim();
        return (clean && clean.toLowerCase() !== "not applicable") ? clean : null;
      })(),
      pincode: val(l.PINCODE) || null,
      mailingName: val(l.MAILINGNAME) || null,   // alias / trade name
      bankAccount: val(l.BANKACCNO)  || null,
      ifsc:        val(l.IFSCODE)    || null,
      swiftCode:   val(l.SWIFTCODE)  || null,
      bankName:    val(l.BANKNAME)   || null,
      address: (() => {
        // Try MAILINGDETAILS.LIST first (multi-line address), fall back to ADDRESS
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
        return extractAddress(l.ADDRESS) || null;
      })(),
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
    "NAME,GUID,PARENT,NUMBERINGMETHOD,ISOPTIONAL,ISACTIVE,COMMONNARRATION,AFFECTSSTOCK",
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
        parent: val(v.PARENT),
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

  const items = itemsArr
    .map((s) => {
      const name = s.$?.NAME || val(s.NAME) || null;
      if (!name) return null;
      const { hsnCode, gstRate } = extractHsnDetails(s);
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
  const tallyFrom = fromDate ? String(fromDate).replace(/-/g, "") : "20160101";
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
      <FETCH>GUID,DATE,VOUCHERNUMBER,VOUCHERTYPENAME,PARTYLEDGERNAME,NARRATION,REFERENCE,ISINVOICE,ISOPTIONAL,ISPOSTDATED,ALLLEDGERENTRIES.LIST,ALLINVENTORYENTRIES.LIST,INVENTORYENTRIES.LIST</FETCH>
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
      if (ledgerName) entries.push({ ledger: ledgerName, amount, isDebit: isDeemedPositive });
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
    // parseTallyQty / parseTallyRate strip the UoM suffix before parsing.

    function parseTallyQty(raw) {
      if (!raw) return 1;
      // "1 Nos", "2.5 Kgs", "10 Box" → keep only leading number
      const n = parseFloat(String(raw).replace(/[^0-9.\-]/g, "").trim());
      return isNaN(n) || n === 0 ? 1 : Math.abs(n);
    }
    function parseTallyRate(raw) {
      if (!raw) return 0;
      // "79.00 /Nos", "150.00/ Pcs" → keep only leading number
      const n = parseFloat(String(raw).replace(/[^0-9.\-]/g, "").trim());
      return isNaN(n) ? 0 : Math.abs(n);
    }

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

      inventoryItems.push({
        itemName,
        qty:    qty,
        rate:   Math.abs(finalRate),
        amount: Math.abs(finalAmount),
      });
    }

    vouchers.push({
      guid: guid || `voucher-${vouchers.length}`,
      voucherDate:    tallyDateToISO(val(v.DATE)),
      voucherType:    val(v.VOUCHERTYPENAME) || val(v.VOUCHERTYPE),
      voucherNumber:  val(v.VOUCHERNUMBER),
      referenceNo:    val(v.REFERENCE),
      partyName:      val(v.PARTYLEDGERNAME),
      narration:      val(v.NARRATION),
      netAmount,
      entries,
      inventoryItems,                          // ← real item rows from Tally
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
  try {
    logger.info(`Voucher check: fetching ALL vouchers`, { company: companyName });
    const vouchers = await fetchTallyVouchers(companyName);
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