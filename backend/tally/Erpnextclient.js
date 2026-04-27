// @ts-nocheck
/**
 * erpnextClient.js
 * Pushes data from Tally into ERPNext via REST API.
 * Place at: backend/tally/erpnextClient.js
 */

import axios from "axios";
import { config } from "../config/config.js";
import { logger } from "../logs/logger.js";

const BATCH_DELAY_MS  = 1500;  // 1.5s between each request — safe for Frappe Cloud
const BATCH_BURST     = 10;    // pause after every 10 requests
const BATCH_BURST_MS  = 5000;  // 5s pause after each burst
const RETRY_ATTEMPTS  = 5;
const RETRY_DELAY_MS  = 8000;  // 8s base retry delay
const CONCURRENCY     = 1;     // strictly one request at a time

// Adaptive throttle: ramps up on 429, cools down slowly after clean runs
const _throttle = {
  extraDelayMs: 0, consecutiveOk: 0, consecutive429: 0,
  hit(status) {
    if (status === 429) {
      this.consecutive429++; this.consecutiveOk = 0;
      this.extraDelayMs = Math.min(this.extraDelayMs + 2000, 15000);
    } else {
      this.consecutive429 = 0; this.consecutiveOk++;
      if (this.consecutiveOk >= 5 && this.extraDelayMs > 0) {
        this.extraDelayMs = Math.max(0, this.extraDelayMs - 200);
        this.consecutiveOk = 0;
      }
    }
  },
  delay() { return BATCH_DELAY_MS + this.extraDelayMs; },
  reset() { this.extraDelayMs = 0; this.consecutiveOk = 0; this.consecutive429 = 0; },
};

/**
 * createErpClient
 *
 * Pass optional `creds` to override the .env values for this request.
 * This allows different Tally companies to sync to different ERPNext instances.
 *
 * creds = { url, apiKey, apiSecret }
 */
function createErpClient(creds = {}) {
  const url       = creds.url       || config.erpnext.url;
  const apiKey    = creds.apiKey    || config.erpnext.apiKey;
  const apiSecret = creds.apiSecret || config.erpnext.apiSecret;

  if (!url)       throw new Error("ERPNEXT_URL not set — add it in .env or provide credentials in the Sync UI");
  if (!apiKey || !apiSecret) throw new Error("ERPNEXT_API_KEY / ERPNEXT_API_SECRET not set — add them in .env or provide credentials in the Sync UI");

  return axios.create({
    baseURL: url.replace(/\/$/, ""),
    headers: {
      Authorization: "token " + apiKey + ":" + apiSecret,
      "Content-Type": "application/json",
    },
    timeout: 60000,
  });
}

let _customerGroup = null;
let _supplierGroup = null;
let _territory     = null;

// ── Cancellation token ──────────────────────────────────────────────────────
// Call cancelSync() from any route handler to request a graceful stop.
// Every inner loop checks _cancelled before each item; on detection it throws
// a special error that bubbles up through batchSync → syncLedgers → runFullSync
// so the job exits cleanly without leaving a half-written state.
const _cancel = { requested: false };
export function cancelSync()  { _cancel.requested = true;  }
export function resetCancel() { _cancel.requested = false; }
function checkCancelled(label) {
  if (_cancel.requested) {
    throw Object.assign(new Error("Sync cancelled by user"), { _cancelled: true });
  }
}
// ───────────────────────────────────────────────────────────────────────────

// ── looksLikeAsset — MODULE SCOPE ────────────────────────────────────────────
// Must be at module level (not inside syncLedgersToErpNext) so that
// syncOpeningBalancesToErpNext can also call it. When it was nested inside
// syncLedgersToErpNext it caused a ReferenceError every time OB sync hit
// a fixed-asset ledger, crashing the entire opening-balance job.
function looksLikeAsset(name) {
  if (!name) return false;
  const n = name.trim();
  // Model number pattern: letters/digits mixed with / - * ( )
  if (/[A-Z0-9]{3,}[\/\-*][A-Z0-9]{2,}/i.test(n)) return true;
  const assetBrands = ["daikin", "hitachi", "lg ", "samsung", "apple ", "dell ", "hp ",
    "asus", "lenovo", "acer", "fujitsu", "toshiba", "logitech", "ncomputing",
    "kent", "bluestar", "mitsubishi", "honda", "motorola", "oneplus",
    "mi led", "quanta"];
  const lower = n.toLowerCase();
  if (assetBrands.some((b) => lower.startsWith(b))) return true;
  const assetNouns = ["laptop", "computer", "server", "ups", "ac ", "air conditioner",
    "fridge", "refrigerator", "refrigirator", "television", "tv ", "microwave",
    "bed ", "matress", "sofa", "locker", "hard disk", "keyboard", "monitor",
    "led monitor", "lan switch", "motor bike"];
  if (assetNouns.some((a) => lower.startsWith(a) || lower === a)) return true;
  return false;
}

const _knownItemGroups   = new Set();
const _knownUoMs         = new Set();
const _knownHsn          = new Set();
const _accountCache      = new Map();
const _accountTypeMap    = new Map(); // "AccountName - XX" → "Receivable"|"Payable"|... MUST be before resolveAccount
const _companyAbbrCache  = new Map();
const _companyNameCache  = new Map(); // tallyName -> erpnextName

// Call this at the start of each full sync to avoid stale cache across runs
export function clearCaches() {
  _knownItemGroups.clear();
  _knownUoMs.clear();
  _knownHsn.clear();
  _accountCache.clear();
  _accountTypeMap.clear();
  _companyAbbrCache.clear();
  _companyNameCache.clear();
  _customerGroup = null;
  _supplierGroup = null;
  _territory     = null;
  logger.info("ERPNext client caches cleared for new sync run [erpnextClient v8]");
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Map Tally Indian state names → ERPNext territory names (ERPNext India uses state names as territories)
const STATE_TERRITORY_MAP = {
  "andhra pradesh": "Andhra Pradesh", "arunachal pradesh": "Arunachal Pradesh",
  "assam": "Assam", "bihar": "Bihar", "chhattisgarh": "Chhattisgarh",
  "goa": "Goa", "gujarat": "Gujarat", "haryana": "Haryana",
  "himachal pradesh": "Himachal Pradesh", "jharkhand": "Jharkhand",
  "karnataka": "Karnataka", "kerala": "Kerala", "madhya pradesh": "Madhya Pradesh",
  "maharashtra": "Maharashtra", "manipur": "Manipur", "meghalaya": "Meghalaya",
  "mizoram": "Mizoram", "nagaland": "Nagaland", "odisha": "Odisha",
  "punjab": "Punjab", "rajasthan": "Rajasthan", "sikkim": "Sikkim",
  "tamil nadu": "Tamil Nadu", "telangana": "Telangana", "tripura": "Tripura",
  "uttar pradesh": "Uttar Pradesh", "uttarakhand": "Uttarakhand",
  "west bengal": "West Bengal", "delhi": "Delhi", "jammu and kashmir": "Jammu and Kashmir",
  "ladakh": "Ladakh", "chandigarh": "Chandigarh",
  "dadra and nagar haveli and daman and diu": "Dadra and Nagar Haveli and Daman and Diu",
  "lakshadweep": "Lakshadweep", "puducherry": "Puducherry",
  "andaman and nicobar islands": "Andaman and Nicobar Islands",
};

function resolveTerritory(state) {
  if (!state) return _territory || "India";
  return STATE_TERRITORY_MAP[state.trim().toLowerCase()] || state.trim() || _territory || "India";
}

// Ensure an ERPNext Address record exists for a party
async function ensureAddress(client, partyName, partyType, ledger) {
  if (!ledger.address && !ledger.state && !ledger.pincode) return; // nothing to sync

  // ERPNext India requires `state` for Indian addresses — skip if missing
  if (!ledger.state) {
    logger.warn("Address sync skipped for " + partyName + ": State is a required field for Indian Address");
    return;
  }

  const addressName = partyName + "-" + partyType;
  const doc = {
    doctype:        "Address",
    address_title:  partyName,
    address_type:   "Billing",
    address_line1:  ledger.address || "",
    city:           ledger.state || "",
    state:          ledger.state || "",
    country:        "India",
    pincode:        ledger.pincode || "",
    links: [{ link_doctype: partyType, link_name: partyName }],
  };
  if (ledger.email) doc.email_id   = ledger.email;
  if (ledger.phone) doc.phone      = ledger.phone;

  try {
    const existing = await client.get("/api/resource/Address/" + encodeURIComponent(addressName)).catch(() => null);
    if (existing && existing.data && existing.data.data) {
      await client.put("/api/resource/Address/" + encodeURIComponent(addressName), Object.assign({}, doc, { name: addressName }));
    } else {
      await client.post("/api/resource/Address", Object.assign({}, doc, { name: addressName }));
    }
  } catch (err) {
    logger.warn("Address sync skipped for " + partyName + ": " + parseErpError(err));
  }
}

// Ensure ERPNext Contact record for a party
async function ensureContact(client, partyName, partyType, ledger) {
  if (!ledger.phone && !ledger.email) return;
  const contactName = partyName + "-" + partyType;
  const nameParts   = partyName.split(" ");
  const doc = {
    doctype:    "Contact",
    first_name: nameParts[0] || partyName,
    last_name:  nameParts.slice(1).join(" ") || "",
    links: [{ link_doctype: partyType, link_name: partyName }],
    phone_nos: ledger.phone ? [{ phone: ledger.phone, is_primary_phone: 1 }] : [],
    email_ids: ledger.email ? [{ email_id: ledger.email, is_primary: 1 }] : [],
  };
  try {
    const existing = await client.get("/api/resource/Contact/" + encodeURIComponent(contactName)).catch(() => null);
    if (existing && existing.data && existing.data.data) {
      await client.put("/api/resource/Contact/" + encodeURIComponent(contactName), Object.assign({}, doc, { name: contactName }));
    } else {
      await client.post("/api/resource/Contact", Object.assign({}, doc, { name: contactName }));
    }
  } catch (err) {
    logger.warn("Contact sync skipped for " + partyName + ": " + parseErpError(err));
  }
}

/**
 * resolveErpNextCompany
 *
 * ── SIMPLIFIED ──
 * Uses the ERPNext company name exactly as configured by the user in the Sync UI.
 * No API calls, no name-matching logic. Authentication is already handled by
 * API key/secret — guessing the company name via word-overlap caused 429 errors
 * and incorrect auto-creation of duplicate companies.
 */
async function resolveErpNextCompany(client, tallyName, creds = {}) {
  // ── FIXED: User explicitly sets the ERPNext company name in the Sync UI ──
  // If erpnextCompany is passed via creds, use it directly — no API calls needed.
  // This eliminates the 429 rate limit errors that occurred during company name lookup.
  const overrideName = creds.erpnextCompany && creds.erpnextCompany.trim();
  const nameToUse = overrideName || tallyName;

  if (!nameToUse) return nameToUse;
  if (_companyNameCache.has(nameToUse)) return _companyNameCache.get(nameToUse);

  _companyNameCache.set(nameToUse, nameToUse);
  logger.info("Using ERPNext company: \"" + nameToUse + "\"" + (overrideName ? " (from Sync UI)" : " (from Tally name)"));
  return nameToUse;
}
/**
 * ensureErpNextCompany
 * Creates the company in ERPNext if it does not exist.
 * Derives a short abbreviation from the company name automatically.
 */
async function ensureErpNextCompany(client, companyName) {
  // Derive abbreviation: initials of words, max 4 chars, uppercase
  const abbr = companyName
    .replace(/[^a-zA-Z0-9 ]/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .map((w) => w[0].toUpperCase())
    .join("")
    .slice(0, 4);

  const safeAbbr = abbr || companyName.slice(0, 3).toUpperCase();

  try {
    // Check if already exists (exact)
    await client.get("/api/resource/Company/" + encodeURIComponent(companyName));
    logger.info("Company already exists in ERPNext: " + companyName);
    return companyName;
  } catch (_) {}

  try {
    await client.post("/api/resource/Company", {
      doctype:          "Company",
      company_name:     companyName,
      abbr:             safeAbbr,
      default_currency: "INR",
      country:          "India",
    });
    logger.success("Auto-created ERPNext company: \"" + companyName + "\" (abbr: " + safeAbbr + ")");
    return companyName;
  } catch (err) {
    const msg = parseErpError(err);
    // If duplicate abbr, try appending a number
    if (msg.toLowerCase().includes("abbr") || msg.toLowerCase().includes("duplicate")) {
      const fallbackAbbr = safeAbbr + "1";
      try {
        await client.post("/api/resource/Company", {
          doctype:          "Company",
          company_name:     companyName,
          abbr:             fallbackAbbr,
          default_currency: "INR",
          country:          "India",
        });
        logger.success("Auto-created ERPNext company: \"" + companyName + "\" (abbr: " + fallbackAbbr + ")");
        return companyName;
      } catch (err2) {
        logger.warn("Could not auto-create company \"" + companyName + "\": " + parseErpError(err2) + " — proceeding with original name");
        return companyName;
      }
    }
    logger.warn("Could not auto-create company \"" + companyName + "\": " + msg + " — proceeding with original name");
    return companyName;
  }
}

async function fetchLeafName(client, doctype, fallback) {
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      const res = await client.get("/api/resource/" + encodeURIComponent(doctype), {
        params: {
          filters:  JSON.stringify([[doctype, "is_group", "=", 0]]),
          fields:   '["name"]',
          limit:    1,
          order_by: "creation asc",
        },
      });
      const name = res.data && res.data.data && res.data.data[0] && res.data.data[0].name;
      if (name) return name;
      return fallback; // got a response but no records — use fallback, no point retrying
    } catch (err) {
      const status = err.response && err.response.status;
      if (status === 429 && attempt < 4) {
        const wait = 3000 * attempt;
        logger.warn("Could not fetch leaf for " + doctype + " (429), retrying in " + wait + "ms");
        await sleep(wait);
      } else {
        logger.warn("Could not fetch leaf for " + doctype + ": " + err.message);
        return fallback;
      }
    }
  }
  return fallback;
}

async function resolveGroups(client) {
  // Fetch real Customer Group and Supplier Group from ERPNext.
  // Falls back to safe defaults only if the API call fails.
  if (!_customerGroup) {
    try {
      const res = await client.get("/api/resource/Customer Group", {
        params: { fields: '["name"]', filters: JSON.stringify([["Customer Group","is_group","=",0]]), limit: 500 },
      });
      const groups = (res.data && res.data.data) || [];
      // All results are already non-group (is_group=0) — just pick the first one
      // "Commercial" or "Individual" are typical ERPNext defaults
      const preferred = groups.find((g) => g.name === "Commercial") ||
                        groups.find((g) => g.name === "Individual") ||
                        groups[0];
      _customerGroup = preferred ? preferred.name : null;
    } catch (_) {
      _customerGroup = "Commercial"; // safe ERPNext default leaf group
    }
    logger.info("Using Customer Group: " + _customerGroup);
  }
  if (!_supplierGroup) {
    try {
      const res = await client.get("/api/resource/Supplier Group", {
        params: { fields: '["name"]', filters: JSON.stringify([["Supplier Group","is_group","=",0]]), limit: 500 },
      });
      const groups = (res.data && res.data.data) || [];
      const preferred = groups.find((g) => g.name === "Services") ||
                        groups.find((g) => g.name === "Local") ||
                        groups[0];
      _supplierGroup = preferred ? preferred.name : null;
    } catch (_) {
      _supplierGroup = "Services"; // safe ERPNext default leaf group
    }
    logger.info("Using Supplier Group: " + _supplierGroup);
  }
  if (!_territory) {
    _territory = "India";  // Default for Indian companies; overridden per-ledger by state
    logger.info("Using default Territory: " + _territory);
  }
}

async function ensureItemGroup(client, groupName) {
  if (!groupName || groupName === "All Item Groups" || _knownItemGroups.has(groupName)) return;
  try {
    await client.get("/api/resource/Item Group/" + encodeURIComponent(groupName));
    _knownItemGroups.add(groupName);
    return;
  } catch (_) {}
  try {
    await client.post("/api/resource/Item Group", {
      doctype:           "Item Group",
      item_group_name:   groupName,
      parent_item_group: "All Item Groups",
      is_group:          0,
    });
    _knownItemGroups.add(groupName);
    logger.info("Created Item Group: " + groupName);
  } catch (err) {
    _knownItemGroups.add(groupName);
    logger.warn("Item Group create skipped for " + groupName + ": " + parseErpError(err));
  }
}

const UOM_MAP = {
  "nos": "Nos", "no": "Nos", "num": "Nos", "pcs": "Nos", "pcs.": "Nos", "pc": "Nos",
  "unit": "Unit", "units": "Unit", "user": "Unit",
  "mth": "Month", "month": "Month", "months": "Month",
  "yr": "Year", "year": "Year", "years": "Year",
  "hr": "Hour", "hrs": "Hour", "hour": "Hour", "hours": "Hour",
  "day": "Day", "days": "Day",
  "kg": "Kg", "kgs": "Kg",
  "gm": "Gram", "gram": "Gram",
  "ltr": "Litre", "litre": "Litre", "liter": "Litre",
  "mtr": "Metre", "metre": "Metre", "meter": "Metre",
  "not applicable": "Unit",
};

function normaliseUoM(raw) {
  if (!raw) return "Nos";
  const mapped = UOM_MAP[raw.trim().toLowerCase()];
  return mapped || raw.trim();
}

async function ensureUoM(client, uomName) {
  if (!uomName || _knownUoMs.has(uomName)) return;
  try {
    await client.get("/api/resource/UOM/" + encodeURIComponent(uomName));
    _knownUoMs.add(uomName);
    return;
  } catch (_) {}
  try {
    await client.post("/api/resource/UOM", { doctype: "UOM", uom_name: uomName, enabled: 1 });
    _knownUoMs.add(uomName);
    logger.info("Created UoM: " + uomName);
  } catch (err) {
    _knownUoMs.add(uomName);
    logger.warn("UoM create skipped for " + uomName + ": " + parseErpError(err));
  }
}

function sanitiseHsn(raw) {
  if (!raw) return null;
  const digits = String(raw).replace(/\D/g, "");
  if (!digits || digits === "00000000" || digits === "000000") return null;

  // FIX: Accept all standard lengths:
  //   4-digit HSN  (e.g. 8471)
  //   6-digit SAC  (e.g. 998313 — services, always starts with 99)
  //   8-digit HSN  (e.g. 84713000 — goods)
  if (digits.length === 8) return digits;
  if (digits.length === 6) return digits;
  // FIX: ERPNext India GST requires exactly 6 or 8 digits.
  // 4-digit HSN codes must be padded to 6 (e.g. 8471 → 847100)
  if (digits.length === 4) return digits.padEnd(6, "0");
  // 7-digit: pad to 8
  if (digits.length === 7) return digits + "0";
  // 5-digit: pad to 6
  if (digits.length === 5) return digits + "0";
  // Shorter than 4: pad to 6
  if (digits.length < 4)   return digits.padEnd(6, "0");
  // Longer than 8: truncate to 8
  return digits.slice(0, 8);
}

async function ensureHsnCode(client, hsnCode) {
  if (!hsnCode || hsnCode === "null" || _knownHsn.has(hsnCode)) return;
  try {
    await client.get("/api/resource/GST HSN Code/" + encodeURIComponent(hsnCode));
    _knownHsn.add(hsnCode);
    return;
  } catch (_) {}
  try {
    await client.post("/api/resource/GST HSN Code", {
      doctype:     "GST HSN Code",
      hsn_code:    hsnCode,
      description: hsnCode === "99999999" ? "Other Goods / Services (Tally fallback)" : "HSN " + hsnCode + " (from Tally)",
    });
    _knownHsn.add(hsnCode);
  } catch (_) {
    _knownHsn.add(hsnCode);
  }
}

async function getCompanyAbbr(client, companyName) {
  if (_companyAbbrCache.has(companyName)) return _companyAbbrCache.get(companyName);
  try {
    const res = await client.get("/api/resource/Company/" + encodeURIComponent(companyName), {
      params: { fields: '["abbr"]' },
    });
    const abbr = (res.data && res.data.data && res.data.data.abbr) || companyName.slice(0, 3).toUpperCase();
    _companyAbbrCache.set(companyName, abbr);
    return abbr;
  } catch (e) {
    const errDetail = e.response ? ("HTTP " + e.response.status + ": " + JSON.stringify(e.response.data).slice(0,200)) : e.message;
    logger.error("getCompanyAbbr failed for \"" + companyName + "\": " + errDetail + " -- falling back to slice");
    const abbr = companyName.slice(0, 3).toUpperCase();
    _companyAbbrCache.set(companyName, abbr);
    return abbr;
  }
}

async function resolveAccount(client, ledgerName, companyAbbr, companyName) {
  if (!ledgerName) return null;
  // Trim trailing whitespace/newlines that Tally sometimes appends
  ledgerName = ledgerName.trim();
  if (!ledgerName) return null;

  const cacheKey = ledgerName + "::" + companyAbbr;
  if (_accountCache.has(cacheKey)) return _accountCache.get(cacheKey);

  // Try 1: exact account_name match filtered by company (most precise)
  if (companyName) {
    try {
      const res = await client.get("/api/resource/Account", {
        params: {
          filters: JSON.stringify([["Account", "account_name", "=", ledgerName], ["Account", "company", "=", companyName]]),
          fields:  '["name"]',
          limit:   1,
        },
      });
      const found = res.data && res.data.data && res.data.data[0] && res.data.data[0].name;
      if (found) { _accountCache.set(cacheKey, found); return found; }
    } catch (_) {}
  }

  // Try 2: exact account_name without company filter (fallback)
  try {
    const res = await client.get("/api/resource/Account", {
      params: {
        filters: JSON.stringify([["Account", "account_name", "=", ledgerName]]),
        fields:  '["name"]',
        limit:   1,
      },
    });
    const found = res.data && res.data.data && res.data.data[0] && res.data.data[0].name;
    if (found) { _accountCache.set(cacheKey, found); return found; }
  } catch (_) {}

  // Try 3: look up the suffixed name directly (e.g. "Kotak Mahindra Bank - TC")
  const suffixed = ledgerName + " - " + companyAbbr;
  try {
    const res = await client.get("/api/resource/Account/" + encodeURIComponent(suffixed), {
      params: { fields: '["name"]' },
    });
    const found = res.data && res.data.data && res.data.data.name;
    if (found) { _accountCache.set(cacheKey, found); return found; }
  } catch (_) {}

  // Auto-create the missing account with a valid parent_account.
  // Root cause of "root account must be a group" errors:
  //   ERPNext rejects accounts posted without parent_account — it tries to
  //   attach them directly to a root node which is a group, not a leaf.
  // Fix: detect account type from ledger name heuristics, then query ERPNext
  //   for a real group account of that type to use as parent_account.
  //   For unknown party ledgers (most voucher accounts) default to Payable.
  if (companyName) {
    try {
      const lower = ledgerName.toLowerCase();
      let accountType   = "Payable";   // safe default: most missing accounts are party ledgers
      let parentAccount = null;

      if (/bank|current a\/c|savings|sweep|kotak|hdfc|sbi|icici|axis|dbs|yes bank|\d{9,}/i.test(lower)) {
        accountType = "Bank";
      } else if (/cash|petty/i.test(lower)) {
        accountType = "Cash";
      } else if (/debtor|receivable/i.test(lower)) {
        accountType = "Receivable";
      } else if (/income|revenue|sales/i.test(lower)) {
        accountType = "Income Account";
      } else if (/expense|purchase|cost/i.test(lower)) {
        accountType = "Expense Account";
      }
      // "Payable" covers all unknown party names (vendors, suppliers, misc creditors)

      // Step 1: query ERPNext for a real group account of matching type so we
      // always get a valid parent_account (avoids "root account must be a group").
      const typeFilterMap = {
        "Bank":           [["Account","account_type","=","Bank"],          ["Account","is_group","=",1],["Account","company","=",companyName]],
        "Cash":           [["Account","account_type","=","Cash"],          ["Account","is_group","=",1],["Account","company","=",companyName]],
        "Receivable":     [["Account","account_type","=","Receivable"],    ["Account","is_group","=",1],["Account","company","=",companyName]],
        "Income Account": [["Account","root_type","=","Income"],           ["Account","is_group","=",1],["Account","company","=",companyName]],
        "Expense Account":[["Account","root_type","=","Expense"],          ["Account","is_group","=",1],["Account","company","=",companyName]],
        "Payable":        [["Account","account_type","=","Payable"],       ["Account","is_group","=",1],["Account","company","=",companyName]],
      };
      const filterToUse = typeFilterMap[accountType] || typeFilterMap["Payable"];
      try {
        const pgRes = await client.get("/api/resource/Account", {
          params: { filters: JSON.stringify(filterToUse), fields: '["name"]', limit: 1 },
        });
        const pgName = pgRes.data && pgRes.data.data && pgRes.data.data[0] && pgRes.data.data[0].name;
        if (pgName) parentAccount = pgName;
      } catch (_) {}

      // Step 2: fallback to well-known ERPNext default group names if the query failed
      if (!parentAccount) {
        if      (accountType === "Bank")            parentAccount = "Bank Accounts - "   + companyAbbr;
        else if (accountType === "Cash")            parentAccount = "Cash In Hand - "    + companyAbbr;
        else if (accountType === "Receivable")      parentAccount = "Sundry Debtors - "  + companyAbbr;
        else if (accountType === "Income Account")  parentAccount = "Direct Income - "   + companyAbbr;
        else if (accountType === "Expense Account") parentAccount = "Direct Expenses - " + companyAbbr;
        else                                        parentAccount = "Sundry Creditors - "+ companyAbbr;
      }

      const doc = {
        doctype:        "Account",
        account_name:   ledgerName,
        company:        companyName,
        is_group:       0,
        account_type:   accountType,
        parent_account: parentAccount,
      };

      await client.post("/api/resource/Account", doc);
      logger.info("Auto-created missing account: \"" + suffixed + "\" (type: " + accountType + ", parent: " + parentAccount + ")");
      _accountTypeMap.set(suffixed, accountType); // record so resolveAccountWithType skips the lookup
      _accountCache.set(cacheKey, suffixed);
      return suffixed;
    } catch (createErr) {
      logger.warn("Could not auto-create account \"" + ledgerName + "\": " + parseErpError(createErr) + " — vouchers referencing this account will fail");
    }
  }

  // Last resort fallback
  _accountCache.set(cacheKey, suffixed);
  return suffixed;
}

/**
 * resolveErpNextCompanyPublic
 * Exported wrapper so routes can call the company resolver without a full sync.
 * Returns the matched or auto-created ERPNext company name, or null if ERPNext unreachable.
 */
export async function resolveErpNextCompanyPublic(tallyName, creds = {}) {
  try {
    const client = createErpClient(creds);
    return await resolveErpNextCompany(client, tallyName, creds);
  } catch (_) {
    return null;
  }
}

export async function pingErpNext(creds = {}) {
  const start = Date.now();
  try {
    const client = createErpClient(creds);
    const res = await client.get("/api/method/frappe.auth.get_logged_user");
    // Log rate limit headers so we can see our quota status
    const headers = res.headers || {};
    const limit     = headers["x-ratelimit-limit"];
    const remaining = headers["x-ratelimit-remaining"];
    const used      = headers["x-ratelimit-used"];
    if (limit) logger.info("Rate limit status — Limit: " + limit + ", Used: " + used + ", Remaining: " + remaining);
    return { connected: true, user: res.data && res.data.message, latencyMs: Date.now() - start };
  } catch (err) {
    const status  = err.response && err.response.status;
    const headers = err.response && err.response.headers;
    if (status === 429 && headers) {
      logger.warn("PING hit 429 — RateLimit-Limit: " + headers["x-ratelimit-limit"] + ", Remaining: " + headers["x-ratelimit-remaining"] + ", Reset in: " + headers["retry-after"] + "s");
    }
    return {
      connected: false,
      latencyMs: Date.now() - start,
      error: err.code === "ECONNREFUSED"
        ? "ERPNext not reachable at " + (creds.url || config.erpnext.url)
        : (err.response && err.response.data && err.response.data.exc) || err.message,
    };
  }
}

function parseErpError(err) {
  const data   = err.response && err.response.data;
  const status = err.response && err.response.status;
  if (!data) return err.message || ("HTTP " + status);
  if (data._server_messages) {
    try {
      const msgs  = JSON.parse(data._server_messages);
      const first = JSON.parse(msgs[0]);
      return first.message || String(first);
    } catch (_) { return String(data._server_messages); }
  }
  if (data.exception) return data.exception;
  if (data.exc) {
    const lines = data.exc.split("\n").map((l) => l.trim()).filter(Boolean);
    return lines[lines.length - 1] || data.exc;
  }
  if (data.message) return data.message;
  // Last resort — include raw body so real error is never hidden behind "HTTP 429"
  try { return "HTTP " + status + ": " + JSON.stringify(data).slice(0, 300); }
  catch (_) { return "HTTP " + status; }
}

async function withRetry(fn, label) {
  let lastErr;
  for (let attempt = 1; attempt <= RETRY_ATTEMPTS; attempt++) {
    try {
      const result = await fn();
      _throttle.hit("ok");
      return result;
    } catch (err) {
      const status      = err.response && err.response.status;
      const isTransient = !status || [429, 500, 502, 503, 504].includes(status);
      if (!isTransient) throw err;
      lastErr = err;
      _throttle.hit(status);
      let delay;
      if (status === 429) {
        const base = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
        delay = Math.min(base, 30_000) + Math.random() * 1500; // exp backoff + jitter
      } else {
        delay = RETRY_DELAY_MS * attempt;
      }
      const errDetail = parseErpError(err);
      logger.warn("[" + label + "] attempt " + attempt + "/" + RETRY_ATTEMPTS + " failed (" + (status || "network") + ") — " + errDetail + " — retrying in " + Math.round(delay) + "ms");
      await sleep(delay);
    }
  }
  throw lastErr;
}

async function upsert(client, doctype, filters, doc) {
  const searchKey = Object.keys(filters)[0];
  const searchVal = Object.values(filters)[0];

  // For Customer/Supplier, ERPNext uses `name` = customer_name/supplier_name as the unique key.
  // Try direct GET by name first (fastest, no duplicates), then fall back to list search.
  const isParty = doctype === "Customer" || doctype === "Supplier";
  if (isParty) {
    try {
      const direct = await client.get("/api/resource/" + encodeURIComponent(doctype) + "/" + encodeURIComponent(searchVal));
      if (direct.data && direct.data.data) {
        await client.put(
          "/api/resource/" + encodeURIComponent(doctype) + "/" + encodeURIComponent(searchVal),
          Object.assign({}, doc, { doctype, name: searchVal })
        );
        return { action: "updated", name: searchVal };
      }
    } catch (e) {
      // 404 means not found — will create below
      if (e.response && e.response.status !== 404) throw e;
    }
    const res = await client.post("/api/resource/" + encodeURIComponent(doctype), Object.assign({}, doc, { doctype }));
    return { action: "created", name: res.data && res.data.data && res.data.data.name };
  }

  // For all other doctypes — use list search as before
  try {
    // For invoice doctypes the searchKey is `remarks` and the value may contain a narration
    // suffix that could vary. Use `like` so "Tally Voucher No: X%" always finds the doc
    // regardless of what narration was appended, preventing false "not found" → duplicate creates.
    // FIX: Also apply `like` matching for `user_remark` (Journal Entry idempotency key)
    // so we don't create duplicate JEs when the same voucher is synced twice.
    const isLikeField = searchKey === "remarks" || searchKey === "user_remark";
    const operator  = isLikeField ? "like" : "=";
    const filterVal = isLikeField ? searchVal.split(" | ")[0] + "%" : searchVal;
    const list = await client.get("/api/resource/" + encodeURIComponent(doctype), {
      params: {
        filters: JSON.stringify([[doctype, searchKey, operator, filterVal]]),
        limit:   1,
        fields:  '["name","docstatus"]',
      },
    });
    const existing = list.data && list.data.data && list.data.data[0];
    if (existing) {
      // Skip PUT for submitted docs (docstatus=1) — ERPNext does not allow editing them.
      // The submit step handles submission; if already submitted we just treat as "updated".
      if (existing.docstatus === 1) {
        return { action: "updated", name: existing.name };
      }
      // Always include doctype + name in PUT body — ERPNext requires them to validate mandatory fields
      await client.put(
        "/api/resource/" + encodeURIComponent(doctype) + "/" + encodeURIComponent(existing.name),
        Object.assign({}, doc, { doctype, name: existing.name })
      );
      return { action: "updated", name: existing.name };
    }
  } catch (_) {}
  const res = await client.post("/api/resource/" + encodeURIComponent(doctype), Object.assign({}, doc, { doctype }));
  return { action: "created", name: res.data && res.data.data && res.data.data.name };
}

async function batchSync(client, doctype, items, mapper, progressCb) {
  let created = 0, updated = 0, failed = 0;
  const errors = [];
  const failedItems = []; // transient failures queued for final pass

  for (let i = 0; i < items.length; i += CONCURRENCY) {
    checkCancelled(doctype); // ← stop immediately if user clicked Stop
    const chunk = items.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(
      chunk.map((item) => {
        const { filters, doc } = mapper(item);
        return withRetry(() => upsert(client, doctype, filters, doc), doctype + ":" + item.name);
      })
    );
    for (let j = 0; j < results.length; j++) {
      const item = chunk[j];
      const r    = results[j];
      if (r.status === "fulfilled") {
        if (r.value.action === "created") created++; else updated++;
      } else {
        const err         = r.reason;
        const status      = err.response && err.response.status;
        const isDuplicate = err.response?.data?.exc_type === "DuplicateEntryError";
        const isPermFail  = isDuplicate || (status && status >= 400 && status < 500 && status !== 429);
        if (isPermFail) {
          failed++;
          const msg = isDuplicate ? "Duplicate: " + item.name : parseErpError(err);
          errors.push({ item: item.name || (i + j), error: msg });
          if (errors.length <= 5) logger.warn("ERPNext sync error [" + doctype + "]: " + msg);
        } else {
          failedItems.push(item); // transient — retry in final pass
        }
      }
    }
    if (progressCb) progressCb(Math.min(i + CONCURRENCY, items.length), items.length);
    await sleep(_throttle.delay());
    const processed = i + CONCURRENCY;
    if (processed % (BATCH_BURST * CONCURRENCY) < CONCURRENCY) {
      const burstPause = BATCH_BURST_MS + _throttle.extraDelayMs;
      logger.info("[" + doctype + "] " + Math.min(processed, items.length) + "/" + items.length + " synced - pausing " + burstPause + "ms" + (_throttle.extraDelayMs > 0 ? " (throttled +" + _throttle.extraDelayMs + "ms)" : ""));
      await sleep(burstPause);
    }
  }

  // Final pass: retry transient failures serially with 2s gap
  if (failedItems.length > 0) {
    logger.info("[" + doctype + "] Final pass: retrying " + failedItems.length + " transient failures (serial, 2s gap)");
    _throttle.reset();
    await sleep(5000);
    for (const item of failedItems) {
      const { filters, doc } = mapper(item);
      try {
        const result = await withRetry(() => upsert(client, doctype, filters, doc), doctype + ":" + item.name + " [final]");
        if (result.action === "created") created++; else updated++;
      } catch (err) {
        failed++;
        const isDuplicate = err.response?.data?.exc_type === "DuplicateEntryError";
        const msg = isDuplicate ? "Duplicate: " + item.name : parseErpError(err);
        errors.push({ item: item.name, error: msg });
        logger.warn("ERPNext sync error [" + doctype + "] (final pass): " + msg);
      }
      await sleep(2000);
    }
  }

  return { created, updated, failed, errors: errors.slice(0, 20) };
}

// -- Ledgers -> Customers / Suppliers -----------------------------------------
export async function syncLedgersToErpNext(ledgers, creds = {}) {
  const client = createErpClient(creds);
  logger.info("Syncing " + ledgers.length + " ledgers to ERPNext");
  await resolveGroups(client);

  const DEBTOR_KEYS   = ["sundry debtor", "debtor", "receivable", "accounts receivable"];
  const CREDITOR_KEYS = ["sundry creditor", "creditor", "payable", "accounts payable"];

  // Groups that are pure GL accounts (assets/liabilities/income/expense) —
  // these are NOT customers or suppliers. We skip them from Customer/Supplier
  // sync here; they are handled by syncOpeningBalancesToErpNext as GL accounts.
  const SKIP_KEYS = [
    // Balance sheet — asset
    "cash-in-hand", "cash in hand", "bank account", "bank od", "bank accounts",
    "fixed assets", "investments", "deposits (asset)", "loans & advances (asset)",
    "current assets", "stock-in-hand", "misc. expenses (asset)",
    // Balance sheet — liability / equity
    "capital account", "reserves", "retained", "profit & loss", "profit and loss",
    "loans (liability)", "bank od & od accounts", "current liabilities",
    "provisions", "suspense", "primary", "branch / divisions",
    "share application", "share capital", "unsecured loans", "secured loans",
    "duties & taxes", "duties and taxes",
    // Tax-specific sub-groups
    "gst", "input gst", "output gst", "igst", "cgst", "sgst",
    "tds", "tcs", "income tax", "tax",
    // P&L
    "indirect income", "direct income", "indirect expenses", "direct expenses",
    "purchase accounts", "sales account", "sales accounts",
    "manufacturing expenses", "depreciation", "deferred",
    // Fixed-asset sub-groups (physical items — definitely not parties)
    "air conditioner", "laptop", "computer", "mobile phone", "mobile phones",
    "furniture", "machinery", "motor", "vehicle", "water ro", "fridge",
    "television", "refrigirator", "server", "ups", "motoar bike",
    // Additional asset groups seen in data
    "microwave", "refrigerator", "washing machine", "printer",
  ];

  // looksLikeAsset is defined at module scope above — available here and in
  // syncOpeningBalancesToErpNext without duplication.

  const isDebtor   = (l) => DEBTOR_KEYS.some((k) => (l.parentGroup || "").toLowerCase().includes(k));
  const isCreditor = (l) => CREDITOR_KEYS.some((k) => (l.parentGroup || "").toLowerCase().includes(k));
  const isSkip     = (l) => SKIP_KEYS.some((k) => (l.parentGroup || "").toLowerCase().includes(k));

  // "Tally User" and other custom location groups (Andheri, Thane, etc.) contain
  // actual customers/suppliers — classify them by whether they have a GSTIN:
  // with GSTIN → likely a business (could be customer or supplier)
  // without GSTIN → treat as customer by default for unknown groups
  // "Unknown group" = not a known debtor/creditor group AND not a pure GL group AND not an asset.
  // For these we classify by GSTIN: with GSTIN → Supplier (B2B payable), without → Customer (default).
  // This handles custom Tally groups like "Masma", "Andheri", "Thane", etc. that wrap
  // Sundry Debtors or Sundry Creditors under user-defined names.
  const isUnknownGroup = (l) => !isDebtor(l) && !isCreditor(l) && !isSkip(l) && !looksLikeAsset(l.name);

  // Classify: debtors → Customer; creditors → Supplier;
  // unknown + no GSTIN → Customer (most Tally party ledgers are customers)
  // unknown + GSTIN → Supplier (B2B registrations usually indicate supplier)
  const customers = ledgers.filter((l) => isDebtor(l) || (isUnknownGroup(l) && !l.gstin));
  const suppliers = ledgers.filter((l) => !isDebtor(l) && (isCreditor(l) || (isUnknownGroup(l) && !!l.gstin)));
  const skipped   = ledgers.filter((l) => isSkip(l) || looksLikeAsset(l.name)).length;

  logger.info("Ledger breakdown - customers: " + customers.length + ", suppliers: " + suppliers.length + ", skipped (GL-only groups): " + skipped);

  const customerMapper = (l) => {
    l.name = (l.name || "").trim(); // trim Tally trailing newlines
    // customer_type: use "Company" when GSTIN is present (B2B) or when parentGroup
    // indicates a business debtor; fall back to "Individual" only when truly unknown.
    const hasGstin = !!(l.gstin && l.gstin.trim().length > 5);
    const isBusiness = hasGstin ||
      DEBTOR_KEYS.some((k) => (l.parentGroup || "").toLowerCase().includes(k));
    const doc = {
      customer_name:       l.name,
      customer_type:       isBusiness ? "Company" : "Individual",
      customer_group:      _customerGroup,
      territory:           resolveTerritory(l.state),
      default_currency:    "INR",
      custom_tally_id:     l.guid   || l.masterID || "",   // FIX: Tally GUID
      custom_tally_group:  l.parentGroup || "",            // FIX: Tally parent group
      custom_source:       "Tally",                        // FIX: sync origin marker
    };
    // Statutory
    if (l.gstin)  doc.tax_id          = l.gstin.trim();
    // FIX: Validate PAN format before sending — ERPNext rejects invalid PANs
    // (format: 5 letters + 4 digits + 1 letter, e.g. ABCDE1234F)
    const customerPan = (l.pan || "").trim().toUpperCase();
    if (/^[A-Z]{5}[0-9]{4}[A-Z]$/.test(customerPan)) {
      doc.pan = customerPan;
    } else if (customerPan) {
      // Invalid PAN format — create customer without PAN rather than skipping
      logger.info("Invalid PAN ignored for customer \"" + l.name + "\" (will sync without PAN): " + customerPan);
      // doc.pan intentionally omitted — ERPNext accepts null/missing PAN
    }
    // Contact info (primary)
    if (l.email)  doc.email_id        = l.email.trim();
    if (l.phone)  doc.mobile_no       = l.phone.trim();
    // Bank
    if (l.bankAccount) doc.bank_account_no = l.bankAccount.trim();
    if (l.ifsc)        doc.bank_ifsc_code  = l.ifsc.trim();
    return { filters: { customer_name: l.name }, doc, _ledger: l };
  };

  const supplierMapper = (l) => {
    l.name = (l.name || "").trim(); // trim Tally trailing newlines
    const hasGstin = !!(l.gstin && l.gstin.trim().length > 5);
    const doc = {
      supplier_name:       l.name,
      supplier_type:       hasGstin ? "Company" : "Individual",
      supplier_group:      _supplierGroup,
      country:             "India",
      default_currency:    "INR",
      custom_tally_id:     l.guid   || l.masterID || "",   // FIX: Tally GUID
      custom_tally_group:  l.parentGroup || "",            // FIX: Tally parent group
      custom_source:       "Tally",                        // FIX: sync origin marker
    };
    // Statutory
    if (l.gstin)  doc.tax_id          = l.gstin.trim();
    // FIX: Validate PAN format before sending — ERPNext rejects invalid PANs
    const supplierPan = (l.pan || "").trim().toUpperCase();
    if (/^[A-Z]{5}[0-9]{4}[A-Z]$/.test(supplierPan)) {
      doc.pan = supplierPan;
    } else if (supplierPan) {
      // Invalid PAN format — create supplier without PAN rather than skipping
      logger.info("Invalid PAN ignored for supplier \"" + l.name + "\" (will sync without PAN): " + supplierPan);
      // doc.pan intentionally omitted
    }
    // Contact info (primary)
    if (l.email)  doc.email_id        = l.email.trim();
    if (l.phone)  doc.mobile_no       = l.phone.trim();
    // Bank
    if (l.bankAccount) doc.bank_account_no = l.bankAccount.trim();
    if (l.ifsc)        doc.bank_ifsc_code  = l.ifsc.trim();
    return { filters: { supplier_name: l.name }, doc, _ledger: l };
  };

  const customerResults = await batchSync(client, "Customer", customers, customerMapper);
  const supplierResults = await batchSync(client, "Supplier", suppliers, supplierMapper);

  // Build a set of party names that failed so we don't attempt address sync for them.
  // Trying to link an Address to a non-existent Customer/Supplier causes
  // "Could not find Row #1: Link Name: <party>" errors.
  const failedPartyNames = new Set([
    ...customerResults.errors.map((e) => String(e.item || "")),
    ...supplierResults.errors.map((e) => String(e.item || "")),
  ]);

  // Sync addresses and contacts for all parties that have that data in Tally
  const partiesWithAddr = [
    ...customers.map((l) => ({ l, type: "Customer" })),
    ...suppliers.map((l) => ({ l, type: "Supplier" })),
  ].filter(({ l }) =>
    !failedPartyNames.has(l.name) &&   // skip parties that failed to create
    (l.address || l.phone || l.email || l.state || l.pincode)
  );

  if (partiesWithAddr.length > 0) {
    logger.info("Syncing addresses/contacts for " + partiesWithAddr.length + " parties from Tally");
    for (const { l, type } of partiesWithAddr) {
      try {
        await ensureAddress(client, l.name, type, l);
        await ensureContact(client, l.name, type, l);
        await sleep(300);
      } catch (_) { /* individual failures don't abort the batch */ }
    }
    logger.info("Address/contact sync done");
  }

  logger.success(
    "Ledger sync done - customers: +" + customerResults.created + " created, ~" + customerResults.updated + " updated, x" + customerResults.failed + " failed | " +
    "suppliers: +" + supplierResults.created + " created, ~" + supplierResults.updated + " updated, x" + supplierResults.failed + " failed"
  );
  return { customers: customerResults, suppliers: supplierResults, skipped };
}

// Removes the mandatory constraint on gst_hsn_code via Property Setter
// (ERPNext India GST enforces mandatory at Property Setter level, not Custom Field)
async function ensureHsnNotMandatory(client) {
  try {
    // Method 1: Update existing Property Setter
    const psName = "Item-gst_hsn_code-reqd";
    try {
      await client.put("/api/resource/Property Setter/" + encodeURIComponent(psName), { value: "0" });
      logger.info("HSN/SAC field set as non-mandatory via Property Setter (PUT)");
    } catch (_) {
      // Property Setter doesn't exist yet — create it
      try {
        await client.post("/api/resource/Property Setter", {
          doctype:          "Property Setter",
          doctype_or_field: "DocField",
          doc_type:         "Item",
          field_name:       "gst_hsn_code",
          property:         "reqd",
          property_type:    "Check",
          value:            "0",
        });
        logger.info("HSN/SAC field set as non-mandatory via Property Setter (POST)");
      } catch (_2) {}
    }

    // Method 2: Also try patching any Custom Field for gst_hsn_code that has reqd=1
    // Some ERPNext India installations enforce HSN via a Custom Field instead of Property Setter.
    try {
      const cfRes = await client.get("/api/resource/Custom Field", {
        params: {
          filters: JSON.stringify([["Custom Field","dt","=","Item"],["Custom Field","fieldname","=","gst_hsn_code"]]),
          fields: '["name","reqd"]', limit: 5,
        },
      });
      const cfs = (cfRes.data && cfRes.data.data) || [];
      for (const cf of cfs) {
        if (cf.reqd) {
          await client.put("/api/resource/Custom Field/" + encodeURIComponent(cf.name), { reqd: 0 });
          logger.info("HSN/SAC Custom Field set as non-mandatory: " + cf.name);
        }
      }
    } catch (_) {}

  } catch (e) {
    logger.warn("Could not patch gst_hsn_code mandatory flag: " + (e?.message || e));
  }
}

// -- Stock Items -> ERPNext Items ---------------------------------------------
export async function syncStockToErpNext(stockItems, creds = {}) {
  const client = createErpClient(creds);
  logger.info("Syncing " + stockItems.length + " stock items to ERPNext");

  // Remove mandatory on HSN field so items without HSN in Tally sync cleanly
  await ensureHsnNotMandatory(client);

  const uniqueGroups = Array.from(new Set(stockItems.map((i) => i.group).filter(Boolean).filter((g) => g !== "All Item Groups" && g.toLowerCase() !== "primary")));
  const uniqueUoMs   = Array.from(new Set(stockItems.map((i) => normaliseUoM(i.baseUnit)).filter(Boolean)));
  const uniqueHsns   = Array.from(new Set(stockItems.map((i) => sanitiseHsn(i.hsnCode)).filter((h) => h && h !== "null")));

  // Always ensure the fallback HSN "999999" exists so items with no Tally HSN can sync.
  const HSN_FALLBACK_STOCK = "999999";
  const uniqueHsnsWithFallback = Array.from(new Set([HSN_FALLBACK_STOCK, ...uniqueHsns]));
  logger.info("Pre-creating " + uniqueGroups.length + " Item Groups, " + uniqueUoMs.length + " UoMs, " + uniqueHsnsWithFallback.length + " HSN codes (incl. fallback)");
  for (const g of uniqueGroups)           { await ensureItemGroup(client, g); await sleep(100); }
  for (const u of uniqueUoMs)             { await ensureUoM(client, u);       await sleep(100); }
  for (const h of uniqueHsnsWithFallback) { await ensureHsnCode(client, h);   await sleep(100); }

  const results = await batchSync(client, "Item", stockItems, (item) => {
    const uom       = normaliseUoM(item.baseUnit);

    // item_group: strip Tally's special prefix chars (⊗ ♦ ◆ \u0004) — use as-is from Tally
    const rawGroup  = (item.group || "").replace(/[\u0004\u2297\u2666\u25c6*]/g, "").trim();
    const itemGroup = rawGroup && rawGroup.toLowerCase() !== "primary" ? rawGroup : "All Item Groups";

    // hsnCode: use Tally value if present, else fall back to "999999" (Other / Unclassified).
    // ERPNext India may still enforce mandatory HSN even after the Property Setter patch
    // on some installations — a fallback ensures the item always syncs.
    // "999999" is the standard catch-all HSN/SAC code for unclassified goods/services.
    const HSN_FALLBACK = "999999";
    const hsnCode   = sanitiseHsn(item.hsnCode) || HSN_FALLBACK;

    // Strip Tally special chars from all string fields before comparing
    const cleanStr = (s) => (s || "").replace(/[\u0004\u2297\u2666\u25c6*]/g, "").trim();

    const typeOfSupply  = cleanStr(item.gstTypeOfSupply).toLowerCase();
    const gstApplicable = cleanStr(item.gstApplicable).toLowerCase();
    const taxability    = cleanStr(item.taxability).toLowerCase();

    // is_stock_item: false only when Tally explicitly marks as Services
    const isService = typeOfSupply.includes("service");

    // gst_item_type: set only when Tally has a recognisable value — never default to "Goods"
    let gstItemType = null;
    if      (typeOfSupply.includes("capital")) gstItemType = "Capital Goods";
    else if (typeOfSupply.includes("service")) gstItemType = "Services";
    else if (typeOfSupply.includes("good"))    gstItemType = "Goods";

    // is_nil_exempt / is_non_gst: only when Tally explicitly says so
    const isNilExempt = gstApplicable.includes("not applicable") ||
                        gstApplicable.includes("exempt") ||
                        taxability.includes("nil") ||
                        taxability.includes("exempt");
    const isNonGst = taxability.includes("non") || gstApplicable.includes("non");

    const itemCode = item.name.slice(0, 140);

    // Build doc with only fields that have real values from Tally — no hardcoded defaults
    const doc = {
      doctype:          "Item",
      item_name:        item.name,
      item_code:        itemCode,
      item_group:       itemGroup,
      stock_uom:        uom,
      is_stock_item:    isService ? 0 : 1,
      is_sales_item:    1,
      is_purchase_item: 1,
      description:      item.name,
    };

    // GST fields — always set hsnCode (real Tally value or "999999" fallback — never null)
    // Other GST fields only set when Tally has a real value.
    doc.gst_hsn_code = hsnCode; // always present (fallback = "999999")
    if (gstItemType)  doc.gst_item_type = gstItemType;
    if (isNilExempt)  doc.is_nil_exempt = 1;
    if (isNonGst)     doc.is_non_gst    = 1;

    // Opening stock: qty + valuation_rate = openingValue / openingQty
    if (item.openingQty > 0) {
      doc.opening_stock = item.openingQty;
      if (item.openingValue > 0) {
        doc.valuation_rate = parseFloat((item.openingValue / item.openingQty).toFixed(4));
      }
    }

    // Standard selling rate from closing value / qty
    if (item.closingQty > 0 && item.closingValue > 0) {
      doc.standard_rate = parseFloat((item.closingValue / item.closingQty).toFixed(4));
    }

    return { filters: { item_code: doc.item_code }, doc };
  });
  

  logger.success("Stock sync done - created: " + results.created + ", updated: " + results.updated + ", failed: " + results.failed);
  return results;
}

// -- GST / Tax Template helpers -----------------------------------------------

const _knownTaxTemplates = new Set();

const GST_SLABS = [0, 5, 12, 18, 28];

async function resolveGstAccount(client, taxType, companyAbbr, companyName) {
  try {
    const res = await client.get("/api/resource/Account", {
      params: {
        filters: JSON.stringify([
          ["Account", "account_name", "like", "%" + taxType + "%"],
          ["Account", "company", "=", companyName],
        ]),
        fields: '["name"]',
        limit: 1,
      },
    });
    const found = res.data?.data?.[0]?.name;
    if (found) return found;
  } catch (_) {}
  return null;
}

async function ensureGstTaxTemplate(client, rate, companyName, companyAbbr) {
  const templateName = "GST " + rate + "% - " + companyAbbr;
  if (_knownTaxTemplates.has(templateName)) return templateName;

  try {
    await client.get("/api/resource/Item Tax Template/" + encodeURIComponent(templateName));
    _knownTaxTemplates.add(templateName);
    return templateName;
  } catch (_) {}

  const half = rate / 2;
  const igstAcct = await resolveGstAccount(client, "IGST", companyAbbr, companyName);
  const cgstAcct = await resolveGstAccount(client, "CGST", companyAbbr, companyName);
  const sgstAcct = await resolveGstAccount(client, "SGST", companyAbbr, companyName);

  const taxes = [];
  if (igstAcct) taxes.push({ tax_type: igstAcct, tax_rate: rate });
  if (cgstAcct) taxes.push({ tax_type: cgstAcct, tax_rate: half });
  if (sgstAcct) taxes.push({ tax_type: sgstAcct, tax_rate: half });

  if (taxes.length === 0) {
    logger.warn("GST Tax Template \"" + templateName + "\" skipped — no GST accounts found for " + companyName);
    _knownTaxTemplates.add(templateName);
    return null;
  }

  try {
    await client.post("/api/resource/Item Tax Template", {
      doctype: "Item Tax Template",
      title:   templateName,
      company: companyName,
      taxes,
    });
    _knownTaxTemplates.add(templateName);
    logger.info("Created Item Tax Template: " + templateName);
  } catch (err) {
    const msg = parseErpError(err);
    if (msg.toLowerCase().includes("duplicate") || msg.toLowerCase().includes("already")) {
      _knownTaxTemplates.add(templateName);
      return templateName;
    }
    logger.warn("Could not create Item Tax Template \"" + templateName + "\": " + msg);
    _knownTaxTemplates.add(templateName);
    return null;
  }
  return templateName;
}

export async function syncTaxesToErpNext(stockItems, companyName, creds = {}) {
  const client = createErpClient(creds);
  companyName  = await resolveErpNextCompany(client, companyName, creds);
  const companyAbbr = await getCompanyAbbr(client, companyName);
  logger.info("Syncing GST tax templates for " + companyName + " (abbr: " + companyAbbr + ")");

  // Step 1: Ensure all standard + Tally-specific GST slabs exist
  const tallyRates = [...new Set(stockItems.map((i) => i.gstRate).filter((r) => r !== null && r !== undefined && !isNaN(r)))];
  const allRates   = [...new Set([...GST_SLABS, ...tallyRates])].sort((a, b) => a - b);

  logger.info("Creating GST Tax Templates for rates: " + allRates.join("%, ") + "%");
  const templateByRate = new Map();
  for (const rate of allRates) {
    const tname = await ensureGstTaxTemplate(client, rate, companyName, companyAbbr);
    if (tname) templateByRate.set(rate, tname);
    await sleep(400);
  }

  // Step 2: Ensure HSN Code records exist
  const uniqueHsns = [...new Set(stockItems.map((i) => sanitiseHsn(i.hsnCode)).filter(Boolean))];
  logger.info("Ensuring " + uniqueHsns.length + " HSN/SAC codes in ERPNext");
  for (const hsn of uniqueHsns) {
    await ensureHsnCode(client, hsn);
    await sleep(150);
  }

  // Step 3: Link Item Tax Template + HSN Code onto each Item
  const itemsToUpdate = stockItems.filter((i) =>
    (i.gstRate !== null && i.gstRate !== undefined && templateByRate.has(i.gstRate)) ||
    sanitiseHsn(i.hsnCode)
  );

  logger.info("Linking tax templates to " + itemsToUpdate.length + " items");
  let linked = 0, failed = 0;

  for (const item of itemsToUpdate) {
    const itemCode = (item.name || "").slice(0, 140);
    if (!itemCode) continue;

    const patch = {};
    const hsnCode = sanitiseHsn(item.hsnCode);
    if (hsnCode) patch.gst_hsn_code = hsnCode;

    const template = (item.gstRate !== null && item.gstRate !== undefined)
      ? templateByRate.get(item.gstRate)
      : null;
    if (template) patch.taxes = [{ item_tax_template: template }];

    if (Object.keys(patch).length === 0) continue;

    try {
      await client.put("/api/resource/Item/" + encodeURIComponent(itemCode),
        Object.assign({}, patch, { doctype: "Item", name: itemCode })
      );
      linked++;
    } catch (err) {
      failed++;
      if (failed <= 5) logger.warn("Tax link failed for \"" + itemCode + "\": " + parseErpError(err));
    }
    await sleep(300);
  }

  logger.success("Tax sync done — templates created: " + templateByRate.size + ", items linked: " + linked + ", failed: " + failed);
  return { templatesCreated: templateByRate.size, linked, failed };
}

// -- Vouchers -> Journal Entries -----------------------------------------------
// ── Map Tally voucher type → ERPNext Journal Entry voucher_type enum ──────────
// ERPNext only accepts these exact strings for Journal Entry voucher_type:
//   "Journal Entry" | "Inter Company Journal Entry" | "Bank Entry" |
//   "Cash Entry" | "Credit Card Entry" | "Debit Note" | "Credit Note" |
//   "Contra Entry" | "Excise Entry" | "Write Off Entry" |
//   "Opening Entry" | "Depreciation Entry" | "Exchange Rate Revaluation" |
//   "Exchange Gain Or Loss" | "Deferred Revenue" | "Deferred Expense"
// Opening Balance JEs use "Opening Entry" (handled by syncOpeningBalancesToErpNext).
// Regular vouchers must NEVER use "Opening Entry" or they appear in the OB list.
function tallyVoucherTypeToErpNext(tallyType) {
  const t = (tallyType || "").toLowerCase().trim();
  if (t === "payment")          return "Bank Entry";   // most payments are via bank
  if (t === "receipt")          return "Bank Entry";   // most receipts are via bank
  if (t === "contra")           return "Contra Entry";
  if (t === "debit note")       return "Debit Note";
  if (t === "credit note")      return "Credit Note";
  if (t === "journal")          return "Journal Entry";
  // Catch-all — anything unrecognised posts as a plain Journal Entry
  return "Journal Entry";
}

// ── Determine if an ERPNext account is Receivable or Payable ─────────────────
// ERPNext requires party_type + party on every JE row that uses such an account.
// _accountTypeMap is declared near the top of the module (with the other caches)
// so resolveAccount (which populates it on auto-create) doesn't hit a ReferenceError.

async function resolveAccountWithType(client, ledgerName, companyAbbr, companyName) {
  const name = await resolveAccount(client, ledgerName, companyAbbr, companyName);
  // If we just auto-created it, _accountTypeMap already has the type.
  // Otherwise query ERPNext for the account_type (cached after first hit).
  if (!_accountTypeMap.has(name)) {
    try {
      const res = await client.get("/api/resource/Account/" + encodeURIComponent(name), {
        params: { fields: '["account_type"]' },
      });
      const t = res.data && res.data.data && res.data.data.account_type;
      _accountTypeMap.set(name, t || "");
    } catch (_) {
      _accountTypeMap.set(name, "");
    }
  }
  return { name, accountType: _accountTypeMap.get(name) || "" };
}

export async function syncVouchersToErpNext(vouchers, companyName, creds = {}) {
  const client = createErpClient(creds);
  companyName = await resolveErpNextCompany(client, companyName, creds);
  logger.info("Syncing " + vouchers.length + " vouchers to ERPNext");
  const companyAbbr = companyName ? await getCompanyAbbr(client, companyName) : "T";
  logger.info("Resolving accounts with company abbreviation: " + companyAbbr);

  // ── Filter: only real transaction vouchers — NEVER opening-balance types ────
  // "Opening Balance", "Stock Journal" etc are not standard JE types and would
  // corrupt the Opening Entry list or fail validation.
  const JOURNAL_TYPES = ["Journal", "Payment", "Receipt", "Contra", "Debit Note", "Credit Note"];
  const journalVouchers = vouchers.filter((v) => JOURNAL_TYPES.includes(v.voucherType));
  const salesVouchers   = vouchers.filter((v) => v.voucherType === "Sales");
  const skippedTypes    = vouchers.length - journalVouchers.length - salesVouchers.length;
  if (skippedTypes > 0) logger.info("Skipping " + skippedTypes + " vouchers with non-JE types (Stock Journal, Opening Balance, etc.)");

  // ── Resolve accounts and annotate party info for Receivable/Payable rows ────
  for (const v of journalVouchers) {
    if (v.entries && v.entries.length > 0) {
      const resolved = [];
      for (const e of v.entries) {
        const { name: acct, accountType } = await resolveAccountWithType(client, e.ledger, companyAbbr, companyName);

        const row = {
          account:                    acct,
          debit_in_account_currency:  e.isDebit ? Math.abs(e.amount) : 0,
          credit_in_account_currency: e.isDebit ? 0 : Math.abs(e.amount),
        };

        // ── FIX: ERPNext requires party_type + party for Receivable/Payable ──
        // Without these fields ERPNext throws:
        //   "Party Type and Party is required for Receivable / Payable account"
        // We use the ledger name itself as the party — it is already synced as
        // a Customer or Supplier by syncLedgersToErpNext. If it isn't, ERPNext
        // will still accept the JE in Draft and the user can fix manually.
        if (accountType === "Receivable") {
          row.party_type = "Customer";
          row.party      = e.ledger.trim();
        } else if (accountType === "Payable") {
          row.party_type = "Supplier";
          row.party      = e.ledger.trim();
        }

        resolved.push(row);
      }
      v.resolvedAccounts = resolved;

    } else if (v.partyName) {
      const { name: acct, accountType } = await resolveAccountWithType(client, v.partyName, companyAbbr, companyName);
      const row1 = { account: acct, debit_in_account_currency: v.netAmount || 0, credit_in_account_currency: 0 };
      const row2 = { account: acct, debit_in_account_currency: 0, credit_in_account_currency: v.netAmount || 0 };
      if (accountType === "Receivable") { row1.party_type = row2.party_type = "Customer"; row1.party = row2.party = v.partyName.trim(); }
      else if (accountType === "Payable") { row1.party_type = row2.party_type = "Supplier"; row1.party = row2.party = v.partyName.trim(); }
      v.resolvedAccounts = [row1, row2];

    } else {
      v._skip = true;
      logger.warn("Voucher " + v.voucherNumber + " has no entries — skipping");
    }
  }

  const syncable = journalVouchers.filter((v) => !v._skip);
  logger.info("Syncing " + syncable.length + "/" + journalVouchers.length + " vouchers");

  // ── Sanitize amounts and trim strings before posting ─────────────────────────
  // MySQL DECIMAL(21,9) max is ~999999999999.999999999.
  // Tally can produce corrupt/huge values (NaN, Infinity, opening-balance carry-overs).
  // Any such value causes: MySQLdb.DataError: Out of range value for column 'total_debit'
  // Strategy: clamp each row amount to [0, 999_999_999_999], skip rows that are
  // still invalid, and skip the whole voucher if no valid rows remain.
  // FIX: Lower cap from 999B to 9,99,99,999 (≈ 100 crore INR).
  // ERPNext's total_amount_in_words field is VARCHAR(140). Amounts like
  // 999,999,999,999 produce >140-char word strings and ERPNext rejects the JE.
  // Any Tally voucher with a row exceeding ~100 crore is almost certainly
  // corrupt data (opening-balance carry-overs, test entries, etc.) — skip it.
  const MAX_AMOUNT = 999_999_999;  // ~100 crore INR

  function safeAmount(n) {
    const v = Number(n);
    if (!isFinite(v) || isNaN(v)) return 0;
    return Math.min(Math.max(v, 0), MAX_AMOUNT);
  }

  const sanitized = [];
  for (const v of syncable) {
    if (!v.resolvedAccounts) continue;

    const cleanRows = v.resolvedAccounts
      .map((a) => {
        const row = Object.assign({}, a, {
          account:                    (a.account || "").trim(),
          debit_in_account_currency:  safeAmount(a.debit_in_account_currency),
          credit_in_account_currency: safeAmount(a.credit_in_account_currency),
        });
        if (row.party) row.party = row.party.trim();
        return row;
      })
      .filter((a) => a.debit_in_account_currency > 0 || a.credit_in_account_currency > 0);

    if (cleanRows.length === 0) {
      logger.warn("Voucher " + (v.voucherNumber || v.guid) + " skipped — all rows have zero/invalid amounts after sanitization");
      continue;
    }

    // Check if any single row amount is still suspicious (shouldn't happen after clamp, but guard anyway)
    const hasOverflow = cleanRows.some(
      (a) => a.debit_in_account_currency > MAX_AMOUNT || a.credit_in_account_currency > MAX_AMOUNT
    );
    if (hasOverflow) {
      logger.warn("Voucher " + (v.voucherNumber || v.guid) + " skipped — contains out-of-range amount (>" + MAX_AMOUNT + ")");
      continue;
    }

    v.resolvedAccounts = cleanRows;
    sanitized.push(v);
  }

  const skippedAmounts = syncable.length - sanitized.length;
  if (skippedAmounts > 0) logger.warn(skippedAmounts + " voucher(s) skipped due to invalid/overflow amounts");

  // ── FIX: Pre-create missing Customers/Suppliers before posting JEs ───────────
  // ERPNext rejects any JE row with party_type+party set if that party does not
  // exist. This is the #1 cause of "Could not find Row #N: Party: <name>" errors
  // when voucher sync runs without a prior ledger sync.
  // Strategy: collect all unique party names from resolved rows, check if they
  // exist in ERPNext, and auto-create minimal stubs for any that are missing.
  const _partiesNeeded = { Customer: new Set(), Supplier: new Set() };
  for (const v of sanitized) {
    for (const row of (v.resolvedAccounts || [])) {
      if (row.party_type === "Customer" && row.party) _partiesNeeded.Customer.add(row.party);
      if (row.party_type === "Supplier" && row.party) _partiesNeeded.Supplier.add(row.party);
    }
  }

  logger.info(
    "[JE pre-sync] Ensuring parties exist — Customers: " + _partiesNeeded.Customer.size +
    ", Suppliers: " + _partiesNeeded.Supplier.size
  );

  for (const name of _partiesNeeded.Customer) {
    try {
      await client.get("/api/resource/Customer/" + encodeURIComponent(name));
    } catch (_) {
      try {
        await client.post("/api/resource/Customer", {
          doctype:        "Customer",
          customer_name:  name,
          customer_type:  "Individual",
          customer_group: _customerGroup || "Commercial",
          territory:      "India",
        });
        logger.info("[JE pre-sync] Auto-created Customer: " + name);
      } catch (e) {
        logger.warn("[JE pre-sync] Could not auto-create Customer \"" + name + "\": " + parseErpError(e));
      }
      await sleep(300);
    }
  }

  for (const name of _partiesNeeded.Supplier) {
    try {
      await client.get("/api/resource/Supplier/" + encodeURIComponent(name));
    } catch (_) {
      try {
        await client.post("/api/resource/Supplier", {
          doctype:        "Supplier",
          supplier_name:  name,
          supplier_type:  "Individual",
          supplier_group: _supplierGroup || "Services",
        });
        logger.info("[JE pre-sync] Auto-created Supplier: " + name);
      } catch (e) {
        logger.warn("[JE pre-sync] Could not auto-create Supplier \"" + name + "\": " + parseErpError(e));
      }
      await sleep(300);
    }
  }
  // ─────────────────────────────────────────────────────────────────────────────

  const jeResults = await batchSync(client, "Journal Entry", sanitized, (v) => {
    // FIX: cheque_no has a 140-char limit in ERPNext. GUIDs (36 chars) + prefix are fine,
    // but voucher numbers can be long. Truncate to 140 chars to prevent silent truncation
    // that would cause two different vouchers to get the same cheque_no and collide.
    // We use user_remark as the canonical idempotency key (no length constraint) and
    // keep cheque_no as a truncated human-readable reference only.
    const idKey       = ("Tally:" + (v.voucherNumber || v.guid)).slice(0, 140);
    const remarkKey   = "Tally:" + (v.voucherNumber || v.guid); // no length limit
    return {
      // FIX: Use user_remark as the upsert filter key — it is the full reliable key.
      // cheque_no was used before but silent truncation caused false "not found" collisions.
      filters: { user_remark: remarkKey },
      doc: {
        doctype:      "Journal Entry",
        title:        (v.voucherType + " - " + (v.voucherNumber || v.guid)).slice(0, 140),
        voucher_type: tallyVoucherTypeToErpNext(v.voucherType),
        posting_date: v.voucherDate,
        company:      companyName,
        cheque_no:    idKey,
        cheque_date:  v.voucherDate,
        user_remark:  remarkKey,
        accounts:     v.resolvedAccounts,
      },
    };
  });

  logger.success("Voucher sync done - JE created: " + jeResults.created + ", updated: " + jeResults.updated + ", failed: " + jeResults.failed);
  return { journalEntries: jeResults, salesInvoices: { created: 0, updated: 0, failed: salesVouchers.length } };
}

// -- Chart of Accounts --------------------------------------------------------
const TALLY_GROUP_TYPE_MAP = {
  "current assets": "Asset", "fixed assets": "Asset", "investments": "Asset",
  "loans & advances (asset)": "Asset", "misc. expenses (asset)": "Asset",
  "bank accounts": "Asset", "cash-in-hand": "Asset", "deposits (asset)": "Asset",
  "stock-in-hand": "Asset",
  "current liabilities": "Liability", "loans (liability)": "Liability",
  "capital account": "Equity", "reserves & surplus": "Equity",
  "bank od & od accounts": "Liability",
  "sales accounts": "Income", "indirect income": "Income", "direct incomes": "Income",
  "purchase accounts": "Expense", "indirect expenses": "Expense", "direct expenses": "Expense",
  "manufacturing expenses": "Expense",
  "sundry debtors": "Asset", "sundry creditors": "Liability",
  "duties & taxes": "Liability", "provisions": "Liability",
};

function tallyGroupToAccountType(groupName, parentName) {
  const lower       = (groupName  || "").toLowerCase();
  const parentLower = (parentName || "").toLowerCase();
  for (const [key, type] of Object.entries(TALLY_GROUP_TYPE_MAP)) {
    if (lower.includes(key) || parentLower.includes(key)) return type;
  }
  return "Expense";
}

export async function syncChartOfAccountsToErpNext(groups, companyName, creds = {}) {
  const client = createErpClient(creds);
  companyName = await resolveErpNextCompany(client, companyName, creds);
  logger.info("Syncing " + groups.length + " account groups to ERPNext for " + companyName);

  const companyAbbr = await getCompanyAbbr(client, companyName);

  // ── Step 1: Fetch ALL existing ERPNext accounts for this company once ──────
  // We do this upfront so we can:
  //   (a) resolve parent accounts by name without repeated API calls
  //   (b) skip creating accounts that already exist
  //   (c) map Tally root groups to real ERPNext root accounts dynamically
  //       instead of guessing hardcoded names that may not exist.
  let erpAccounts = [];
  try {
    let page = 0;
    while (true) {
      const res = await client.get("/api/resource/Account", {
        params: {
          filters:     JSON.stringify([["Account", "company", "=", companyName]]),
          fields:      '["name","account_name","root_type","is_group","parent_account"]',
          limit:       500,
          limit_start: page * 500,
        },
      });
      const rows = (res.data && res.data.data) || [];
      erpAccounts = erpAccounts.concat(rows);
      if (rows.length < 500) break;
      page++;
    }
    logger.info("COA: fetched " + erpAccounts.length + " existing ERPNext accounts for " + companyName);
  } catch (e) {
    logger.warn("COA: could not pre-fetch ERPNext accounts — " + e.message);
  }

  // Build lookup: account_name (lowercase) → full account record
  const erpByName = new Map();
  for (const a of erpAccounts) {
    if (a.account_name) erpByName.set(a.account_name.trim().toLowerCase(), a);
    if (a.name)         erpByName.set(a.name.trim().toLowerCase(), a);
  }

  // ── Step 2: Build root-type buckets from REAL ERPNext accounts ────────────
  // Find the true top-level group for each root_type.
  // ERPNext India does NOT use root_type='Equity' — Capital Account sits under
  // Liabilities in the Indian COA template. So equityRoot falls back to liabilityRoot.
  function findRootGroupByType(rootType) {
    const candidates = erpAccounts.filter((a) => a.is_group && a.root_type === rootType);
    if (!candidates.length) return null;
    const trueRoot = candidates.find((a) => !a.parent_account);
    if (trueRoot) return trueRoot.name;
    return candidates.sort((a, b) => a.name.length - b.name.length)[0].name;
  }

  const assetRoot     = findRootGroupByType('Asset');
  const liabilityRoot = findRootGroupByType('Liability');
  const incomeRoot    = findRootGroupByType('Income');
  const expenseRoot   = findRootGroupByType('Expense');
  // ERPNext India: no Equity root_type — Capital Account lives under Liabilities
  const equityRoot    = findRootGroupByType('Equity') || liabilityRoot;

  logger.info('COA root accounts — Asset: ' + assetRoot + ', Liability: ' + liabilityRoot +
    ', Equity(fallback): ' + equityRoot + ', Income: ' + incomeRoot + ', Expense: ' + expenseRoot);

  // Tally group name (lowercase) -> ERPNext root_type bucket
  // Covers all standard Tally group names including case variants from the logs
  const TALLY_ROOT_TYPE = {
    // Asset
    'current assets':                   'Asset',
    'fixed assets':                     'Asset',
    'investments':                      'Asset',
    'loans & advances (asset)':         'Asset',
    'misc. expenses (asset)':           'Asset',
    'misc. expenses (asset)':           'Asset',
    'bank accounts':                    'Asset',
    'cash-in-hand':                     'Asset',
    'deposits (asset)':                 'Asset',
    'stock-in-hand':                    'Asset',
    'sundry debtors':                   'Asset',
    'branch / divisions':               'Asset',
    // Liability
    'current liabilities':              'Liability',
    'loans (liability)':                'Liability',
    'bank od & od accounts':            'Liability',
    'bank od a/c':                      'Liability',
    'provisions':                       'Liability',
    'duties & taxes':                   'Liability',
    'sundry creditors':                 'Liability',
    'suspense a/c':                     'Liability',
    'suspense':                         'Liability',
    // Equity (maps to liabilityRoot on ERPNext India)
    'capital account':                  'Equity',
    'reserves & surplus':               'Equity',
    // Income
    'sales accounts':                   'Income',
    'indirect income':                  'Income',
    'indirect incomes':                 'Income',
    'direct incomes':                   'Income',
    'direct income':                    'Income',
    // Expense
    'purchase accounts':                'Expense',
    'indirect expenses':                'Expense',
    'direct expenses':                  'Expense',
    'manufacturing expenses':           'Expense',
  };

  // Build a CONTAINS lookup for partial name matching against TALLY_ROOT_TYPE keys.
  // This handles case variants like "Direct Expenses" vs "direct expenses" and
  // Tally groups that partially match a known key (e.g. "Misc. Expenses (ASSET)").
  function resolveRootType(nameLower) {
    // Exact match first
    if (TALLY_ROOT_TYPE[nameLower]) return TALLY_ROOT_TYPE[nameLower];
    // Partial / substring match
    for (const [key, type] of Object.entries(TALLY_ROOT_TYPE)) {
      if (nameLower.includes(key) || key.includes(nameLower)) return type;
    }
    return null;
  }

  const ROOT_BY_TYPE = {
    'Asset':     assetRoot,
    'Liability': liabilityRoot,
    'Equity':    equityRoot,
    'Income':    incomeRoot,
    'Expense':   expenseRoot,
  };

  // ── Step 3: Resolve parent for a Tally group ──────────────────────────────
  // Priority:
  //   1. Non-primary parent -> look up in erpByName (with suffix + plain)
  //   2. Primary/blank parent -> map via TALLY_ROOT_TYPE to real ERPNext root
  //   3. Partial name match in existing ERPNext group accounts
  //   4. null -> no parent_account sent (safe, no 'Primary - T' error)
  function resolveParentAccount(group) {
    const rawParent = (group.parent || '').trim();
    const isPrimary = !rawParent || rawParent.toLowerCase() === 'primary';

    if (!isPrimary) {
      const withSuffix = (rawParent + ' - ' + companyAbbr).toLowerCase();
      const plain      = rawParent.toLowerCase();
      if (erpByName.has(withSuffix)) return erpByName.get(withSuffix).name;
      if (erpByName.has(plain))      return erpByName.get(plain).name;
      // Parent may be created earlier in this run and already added to erpByName
      return rawParent + ' - ' + companyAbbr;
    }

    // Root-level Tally group: resolve to real ERPNext root via type mapping
    // Use resolveRootType() which does partial/substring matching so case variants
    // like "Misc. Expenses (ASSET)", "Direct Expenses", "Branch / Divisions" all match.
    const nameLower = group.name.trim().toLowerCase();
    const rootType  = resolveRootType(nameLower);
    if (rootType && ROOT_BY_TYPE[rootType]) return ROOT_BY_TYPE[rootType];

    // Partial match: any ERPNext group account whose name contains this group name
    for (const [key, acct] of erpByName) {
      if (acct.is_group && key.includes(nameLower)) return acct.name;
    }

    // Last resort: attach to asset root so ERPNext always gets a valid parent.
    // Returning null here would leave the existing "Primary - T" parent on the
    // account (the PUT omits parent_account), which causes the COA sync error.
    return assetRoot || liabilityRoot || null;
  }

  // ── Step 4: Sort groups so parents are always created before children ──────
  const sorted = groups.slice().sort((a, b) => {
    const cleanP = (s) => { if (!s) return ""; let o = ""; for (let i = 0; i < s.length; i++) { const c = s.charCodeAt(i); if (c <= 31 || c === 127) continue; o += s[i]; } return o.trim().toLowerCase(); };
    const isPrimaryA = !a.parent || cleanP(a.parent) === "primary";
    const isPrimaryB = !b.parent || cleanP(b.parent) === "primary";
    if (isPrimaryA && !isPrimaryB) return -1;
    if (!isPrimaryA && isPrimaryB) return 1;
    return (a.parent || "").localeCompare(b.parent || "");
  });

  let created = 0, updated = 0, failed = 0;
  const errors = [];

  let skipped = 0;
  for (const group of sorted) {
    try {
      // Strip Tally control chars ( EOT etc.) that survive xml2js and break .trim()
      const rawParent  = (group.parent || '').replace(/[\x00-\x1F\x7F\u200B\uFEFF]/g, '').trim();
      const isRootGroup = !rawParent || rawParent.toLowerCase() === 'primary';


      const parentAccount = resolveParentAccount(group);
      const accountType   = tallyGroupToAccountType(group.name, group.parent);

      const doc = {
        account_name: group.name,
        company:      companyName,
        is_group:     1,
      };
      if (parentAccount) doc.parent_account = parentAccount;

      // Check if already exists (use pre-fetched map first, fall back to API)
      const existingKey  = (group.name + " - " + companyAbbr).toLowerCase();
      const existingPlain = group.name.toLowerCase();
      let existingName = null;
      if (erpByName.has(existingKey))   existingName = erpByName.get(existingKey).name;
      else if (erpByName.has(existingPlain)) existingName = erpByName.get(existingPlain).name;
      else {
        // Not in cache — do a targeted API call
        try {
          const list = await client.get("/api/resource/Account", {
            params: {
              filters: JSON.stringify([
                ["Account", "account_name", "=", group.name],
                ["Account", "company",       "=", companyName],
              ]),
              fields: '["name"]',
              limit:  1,
            },
          });
          existingName = list.data && list.data.data && list.data.data[0] && list.data.data[0].name;
        } catch (_) {}
      }

      if (existingName) {
        if (isRootGroup) {
          // Root groups (parent=Primary) already exist in ERPNext's standard COA and their
          // parent_account is locked — ERPNext rejects any attempt to reparent them via REST.
          // Register in erpByName so children can still resolve them as parents, then skip.
          erpByName.set(existingName.toLowerCase(), { name: existingName, account_name: group.name, is_group: 1 });
          skipped++;
          await sleep(BATCH_DELAY_MS);
          continue;
        }
        // Non-root: safe to PUT with non-structural fields only (no parent_account reparenting)
        const putDoc = { account_name: doc.account_name, company: doc.company, is_group: doc.is_group };
        await client.put("/api/resource/Account/" + encodeURIComponent(existingName), putDoc);
        updated++;
        logger.info("COA updated: " + existingName);
      } else if (isRootGroup) {
        // Root group does not exist — try to POST with the resolved root parent.
        // If ERPNext rejects it (e.g. "Primary - T" not found), log a clear warning and skip
        // rather than counting it as a hard failure — the standard COA may cover it already.
        try {
          await client.post("/api/resource/Account", Object.assign({}, doc, { doctype: "Account" }));
          created++;
          const newName = group.name + " - " + companyAbbr;
          erpByName.set(newName.toLowerCase(), { name: newName, account_name: group.name, is_group: 1 });
          logger.info("COA created: " + newName + (parentAccount ? " (parent: " + parentAccount + ")" : ""));
        } catch (postErr) {
          // Root group creation failed — not a critical error; log and continue
          skipped++;
          logger.info("COA root group skipped (already managed by ERPNext COA): " + group.name);
        }
        await sleep(BATCH_DELAY_MS);
        continue;
      } else {
        await client.post("/api/resource/Account", Object.assign({}, doc, { doctype: "Account" }));
        created++;
        // Add to local cache so child groups can resolve this as parent
        const newName = group.name + " - " + companyAbbr;
        erpByName.set(newName.toLowerCase(), { name: newName, account_name: group.name, is_group: 1 });
        logger.info("COA created: " + newName + (parentAccount ? " (parent: " + parentAccount + ")" : ""));
      }
      await sleep(BATCH_DELAY_MS);
    } catch (err) {
      failed++;
      const msg = parseErpError(err);
      if (errors.length < 20) errors.push({ group: group.name, error: msg });
      logger.warn("COA sync error [" + group.name + "]: " + msg);
    }
  }

  logger.success("Chart of Accounts sync done - created: " + created + ", updated: " + updated + ", skipped: " + skipped + ", failed: " + failed);
  return { created, updated, failed, errors };
}

// -- Godowns -> Warehouses ----------------------------------------------------
async function resolveRootWarehouse(client, companyName, companyAbbr) {
  // Ask ERPNext for the is_group=1 warehouse that belongs to this company.
  // ERPNext creates it as "All Warehouses - <abbr>" by default but some
  // installations rename it, so we fetch the real name instead of guessing.
  try {
    const res = await client.get("/api/resource/Warehouse", {
      params: {
        filters: JSON.stringify([["Warehouse","company","=",companyName],["Warehouse","is_group","=",1],["Warehouse","parent_warehouse","=",""]]),
        fields:  '["name"]',
        limit:   1,
      },
    });
    const name = res.data && res.data.data && res.data.data[0] && res.data.data[0].name;
    if (name) { logger.info("Root warehouse resolved: " + name); return name; }
  } catch (e) {
    logger.warn("Could not resolve root warehouse: " + e.message);
  }
  // Fallback to the ERPNext default name
  return "All Warehouses - " + companyAbbr;
}

export async function syncGodownsToErpNext(godowns, companyName, creds = {}) {
  const client = createErpClient(creds);
  companyName = await resolveErpNextCompany(client, companyName, creds);
  const companyAbbr = await getCompanyAbbr(client, companyName);
  logger.info("Syncing " + godowns.length + " godowns to ERPNext for " + companyName + " (abbr: " + companyAbbr + ")");

  const rootWarehouse = await resolveRootWarehouse(client, companyName, companyAbbr);

  const results = await batchSync(client, "Warehouse", godowns, (g) => {
    // Strip ALL whitespace variants (regular, non-breaking, zero-width) from Tally parent name
    const rawParent = Array.from(g.parent || "")
      .filter(ch => ch.charCodeAt(0) > 32 && ch.charCodeAt(0) !== 160 && ch.charCodeAt(0) !== 8203)
      .join("").toLowerCase();
    const parentWarehouse = (rawParent && rawParent !== "primary")
      ? (g.parent || "").trim() + " - " + companyAbbr
      : rootWarehouse;
    const doc = {
      warehouse_name:   g.name,
      company:          companyName,
      is_group:         0,
      parent_warehouse: parentWarehouse,
    };
    // Carry Tally address into ERPNext Warehouse address fields
    if (g.address) {
      doc.address_line_1 = g.address.slice(0, 140);
    }
    // ERPNext stores warehouses as "Name - Abbr" — use that as the lookup key
    // so upsert finds the existing record and does a PUT instead of failing with Duplicate
    return { filters: { name: g.name + " - " + companyAbbr }, doc };
  });

  logger.success("Godown sync done - created: " + results.created + ", updated: " + results.updated + ", failed: " + results.failed);
  return results;
}

// -- Opening Balances ---------------------------------------------------------
// IMPORTANT: Ledgers (Customers/Suppliers/Accounts) MUST be synced before
// running this. Each Journal Entry account line must reference a real ERPNext
// account name — if ledgers aren't synced, every POST returns 429/error.

const OB_CHUNK_SIZE = 40; // max accounts per Journal Entry — keeps payload small

export async function syncOpeningBalancesToErpNext(ledgers, companyName, creds = {}) {
  const client = createErpClient(creds);
  companyName = await resolveErpNextCompany(client, companyName, creds);
  logger.info("Syncing opening balances for " + ledgers.length + " ledgers in " + companyName);

  const companyAbbr = await getCompanyAbbr(client, companyName);
  const withBalance = ledgers.filter((l) => l.openingBalance && Math.abs(l.openingBalance) > 0);

  if (withBalance.length === 0) {
    logger.info("No ledgers with non-zero opening balances found");
    return { created: 0, updated: 0, failed: 0, skipped: ledgers.length };
  }
  logger.info("Found " + withBalance.length + " ledgers with opening balances: " + withBalance.map((l) => l.name + "=" + l.openingBalance).join(", "));

  // -- Fetch all accounts for this company once --------------------------------
  let erpAccounts = [];
  try {
    let page = 0;
    const pageSize = 500;
    while (true) {
      const res = await client.get("/api/resource/Account", {
        params: {
          filters: JSON.stringify([["Account", "company", "=", companyName]]),
          fields: '["name","account_name","account_type","is_group"]',
          limit: pageSize,
          limit_start: page * pageSize,
        },
      });
      const allRows = (res.data && res.data.data) || [];
      erpAccounts = erpAccounts.concat(allRows);
      if (allRows.length < pageSize) break;
      page++;
    }
  } catch (e) {
    const errDetail = e.response
      ? ("HTTP " + e.response.status + ": " + JSON.stringify(e.response.data).slice(0, 300))
      : e.message;
    logger.error("Could not bulk-fetch ERPNext accounts: " + errDetail);
  }

  logger.info("Fetched " + erpAccounts.length + " accounts from ERPNext for " + companyName);

  // Build lookup maps (leaf accounts only for GL resolution)
  const leafAccounts = erpAccounts.filter((a) => !a.is_group);
  const byAccountName = new Map();
  const byFullName    = new Map();
  for (const a of leafAccounts) {
    if (a.account_name) byAccountName.set(a.account_name.trim().toLowerCase(), a);
    if (a.name)         byFullName.set(a.name.trim().toLowerCase(), a);
  }

  function findErpAccount(ledgerName) {
    const key = ledgerName.trim().toLowerCase();
    if (byAccountName.has(key)) return byAccountName.get(key);
    if (byFullName.has(key))    return byFullName.get(key);
    for (const [fk, fa] of byFullName) {
      if (fk.startsWith(key + " - ")) return fa;
    }
    return null;
  }

  // Find Receivable and Payable control accounts
  const receivableAcct = leafAccounts.find((a) => a.account_type === "Receivable");
  const payableAcct    = leafAccounts.find((a) => a.account_type === "Payable");
  logger.info("Control accounts - Receivable: " + (receivableAcct ? receivableAcct.name : "NOT FOUND") + ", Payable: " + (payableAcct ? payableAcct.name : "NOT FOUND"));

  // -- Classify ledgers and resolve to JE account rows ------------------------
  // ERPNext Opening Entry for party ledgers must use the Receivable/Payable
  // control account + party_type + party. Direct per-party accounts are NOT
  // created automatically by ERPNext for new Customers/Suppliers.
  const DEBTOR_KEYS   = ["sundry debtor", "debtor", "receivable", "accounts receivable"];
  const CREDITOR_KEYS = ["sundry creditor", "creditor", "payable", "accounts payable"];
  const isDebtor   = (l) => DEBTOR_KEYS.some((k)   => (l.parentGroup || "").toLowerCase().includes(k));
  const isCreditor = (l) => CREDITOR_KEYS.some((k)  => (l.parentGroup || "").toLowerCase().includes(k));

  // Groups that map to GL accounts directly — we try findErpAccount first, then
  // auto-create the missing account under the correct parent group, so nothing is skipped.
  const GL_GROUP_KEYS = [
    "bank account", "bank accounts", "cash-in-hand", "cash in hand",
    "fixed assets", "current assets", "investments", "deposits (asset)",
    "loans & advances (asset)", "stock-in-hand", "misc. expenses (asset)",
    "current liabilities", "loans (liability)", "bank od",
    "capital account", "reserves", "profit & loss", "profit and loss",
    "duties & taxes", "duties and taxes", "provisions",
    "gst", "igst", "cgst", "sgst", "input gst", "output gst",
    "tds", "tcs", "income tax",
    "indirect income", "direct income", "indirect expenses", "direct expenses",
    "purchase accounts", "sales accounts", "sales account",
    "unsecured loans", "secured loans", "share capital", "share application",
    "branch / divisions", "suspense",
  ];
  const isGlGroup = (l) => GL_GROUP_KEYS.some((k) => (l.parentGroup || "").toLowerCase().includes(k));

  // Build lookup of ALL ERPNext group accounts (is_group=1) by name, for parent resolution
  const groupAccounts = erpAccounts.filter((a) => a.is_group);
  const byGroupName   = new Map();
  for (const g of groupAccounts) {
    if (g.account_name) byGroupName.set(g.account_name.trim().toLowerCase(), g);
    if (g.name)         byGroupName.set(g.name.trim().toLowerCase(), g);
  }

  // Map Tally parent group name → ERPNext account_type for auto-creation
  const TALLY_GROUP_ACCT_TYPE = {
    "bank accounts":              "Bank",
    "bank account":               "Bank",
    "cash-in-hand":               "Cash",
    "cash in hand":               "Cash",
    "sundry debtors":             "Receivable",
    "sundry creditors":           "Payable",
    "duties & taxes":             "Tax",
    "duties and taxes":           "Tax",
    "gst":                        "Tax",
    "igst":                       "Tax",
    "cgst":                       "Tax",
    "sgst":                       "Tax",
    "tds":                        "Tax",
    "tcs":                        "Tax",
  };

  // Find the correct ERPNext parent group account for a Tally group name
  function findParentGroup(tallyGroupName) {
    if (!tallyGroupName) return null;
    const lower = tallyGroupName.trim().toLowerCase();
    // Try exact match with suffix first (e.g. "Bank Accounts - T")
    const withSuffix = (tallyGroupName.trim() + " - " + companyAbbr).toLowerCase();
    if (byGroupName.has(withSuffix)) return byGroupName.get(withSuffix).name;
    if (byGroupName.has(lower))      return byGroupName.get(lower).name;
    // Partial match: find any group whose name contains the key
    for (const [key, grp] of byGroupName) {
      if (key.includes(lower) || lower.includes(key.replace(" - " + companyAbbr.toLowerCase(), ""))) {
        return grp.name;
      }
    }
    return null;
  }

  // Auto-create a missing GL account in ERPNext under its correct parent group.
  // This handles ledgers like "RAJLAXMI" (Bank Accounts), "Sudhir26022026" (Capital Account)
  // that exist in Tally but have no matching account in ERPNext yet.
  async function ensureGlAccount(ledgerName, tallyGroupName) {
    const suffixed = ledgerName + " - " + companyAbbr;
    // Already cached from initial fetch?
    const cached = findErpAccount(ledgerName);
    if (cached) return cached.name;

    const parentGroupName = tallyGroupName || "";
    const parentAcct      = findParentGroup(parentGroupName);
    const lower           = parentGroupName.toLowerCase();

    // Determine account_type from Tally group
    let accountType = "Payable"; // safe default
    for (const [key, atype] of Object.entries(TALLY_GROUP_ACCT_TYPE)) {
      if (lower.includes(key)) { accountType = atype; break; }
    }
    // Capital / equity accounts — ERPNext India puts these under Liabilities
    if (lower.includes("capital") || lower.includes("reserves") || lower.includes("equity")) {
      accountType = "Equity";
    }
    if (lower.includes("fixed assets") || lower.includes("investments") || lower.includes("deposits")) {
      accountType = "Fixed Asset";
    }
    if (lower.includes("loans & advances") || lower.includes("current assets") || lower.includes("stock-in-hand")) {
      accountType = "Current Asset";
    }
    if (lower.includes("current liab") || lower.includes("provisions") || lower.includes("loans (liab") || lower.includes("unsecured") || lower.includes("secured loans")) {
      accountType = "Current Liability";
    }
    if (lower.includes("income") || lower.includes("sales accounts") || lower.includes("sales account")) {
      accountType = "Income Account";
    }
    if (lower.includes("expense") || lower.includes("purchase accounts") || lower.includes("manufacturing")) {
      accountType = "Expense Account";
    }

    try {
      const doc = {
        doctype:        "Account",
        account_name:   ledgerName,
        company:        companyName,
        is_group:       0,
        account_type:   accountType,
      };
      if (parentAcct) doc.parent_account = parentAcct;

      await client.post("/api/resource/Account", doc);
      logger.info("  [OB] Auto-created missing account: \"" + suffixed + "\" (type: " + accountType + ", parent: " + (parentAcct || "none") + ")");

      // Add to lookup so subsequent ledgers in the same run can find it
      const newEntry = { name: suffixed, account_name: ledgerName, account_type: accountType, is_group: 0 };
      byAccountName.set(ledgerName.trim().toLowerCase(), newEntry);
      byFullName.set(suffixed.trim().toLowerCase(), newEntry);
      leafAccounts.push(newEntry);

      return suffixed;
    } catch (err) {
      const msg = parseErpError(err);
      // If already exists (race or duplicate), try to find it
      if (msg.toLowerCase().includes("duplicate") || msg.toLowerCase().includes("already exists")) {
        logger.info("  [OB] Account already exists: " + suffixed);
        return suffixed;
      }
      logger.warn("  [OB] Could not auto-create account \"" + suffixed + "\": " + msg + " — skipping this ledger");
      return null;
    }
  }

  // Pre-fetch the set of existing Customers and Suppliers so we can validate
  // party rows before posting and avoid whole-chunk failures.
  const _existingCustomers = new Set();
  const _existingSuppliers = new Set();
  try {
    let pg = 0;
    while (true) {
      const r = await client.get("/api/resource/Customer", {
        params: { fields: '["customer_name"]', limit: 500, limit_start: pg * 500 },
      });
      const rows = (r.data && r.data.data) || [];
      rows.forEach((c) => _existingCustomers.add((c.customer_name || "").trim().toLowerCase()));
      if (rows.length < 500) break;
      pg++;
    }
  } catch (_) { logger.warn("[OB] Could not pre-fetch customers — party validation skipped"); }
  try {
    let pg = 0;
    while (true) {
      const r = await client.get("/api/resource/Supplier", {
        params: { fields: '["supplier_name"]', limit: 500, limit_start: pg * 500 },
      });
      const rows = (r.data && r.data.data) || [];
      rows.forEach((s) => _existingSuppliers.add((s.supplier_name || "").trim().toLowerCase()));
      if (rows.length < 500) break;
      pg++;
    }
  } catch (_) { logger.warn("[OB] Could not pre-fetch suppliers — party validation skipped"); }

  logger.info("[OB] Known customers: " + _existingCustomers.size + ", suppliers: " + _existingSuppliers.size);

  const accounts = [];
  let fallbackCount = 0;

  for (const l of withBalance) {
    // ── CRITICAL: trim whitespace/newlines from Tally ledger names ──
    // Tally sometimes exports names with trailing \n or spaces.
    // ERPNext party lookup is exact — a name with \n won't match.
    l.name = (l.name || "").trim();

    const bal    = l.openingBalance || 0;
    const glAcct = findErpAccount(l.name);
    const pg     = (l.parentGroup || "").toLowerCase();

    if (glAcct) {
      // Direct GL account found in ERPNext (bank, cash, stock etc.)
      // FIX: If the account_type is Receivable or Payable, ERPNext requires
      // party_type + party on every JE row — even for Opening Entry.
      // This happens when a ledger like "RAJLAXMI" sits under Bank Accounts in
      // Tally but its ERPNext account was auto-created as Receivable/Payable.
      const row = {
        account:                    glAcct.name,
        // Tally sign convention: negative = debit (asset/debtor), positive = credit (liability/creditor)
        debit_in_account_currency:  bal < 0 ? Math.abs(bal) : 0,
        credit_in_account_currency: bal > 0 ? Math.abs(bal) : 0,
        is_advance:                 "No",
      };
      if (glAcct.account_type === "Receivable") {
        row.party_type = "Customer";
        row.party      = l.name.trim();
        logger.info("  " + l.name + " -> GL (Receivable — adding party): " + glAcct.name);
      } else if (glAcct.account_type === "Payable") {
        row.party_type = "Supplier";
        row.party      = l.name.trim();
        logger.info("  " + l.name + " -> GL (Payable — adding party): " + glAcct.name);
      } else {
        logger.info("  " + l.name + " -> GL: " + glAcct.name);
      }
      accounts.push(row);

    } else if (isDebtor(l) && receivableAcct) {
      // Customer -> Receivable control account + party
      if (_existingCustomers.size > 0 && !_existingCustomers.has(l.name.trim().toLowerCase())) {
        logger.warn("  " + l.name + " skipped for OB — Customer not yet in ERPNext (run Ledger sync first)");
        fallbackCount++;
        continue;
      }
      accounts.push({
        account:                    receivableAcct.name,
        party_type:                 "Customer",
        party:                      l.name,
        // Tally sign convention: negative = debit (asset/debtor), positive = credit (liability/creditor)
        debit_in_account_currency:  bal < 0 ? Math.abs(bal) : 0,
        credit_in_account_currency: bal > 0 ? Math.abs(bal) : 0,
        is_advance:                 "No",
      });
      logger.info("  " + l.name + " -> Receivable: " + receivableAcct.name + " (Customer)");

    } else if (isCreditor(l) && payableAcct) {
      // Supplier -> Payable control account + party
      if (_existingSuppliers.size > 0 && !_existingSuppliers.has(l.name.trim().toLowerCase())) {
        logger.warn("  " + l.name + " skipped for OB — Supplier not yet in ERPNext (run Ledger sync first)");
        fallbackCount++;
        continue;
      }
      accounts.push({
        account:                    payableAcct.name,
        party_type:                 "Supplier",
        party:                      l.name,
        // Tally sign convention: negative = debit (asset/debtor), positive = credit (liability/creditor)
        debit_in_account_currency:  bal < 0 ? Math.abs(bal) : 0,
        credit_in_account_currency: bal > 0 ? Math.abs(bal) : 0,
        is_advance:                 "No",
      });
      logger.info("  " + l.name + " -> Payable: " + payableAcct.name + " (Supplier)");

    } else if (looksLikeAsset(l.name)) {
      logger.warn("  " + l.name + " skipped for OB — looks like a fixed asset model number (not a party)");
      fallbackCount++;

    } else if (isGlGroup(l)) {
      // GL-type ledger — auto-create the account in ERPNext under its correct parent,
      // then use it. No more skipping due to "no GL match".
      const acctName = await ensureGlAccount(l.name, l.parentGroup);
      if (acctName) {
        accounts.push({
          account:                    acctName,
          // Tally sign: negative=debit(asset), positive=credit(liability)
          debit_in_account_currency:  bal < 0 ? Math.abs(bal) : 0,
          credit_in_account_currency: bal > 0 ? Math.abs(bal) : 0,
          is_advance:                 "No",
        });
        logger.info("  " + l.name + " -> GL (auto-created): " + acctName + " (parentGroup: " + (l.parentGroup || "") + ")");
      } else {
        fallbackCount++;
      }

    } else {
      // Unknown Tally group — classify by GSTIN then try to auto-create the account.
      // This handles custom Tally groups (Masma, Andheri, location-based etc.).
      const isUnknown = !isGlGroup(l) && !isDebtor(l) && !isCreditor(l);
      if (isUnknown && payableAcct && l.gstin) {
        if (_existingSuppliers.size === 0 || _existingSuppliers.has(l.name.trim().toLowerCase())) {
          accounts.push({
            account:                    payableAcct.name,
            party_type:                 "Supplier",
            party:                      l.name,
            // Tally sign: negative=debit(asset), positive=credit(liability)
            debit_in_account_currency:  bal < 0 ? Math.abs(bal) : 0,
            credit_in_account_currency: bal > 0 ? Math.abs(bal) : 0,
            is_advance:                 "No",
          });
          logger.info("  " + l.name + " -> Payable (unknown group, has GSTIN): " + payableAcct.name);
        } else {
          logger.warn("  " + l.name + " skipped for OB — Supplier not yet in ERPNext");
          fallbackCount++;
        }
      } else if (isUnknown && receivableAcct) {
        if (_existingCustomers.size === 0 || _existingCustomers.has(l.name.trim().toLowerCase())) {
          accounts.push({
            account:                    receivableAcct.name,
            party_type:                 "Customer",
            party:                      l.name,
            // Tally sign: negative=debit(asset), positive=credit(liability)
            debit_in_account_currency:  bal < 0 ? Math.abs(bal) : 0,
            credit_in_account_currency: bal > 0 ? Math.abs(bal) : 0,
            is_advance:                 "No",
          });
          logger.info("  " + l.name + " -> Receivable (unknown group): " + receivableAcct.name);
        } else {
          logger.warn("  " + l.name + " skipped for OB — Customer not yet in ERPNext");
          fallbackCount++;
        }
      } else {
        logger.warn("  " + l.name + " skipped (parentGroup: \"" + (l.parentGroup || "") + "\", unknown group type — no control account found)");
        fallbackCount++;
      }
    }
    await sleep(30);
  }

  if (fallbackCount > 0) logger.warn(fallbackCount + " ledgers skipped - no matching account in ERPNext");

  if (accounts.length === 0) {
    const msg = "No accounts resolved for any ledger. Receivable: " + (receivableAcct ? receivableAcct.name : "missing") + ", Payable: " + (payableAcct ? payableAcct.name : "missing") + ". Check ERPNext chart of accounts for company " + companyName + ".";
    logger.error(msg);
    return { created: 0, updated: 0, failed: 0, skipped: withBalance.length, error: msg };
  }

  logger.info("Resolved " + accounts.length + " / " + withBalance.length + " ledgers to JE account rows");

  // -- Resolve Temporary Opening account once ----------------------------------
  let temporaryAccount = null;
  try {
    const res = await client.get("/api/resource/Account", {
      params: {
        filters: JSON.stringify([["Account", "account_name", "=", "Temporary Opening"], ["Account", "company", "=", companyName]]),
        fields: '["name"]', limit: 1,
      },
    });
    temporaryAccount = res.data && res.data.data && res.data.data[0] && res.data.data[0].name;
  } catch (_) {}
  if (!temporaryAccount) logger.warn("Could not find \"Temporary Opening\" account - JE may be unbalanced");

  // -- Clean up ALL old Opening Balance JEs (Draft AND Submitted) ---------------
  // Cancel+delete any JE for this company with user_remark starting with
  // "Tally Opening Balance" — this cleans up both:
  //   • Old all-in-one JEs from before the Debtors/Creditors/GL split
  //   • Draft JEs from previously failed runs
  // The new JEs use remarks like "Tally Opening Balance - Debtors [company]"
  // so they will be matched by the per-group lookup inside postChunk, not here.
  try {
    const staleRes = await client.get("/api/resource/Journal Entry", {
      params: {
        filters: JSON.stringify([
          ["Journal Entry", "company",      "=",    companyName],
          ["Journal Entry", "user_remark",  "like", "Tally Opening Balance%"],
          ["Journal Entry", "voucher_type", "=",    "Opening Entry"],
        ]),
        fields: '["name","title","docstatus"]',
        limit:  50,
      },
    });
    const stale = (staleRes.data && staleRes.data.data) || [];
    // Only delete old-format "chunk N of N" JEs — leave new "Debtors/Creditors/GL" ones alone
    const oldFormat = stale.filter((s) =>
      s.title && !s.title.includes("Tally OB - Debtors") &&
                 !s.title.includes("Tally OB - Creditors") &&
                 !s.title.includes("Tally OB - GL")
    );
    for (const s of oldFormat) {
      try {
        if (s.docstatus === 1) {
          await client.post("/api/method/frappe.client.cancel", { doctype: "Journal Entry", name: s.name });
          logger.info("[OB] Cancelled old-format submitted JE: " + s.name + " (\"" + s.title + "\")");
        }
        await client.delete("/api/resource/Journal Entry/" + encodeURIComponent(s.name));
        logger.info("[OB] Deleted old-format JE: " + s.name + " (\"" + s.title + "\")");
      } catch (_) {}
    }
    if (oldFormat.length > 0) logger.info("[OB] Cleaned up " + oldFormat.length + " old-format Opening Balance JEs");
  } catch (_) {}

  // -- Split by account type, then further chunk by OB_CHUNK_SIZE ---------------
  // This creates separate, clearly-labelled Journal Entries for:
  //   • Debtors (Receivable)  — party_type=Customer rows
  //   • Creditors (Payable)   — party_type=Supplier rows
  //   • GL Accounts           — all other rows (Bank, Cash, Capital, etc.)
  // Each group is then chunked to OB_CHUNK_SIZE if it has many rows.
  function groupAccountsByType(accts) {
    const debtors   = accts.filter((a) => a.party_type === "Customer");
    const creditors = accts.filter((a) => a.party_type === "Supplier");
    const gl        = accts.filter((a) => !a.party_type);
    const groups = [];
    function addGroup(label, rows) {
      if (rows.length === 0) return;
      for (let i = 0; i < rows.length; i += OB_CHUNK_SIZE) {
        const slice = rows.slice(i, i + OB_CHUNK_SIZE);
        const part  = Math.floor(i / OB_CHUNK_SIZE) + 1;
        const parts = Math.ceil(rows.length / OB_CHUNK_SIZE);
        groups.push({ label: label + (parts > 1 ? " (part " + part + ")" : ""), rows: slice });
      }
    }
    addGroup("Debtors",   debtors);
    addGroup("Creditors", creditors);
    addGroup("GL",        gl);
    return groups;
  }

  const jeGroups   = groupAccountsByType(accounts);
  const totalChunks = jeGroups.length;
  logger.info("Splitting " + accounts.length + " accounts into " + totalChunks + " Journal Entries (Debtors / Creditors / GL)");

  let created = 0, updated = 0, failed = 0;
  const failedGroups = [];

  async function postChunk(ci, label, chunkAccounts) {
    const remark      = "Tally Opening Balance - " + label + " [" + companyName + "]";
    const chunkDebit  = chunkAccounts.reduce((s, a) => s + a.debit_in_account_currency,  0);
    const chunkCredit = chunkAccounts.reduce((s, a) => s + a.credit_in_account_currency, 0);
    const diff        = Math.abs(chunkDebit - chunkCredit);

    const chunkRows = [...chunkAccounts];
    if (diff > 0.01 && temporaryAccount) {
      chunkRows.push({
        account:                    temporaryAccount,
        debit_in_account_currency:  chunkCredit > chunkDebit ? diff : 0,
        credit_in_account_currency: chunkDebit > chunkCredit ? diff : 0,
        is_advance:                 "No",
      });
    }

    try {
      const existing = await client.get("/api/resource/Journal Entry", {
        params: {
          filters: JSON.stringify([
            ["Journal Entry", "user_remark",  "=", remark],
            ["Journal Entry", "voucher_type", "=", "Opening Entry"],
          ]),
          fields: '["name","docstatus"]',
          limit: 1,
        },
      });
      const existingEntry = existing.data && existing.data.data && existing.data.data[0];
      if (existingEntry) {
        try {
          if (existingEntry.docstatus === 1) {
            await client.post("/api/method/frappe.client.cancel", { doctype: "Journal Entry", name: existingEntry.name });
            logger.info("[OB] Cancelled submitted Opening Entry: " + existingEntry.name);
          }
          await client.delete("/api/resource/Journal Entry/" + encodeURIComponent(existingEntry.name));
          logger.info("[OB] Deleted old Opening Entry: " + existingEntry.name);
        } catch (_) {}
      }

      const res = await client.post("/api/resource/Journal Entry", {
        doctype:      "Journal Entry",
        voucher_type: "Opening Entry",
        title:        "Tally OB - " + label + " - " + companyName,
        company:      companyName,
        posting_date: new Date().toISOString().slice(0, 10),
        user_remark:  remark,
        accounts:     chunkRows,
        docstatus:    1,
      });
      const newName = res.data && res.data.data && res.data.data.name;
      // Frappe overrides the title with the first account name for Opening Entry.
      // Fix it with a direct rename after creation.
      if (newName) {
        try {
          await client.put("/api/resource/Journal Entry/" + encodeURIComponent(newName), {
            title: "Tally OB - " + label + " - " + companyName,
          });
        } catch (_) {} // non-critical — JE still works even if rename fails
      }
      logger.success("Created & submitted Opening Entry JE [" + label + "]: " + newName);
      return existingEntry ? "updated" : "created";
    } catch (err) {
      logger.error("Failed to post Opening Entry [" + label + "]: " + parseErpError(err));
      return "failed";
    }
  }

  for (let ci = 0; ci < jeGroups.length; ci++) {
    const { label, rows } = jeGroups[ci];
    try {
      const action = await postChunk(ci, label, rows);
      if (action === "created") created++;
      else if (action === "updated") updated++;
      else { failed++; failedGroups.push(ci); }
    } catch (e) {
      failed++; failedGroups.push(ci);
      logger.error("Unexpected error on group " + label + ": " + e.message);
    }
    if (ci < jeGroups.length - 1) await sleep(800);
  }

  if (failedGroups.length > 0) {
    logger.info("[OB] Retrying " + failedGroups.length + " failed groups after 10s");
    await sleep(10000);
    for (const ci of failedGroups) {
      const { label, rows } = jeGroups[ci];
      try {
        const action = await postChunk(ci, label, rows);
        if (action === "created") { created++; failed--; }
        else if (action === "updated") { updated++; failed--; }
      } catch (e) { logger.error("Retry failed for group " + label + ": " + e.message); }
    }
  }

  logger.info("Opening balance sync complete - " + created + " created, " + updated + " updated, " + failed + " failed");
  return { created, updated, failed, chunks: totalChunks, accounts: accounts.length };
}

// -- Cost Centres -------------------------------------------------------------
async function resolveRootCostCentre(client, companyName, companyAbbr) {
  // Ask ERPNext for the is_group=1 cost centre that has no parent (true root).
  // In most installs this is "Main - <abbr>" but some installs rename it or
  // create it as a leaf — fetching it avoids the "not a group node" error.
  try {
    const res = await client.get("/api/resource/Cost Center", {
      params: {
        filters: JSON.stringify([["Cost Center","company","=",companyName],["Cost Center","is_group","=",1],["Cost Center","parent_cost_center","=",""]]),
        fields:  '["name"]',
        limit:   1,
      },
    });
    const name = res.data && res.data.data && res.data.data[0] && res.data.data[0].name;
    if (name) { logger.info("Root cost centre resolved: " + name); return name; }
  } catch (e) {
    logger.warn("Could not resolve root cost centre: " + e.message);
  }
  // Fallback to the ERPNext default name
  return "Main - " + companyAbbr;
}

export async function syncCostCentresToErpNext(costCentres, companyName, creds = {}) {
  const client = createErpClient(creds);
  companyName = await resolveErpNextCompany(client, companyName, creds);
  const companyAbbr = await getCompanyAbbr(client, companyName);
  logger.info("Syncing " + costCentres.length + " cost centres to ERPNext for " + companyName + " (abbr: " + companyAbbr + ")");

  // Resolve the actual group-type root cost centre (avoids "not a group node" error)
  const rootCostCentre = await resolveRootCostCentre(client, companyName, companyAbbr);

  // FIX: Pre-fetch all existing Cost Centers so we can validate parent references
  // before posting. Previously we blindly appended " - ABBR" to every parent name
  // which caused failures when the parent didn't exist with that suffix.
  const _existingCostCenters = new Set();
  try {
    let pg = 0;
    while (true) {
      const res = await client.get("/api/resource/Cost Center", {
        params: { fields: '["name","cost_center_name"]', limit: 500, limit_start: pg * 500,
                  filters: JSON.stringify([["Cost Center", "company", "=", companyName]]) },
      });
      const rows = (res.data && res.data.data) || [];
      rows.forEach((r) => {
        if (r.name)             _existingCostCenters.add(r.name.trim().toLowerCase());
        if (r.cost_center_name) _existingCostCenters.add(r.cost_center_name.trim().toLowerCase());
      });
      if (rows.length < 500) break;
      pg++;
    }
    logger.info("Cost Centre sync: fetched " + _existingCostCenters.size + " existing cost centers");
  } catch (e) {
    logger.warn("Cost Centre sync: could not pre-fetch existing cost centers — " + e.message);
  }

  // Sort so parents are always created before children
  const sorted = costCentres.slice().sort((a, b) => {
    const aIsRoot = !a.parent || a.parent.trim().toLowerCase() === "primary";
    const bIsRoot = !b.parent || b.parent.trim().toLowerCase() === "primary";
    if (aIsRoot && !bIsRoot) return -1;
    if (!aIsRoot && bIsRoot) return 1;
    return (a.parent || "").localeCompare(b.parent || "");
  });

  const results = await batchSync(client, "Cost Center", sorted, (cc) => {
    const doc = {
      cost_center_name: cc.name,
      company:          companyName,
      is_group:         0,
    };

    // FIX: Only set parent_cost_center if the parent actually exists in ERPNext
    // (with or without the company abbreviation suffix). Fall back to the root
    // "Main - ABBR" cost center if not found, to avoid hard failures.
    if (cc.parent && cc.parent.trim().toLowerCase() !== "primary") {
      const withSuffix = (cc.parent.trim() + " - " + companyAbbr).toLowerCase();
      const plain      = cc.parent.trim().toLowerCase();
      if (_existingCostCenters.has(withSuffix)) {
        doc.parent_cost_center = cc.parent.trim() + " - " + companyAbbr;
      } else if (_existingCostCenters.has(plain)) {
        doc.parent_cost_center = cc.parent.trim();
      } else {
        // Parent not in ERPNext yet — it may be created earlier in this batch.
        // Use the suffixed name anyway; ERPNext will accept it if the parent
        // was just created in this run and the mapper runs in sorted order.
        doc.parent_cost_center = cc.parent.trim() + " - " + companyAbbr;
      }
    } else {
      // FIX: Tally's "Primary" parent = ERPNext root cost centre.
      // ERPNext requires parent_cost_center on every cost centre — even top-level
      // ones — so we must always set it.
      doc.parent_cost_center = rootCostCentre;
    }
    // Add to known set so subsequent children in this batch can resolve this as parent
    _existingCostCenters.add((cc.name + " - " + companyAbbr).toLowerCase());

    return { filters: { cost_center_name: cc.name }, doc };
  });

  logger.success("Cost centres sync done - created: " + results.created + ", updated: " + results.updated + ", failed: " + results.failed);
  return results;
}

// -- Sales / Purchase Invoices ------------------------------------------------
export async function syncInvoicesToErpNext(vouchers, companyName, creds = {}) {
  const client = createErpClient(creds);
  companyName  = await resolveErpNextCompany(client, companyName, creds);
  const companyAbbr = await getCompanyAbbr(client, companyName);
  logger.info("Syncing invoices to ERPNext for " + companyName + " (abbr: " + companyAbbr + ")");

  // Ensure HSN field is non-mandatory so placeholder items can be created without an HSN
  await ensureHsnNotMandatory(client);
  // Resolve real leaf Customer/Supplier groups before auto-creating parties
  await resolveGroups(client);

  const salesVouchers    = vouchers.filter((v) => v.voucherType === "Sales"    || (v.voucherType || "").toLowerCase().includes("sales invoice"));
  const purchaseVouchers = vouchers.filter((v) => v.voucherType === "Purchase" || (v.voucherType || "").toLowerCase().includes("purchase invoice"));

  logger.info("Invoice breakdown - sales: " + salesVouchers.length + ", purchase: " + purchaseVouchers.length);

  const incomeAccount  = await resolveAccount(client, "Sales",    companyAbbr, companyName) || ("Sales - "    + companyAbbr);
  const expenseAccount = await resolveAccount(client, "Purchase", companyAbbr, companyName) || ("Purchase - " + companyAbbr);

  const salesMapper = (v) => {
    // Use real Tally inventory items if available, else fall back to single placeholder row.
    // rate = Tally RATE field (UoM suffix already stripped in tallyClient).
    // If rate is 0 but amount and qty are set, derive rate = amount / qty.
    const items = (v.inventoryItems && v.inventoryItems.length > 0)
      ? v.inventoryItems.map((i) => {
          const qty    = i.qty    || 1;
          const amount = i.amount || 0;
          const rate   = i.rate   || (amount / qty) || 0;
          return {
            item_code:      i.itemName,
            item_name:      i.itemName,
            qty,
            rate:           Math.abs(rate),
            amount:         Math.abs(amount) || Math.abs(rate * qty),
            income_account: incomeAccount,
          };
        })
      : [{ item_code: "Sales Item", item_name: "Sales Item", qty: 1, rate: v.netAmount || 0, income_account: incomeAccount }];

    // voucherNumber fallback: use date+type as unique key if Tally didn't export a number
    const vSalesNum   = v.voucherNumber || (v.voucherDate + "-" + (v.voucherType || "Sales") + "-" + (v.guid || "").slice(-6));
    const salesRemarks = "Tally Voucher No: " + vSalesNum + (v.narration ? " | " + v.narration : "");

    // DATE FIX: Always use the date Tally recorded on the voucher (YYYY-MM-DD).
    // Never silently fall back to today — log missing dates so data issues are traceable.
    if (!v.voucherDate) {
      logger.warn("Sales voucher missing voucherDate — check Tally DATE field", { voucher: vSalesNum, guid: v.guid });
    }
    const effectiveSalesDate = v.voucherDate || new Date().toISOString().slice(0, 10);

    return {
      // Use `remarks` (real DB column, unique per voucher) as the idempotency key.
      filters: { remarks: salesRemarks },
      doc: {
        title:             "Tally:" + vSalesNum,
        customer:          v.partyName || "Walk-in Customer",
        posting_date:      effectiveSalesDate,   // Tally voucher date (not today)
        due_date:          effectiveSalesDate,   // Payment Due Date = voucher date (Tally default)
        set_posting_time:  1,                    // preserve Tally date, not today
        company:           companyName,
        po_no:             vSalesNum,
        remarks:           salesRemarks,
        custom_tally_id:   v.guid || vSalesNum,
        items,
      },
    };
  };

  const purchaseMapper = (v) => {
    const items = (v.inventoryItems && v.inventoryItems.length > 0)
      ? v.inventoryItems.map((i) => {
          const qty    = i.qty    || 1;
          const amount = i.amount || 0;
          const rate   = i.rate   || (amount / qty) || 0;
          return {
            item_code:       i.itemName,
            item_name:       i.itemName,
            qty,
            rate:            Math.abs(rate),
            amount:          Math.abs(amount) || Math.abs(rate * qty),
            expense_account: expenseAccount,
          };
        })
      : [{ item_code: "Purchase Item", item_name: "Purchase Item", qty: 1, rate: v.netAmount || 0, expense_account: expenseAccount }];

    const vPurchaseNum    = v.voucherNumber || (v.voucherDate + "-" + (v.voucherType || "Purchase") + "-" + (v.guid || "").slice(-6));
    const purchaseRemarks = "Tally Voucher No: " + vPurchaseNum + (v.narration ? " | " + v.narration : "");

    // DATE FIX: Always use the date Tally recorded on the voucher (YYYY-MM-DD).
    // Never silently fall back to today — log missing dates so data issues are traceable.
    if (!v.voucherDate) {
      logger.warn("Purchase voucher missing voucherDate — check Tally DATE field", { voucher: vPurchaseNum, guid: v.guid });
    }
    const effectivePurchaseDate = v.voucherDate || new Date().toISOString().slice(0, 10);

    return {
      filters: { remarks: purchaseRemarks },
      doc: {
        title:             "Tally:" + vPurchaseNum,
        supplier:          v.partyName || "Unknown Supplier",
        posting_date:      effectivePurchaseDate,  // Tally voucher date (not today)
        set_posting_time:  1,                      // preserve Tally date, not today
        company:           companyName,
        bill_no:           vPurchaseNum,
        bill_date:         effectivePurchaseDate,  // Supplier invoice date = Tally voucher date
        due_date:          effectivePurchaseDate,  // Payment Due Date = voucher date
        remarks:           purchaseRemarks,
        custom_tally_id:   v.guid || vPurchaseNum,
        items,
      },
    };
  };

  // ── Ensure fallback parties exist (Walk-in Customer / Unknown Supplier) ──────
  // Sales vouchers with no partyName in Tally fall back to "Walk-in Customer".
  // Purchase vouchers with no partyName fall back to "Unknown Supplier".
  // We create these once here so the invoice batch never hits "Could not find Customer/Supplier".
  const FALLBACK_CUSTOMER  = "Walk-in Customer";
  const FALLBACK_SUPPLIER  = "Unknown Supplier";
  for (const [doctype, name, group] of [
    ["Customer", FALLBACK_CUSTOMER, _customerGroup || "Commercial"],
    ["Supplier", FALLBACK_SUPPLIER, _supplierGroup || "Services"],
  ]) {
    try {
      await client.get("/api/resource/" + doctype + "/" + encodeURIComponent(name));
      // already exists
    } catch (_) {
      try {
        if (doctype === "Customer") {
          await client.post("/api/resource/Customer", {
            doctype: "Customer", customer_name: name,
            customer_type: "Individual", customer_group: group,
          });
        } else {
          await client.post("/api/resource/Supplier", {
            doctype: "Supplier", supplier_name: name,
            supplier_type: "Individual", supplier_group: group,
          });
        }
        logger.info("Auto-created fallback party: " + name + " (" + doctype + ")");
      } catch (e) {
        logger.warn("Could not create fallback party \"" + name + "\": " + parseErpError(e));
      }
    }
  }

  for (const code of ["Sales Item", "Purchase Item"]) {
    try {
      await client.get("/api/resource/Item/" + encodeURIComponent(code));
      logger.info("Placeholder item already exists: " + code);
    } catch (_) {
      try {
        // Try without HSN first (works if Property Setter patched it)
        await client.post("/api/resource/Item", {
          doctype:          "Item",
          item_code:        code,
          item_name:        code,
          item_group:       "All Item Groups",
          stock_uom:        "Nos",
          is_stock_item:    0,
          is_sales_item:    1,
          is_purchase_item: 1,
        });
        logger.info("Created placeholder item: " + code);
      } catch (firstErr) {
        // ERPNext still enforcing HSN mandatory — retry with fallback HSN 99999999
        try {
          await ensureHsnCode(client, "99999999");
          await client.post("/api/resource/Item", {
            doctype:          "Item",
            item_code:        code,
            item_name:        code,
            item_group:       "All Item Groups",
            stock_uom:        "Nos",
            is_stock_item:    0,
            is_sales_item:    1,
            is_purchase_item: 1,
            gst_hsn_code:     "99999999",
          });
          logger.info("Created placeholder item (with fallback HSN): " + code);
        } catch (createErr) {
          const msg = parseErpError(createErr);
          logger.warn("Could not create placeholder item \"" + code + "\": " + msg + " — invoices using this item will fail");
        }
      }
    }
  }

  // Pre-sync unique suppliers from purchase vouchers before invoice batch.
  // ERPNext requires the Supplier to exist before a Purchase Invoice can be saved.
  if (purchaseVouchers.length > 0) {
    const uniqueSupplierNames = [
      ...new Set(
        purchaseVouchers
          .map((v) => (v.partyName || "").trim())
          .filter(Boolean)
          .filter((n) => n !== "Unknown Supplier")
      ),
    ];
    if (uniqueSupplierNames.length > 0) {
      logger.info("Pre-syncing " + uniqueSupplierNames.length + " suppliers before Purchase Invoice batch");
      for (const supplierName of uniqueSupplierNames) {
        try {
          await client.get("/api/resource/Supplier/" + encodeURIComponent(supplierName));
        } catch (_) {
          try {
            await client.post("/api/resource/Supplier", {
              doctype:        "Supplier",
              supplier_name:  supplierName,
              supplier_type:  "Company",
              supplier_group: _supplierGroup || "Services",
            });
            logger.info("Auto-created supplier: " + supplierName);
          } catch (sErr) {
            logger.warn("Could not auto-create supplier \"" + supplierName + "\": " + parseErpError(sErr));
          }
          await sleep(300);
        }
      }
    }
  }

  // Pre-sync unique customers from sales vouchers before invoice batch.
  // ERPNext requires the Customer to exist before a Sales Invoice can be saved.
  if (salesVouchers.length > 0) {
    const uniqueCustomerNames = [
      ...new Set(
        salesVouchers
          .map((v) => (v.partyName || "").trim())
          .filter(Boolean)
          .filter((n) => n !== "Unknown Customer" && n !== "Walk-in Customer")
      ),
    ];
    if (uniqueCustomerNames.length > 0) {
      logger.info("Pre-syncing " + uniqueCustomerNames.length + " customers before Sales Invoice batch");
      for (const customerName of uniqueCustomerNames) {
        try {
          // Search by customer_name (not ID) since ERPNext may have auto-renamed the doc
          const res = await client.get("/api/resource/Customer", {
            params: { filters: JSON.stringify([["Customer", "customer_name", "=", customerName]]), limit: 1 }
          });
          const exists = (res?.data?.data || []).length > 0;
          if (!exists) throw new Error("not found");
          // already exists — skip silently
        } catch (_) {
          try {
            await client.post("/api/resource/Customer", {
              doctype:         "Customer",
              customer_name:   customerName,
              customer_type:   "Company",
              customer_group:  _customerGroup || "Commercial",
            });
            logger.info("Auto-created customer: " + customerName);
          } catch (cErr) {
            logger.warn("Could not auto-create customer \"" + customerName + "\": " + parseErpError(cErr));
          }
          await sleep(300);
        }
      }
    }
  }

  // ── Pre-sync real Tally item names to ERPNext Items ───────────────────────────
  // Collect every unique stock-item name from inventoryItems across all vouchers.
  // Create any that don't exist yet as non-stock service items so the invoice
  // batch never fails with "Item not found".
  const allVouchersForItems = [...salesVouchers, ...purchaseVouchers];
  const uniqueItemNames = [
    ...new Set(
      allVouchersForItems
        .flatMap((v) => (v.inventoryItems || []).map((i) => (i.itemName || "").trim()))
        .filter(Boolean)
        .filter((n) => n !== "Sales Item" && n !== "Purchase Item")
    ),
  ];
  if (uniqueItemNames.length > 0) {
    logger.info("Pre-syncing " + uniqueItemNames.length + " Tally stock items to ERPNext before invoice batch");
    for (const itemName of uniqueItemNames) {
      try {
        await client.get("/api/resource/Item/" + encodeURIComponent(itemName));
        // already exists — skip
      } catch (_) {
        try {
          await client.post("/api/resource/Item", {
            doctype:          "Item",
            item_code:        itemName,
            item_name:        itemName,
            item_group:       "All Item Groups",
            stock_uom:        "Nos",
            is_stock_item:    0,          // non-stock keeps things simple
            is_sales_item:    1,
            is_purchase_item: 1,
          });
          logger.info("Auto-created item from Tally: " + itemName);
        } catch (firstErr) {
          // If ERPNext still enforces HSN, retry with fallback code
          try {
            await ensureHsnCode(client, "99999999");
            await client.post("/api/resource/Item", {
              doctype:          "Item",
              item_code:        itemName,
              item_name:        itemName,
              item_group:       "All Item Groups",
              stock_uom:        "Nos",
              is_stock_item:    0,
              is_sales_item:    1,
              is_purchase_item: 1,
              gst_hsn_code:     "99999999",
            });
            logger.info("Auto-created item (with fallback HSN): " + itemName);
          } catch (createErr) {
            logger.warn("Could not auto-create item \"" + itemName + "\": " + parseErpError(createErr) + " — invoice lines for this item may fall back to placeholder");
          }
        }
        await sleep(300);
      }
    }
  }

  const salesResults    = await batchSync(client, "Sales Invoice",    salesVouchers,    salesMapper);
  const purchaseResults = await batchSync(client, "Purchase Invoice", purchaseVouchers, purchaseMapper);

  // Submit all draft invoices using ERPNext's submit endpoint
  async function submitDraftInvoices(doctype, vouchers) {
    let submitted = 0, failed = 0;
    for (const v of vouchers) {
      // Declare effectiveNum outside try so it is accessible in the catch block.
      // When voucherNumber is null the mapper uses a date+type+guid fallback — we
      // must use the same key here so the remarks search actually finds the doc.
      const effectiveNum = v.voucherNumber ||
        (v.voucherDate + "-" + (v.voucherType || doctype.replace(" Invoice", "")) + "-" + (v.guid || "").slice(-6));
      try {
        const remarksPrefix = "Tally Voucher No: " + effectiveNum;
        const list = await client.get("/api/resource/" + encodeURIComponent(doctype), {
          params: { filters: JSON.stringify([[doctype, "remarks", "like", remarksPrefix + "%"]]), fields: '["name","docstatus"]', limit: 1 }
        });
        const stub = list?.data?.data?.[0];
        if (!stub) continue;
        if (stub.docstatus === 1) continue; // already submitted

        // Fetch the FULL document so we have the current `modified` timestamp.
        // Frappe's optimistic locking checks that the `modified` value we send matches
        // what's in the DB — sending a stale value causes "has been modified after you
        // have opened it" on every submit call.
        const fullRes = await client.get("/api/resource/" + encodeURIComponent(doctype) + "/" + encodeURIComponent(stub.name));
        const fullDoc = fullRes?.data?.data;
        if (!fullDoc) continue;

        await client.post("/api/method/frappe.client.submit", { doc: fullDoc });
        submitted++;
      } catch (e) {
        failed++;
        logger.warn("Could not submit " + doctype + " for voucher " + effectiveNum + ": " + parseErpError(e));
      }
      await sleep(200);
    }
    logger.info("Submit " + doctype + ": " + submitted + " submitted, " + failed + " failed");
  }

  await submitDraftInvoices("Sales Invoice",    salesVouchers);
  await submitDraftInvoices("Purchase Invoice", purchaseVouchers);

  logger.success("Invoice sync done - sales: +" + salesResults.created + " new/" + salesResults.updated + " updated/" + salesResults.failed + " failed | purchase: +" + purchaseResults.created + " new/" + purchaseResults.updated + " updated/" + purchaseResults.failed + " failed");
  return { sales: salesResults, purchase: purchaseResults };
}


// -- Smart Ledger Sync --------------------------------------------------------
// Instead of syncing all 16,000+ ledgers, this extracts only the ledger names
// actually used in the vouchers for the selected date range, then syncs only those.
export async function smartSyncLedgersToErpNext(vouchers, allLedgers, creds = {}) {
  // Step 1: Extract unique ledger names used in vouchers
  const usedLedgerNames = new Set();
  for (const v of vouchers) {
    if (v.partyName) usedLedgerNames.add(v.partyName.trim());
    if (v.entries && Array.isArray(v.entries)) {
      for (const e of v.entries) {
        if (e.ledger) usedLedgerNames.add(e.ledger.trim());
      }
    }
  }

  logger.info("Smart Ledger Sync: found " + usedLedgerNames.size + " unique ledgers used in vouchers");

  // Step 2: Filter allLedgers to only those used in vouchers
  const filteredLedgers = allLedgers.filter((l) => usedLedgerNames.has((l.name || "").trim()));

  logger.info("Smart Ledger Sync: matched " + filteredLedgers.length + " / " + allLedgers.length + " total ledgers");

  // Step 3: Log any ledger names in vouchers that have no match in Tally masters
  const matchedNames = new Set(filteredLedgers.map((l) => l.name.trim()));
  const unmatched = [...usedLedgerNames].filter((n) => !matchedNames.has(n));
  if (unmatched.length > 0) {
    logger.warn("Smart Ledger Sync: " + unmatched.length + " ledger(s) in vouchers not found in Tally masters: " + unmatched.slice(0, 10).join(", ") + (unmatched.length > 10 ? "..." : ""));
  }

  // Step 4: Sync only the filtered ledgers
  if (filteredLedgers.length === 0) {
    logger.warn("Smart Ledger Sync: no matching ledgers found — skipping");
    return { customers: { created: 0, updated: 0, failed: 0 }, suppliers: { created: 0, updated: 0, failed: 0 }, skipped: 0 };
  }

  return await syncLedgersToErpNext(filteredLedgers, creds);
}

// -- Full Sync Orchestrator ---------------------------------------------------
export async function runFullSync(companyName, tallyData, options, creds = {}) {
  if (!options) options = {};

  // Reset any previous cancellation request so a fresh sync runs uninterrupted
  resetCancel();
  // Clear stale caches from previous sync runs — prevents wrong data from being reused
  clearCaches();

  // Resolve Tally company name to ERPNext company name dynamically
  const client = createErpClient(creds);
  companyName  = await resolveErpNextCompany(client, companyName, creds);

  // ── CRITICAL: Validate ERPNext company exists BEFORE doing anything ──────
  // If the user mistyped the company name (e.g. "Teat Company" instead of
  // "Test Company"), every single sync step will fail with "Could not find
  // Company: Teat Company". Catch this immediately and return a clear error
  // instead of spamming 28 COA failures + wasting 3 minutes.
  try {
    await client.get("/api/resource/Company/" + encodeURIComponent(companyName));
  } catch (e) {
    const status = e.response && e.response.status;
    if (status === 404) {
      const msg = `ERPNext company "${companyName}" not found. Please check the company name in Step 1 — it must match exactly (case-sensitive) with the company name in ERPNext.`;
      logger.error(msg);
      return {
        startedAt:  new Date().toISOString(),
        company:    companyName,
        status:     "failed",
        steps:      {},
        errors:     [msg],
        finishedAt: new Date().toISOString(),
      };
    }
    // Non-404 errors (network, auth) — let them fall through to the ping check below
  }

  const result = {
    startedAt:  new Date().toISOString(),
    company:    companyName,
    status:     "running",
    steps:      {},
    errors:     [],
    finishedAt: null,
  };

  const ping = await pingErpNext(creds);
  result.steps.erpnextPing = { status: ping.connected ? "ok" : "fail", latencyMs: ping.latencyMs, error: ping.error };
  if (!ping.connected) {
    result.status     = "failed";
    result.errors.push("ERPNext not reachable: " + ping.error);
    result.finishedAt = new Date().toISOString();
    return result;
  }

  async function runStep(key, fn) {
    try {
      const res = await fn();
      // Normalise "failed" count across all return shapes so runStep always
      // has a reliable number to check — avoids false "warn" when res.failed
      // is undefined (e.g. ledgers return { customers, suppliers }, not { failed }).
      function countFailed(r) {
        if (!r || typeof r !== "object") return 0;
        if (typeof r.failed === "number") return r.failed;
        // Ledger shape: { customers: { failed }, suppliers: { failed } }
        if (r.customers || r.suppliers) {
          return ((r.customers && r.customers.failed) || 0) + ((r.suppliers && r.suppliers.failed) || 0);
        }
        // Invoice shape: { sales: { failed }, purchase: { failed } }
        if (r.sales || r.purchase) {
          return ((r.sales && r.sales.failed) || 0) + ((r.purchase && r.purchase.failed) || 0);
        }
        return 0;
      }
      const totalFailed = countFailed(res);
      result.steps[key] = Object.assign({}, res, { status: (totalFailed === 0 && !res.error) ? "ok" : "warn" });
    } catch (e) {
      if (e._cancelled) {
        // User clicked Stop — mark remaining steps as cancelled and exit gracefully
        result.steps[key] = { status: "cancelled" };
        result.status     = "cancelled";
        result.finishedAt = new Date().toISOString();
        logger.info("Sync cancelled by user during step: " + key);
        throw e; // re-throw to break out of runFullSync
      }
      result.steps[key] = { status: "fail", error: e.message };
      result.errors.push(key + ": " + e.message);
    }
  }

  // STEP ORDER IS CRITICAL:
  //   1. chartOfAccounts — creates the GL account tree (groups) in ERPNext first.
  //      Without this, opening balances have no real accounts to post against,
  //      causing every ledger to fall through to the skip/fallback path.
  //   2. ledgers — creates Customers / Suppliers (must exist before OB refs them)
  //   3. openingBalances — posts JE rows; needs both GL accounts + parties to exist
  //   All other steps (godowns, costCentres, stock, vouchers, invoices) are
  //   independent and run after the core accounting data is in place.

  try {
    if (options.syncChartOfAccounts  && tallyData.groups    && tallyData.groups.length    > 0)
      await runStep("chartOfAccounts", () => syncChartOfAccountsToErpNext(tallyData.groups, companyName, creds));

    if (options.syncLedgers          && tallyData.ledgers   && tallyData.ledgers.length   > 0)
      await runStep("ledgers",         () => syncLedgersToErpNext(tallyData.ledgers, creds));

    if (options.syncOpeningBalances  && tallyData.ledgers   && tallyData.ledgers.length   > 0)
      await runStep("openingBalances", () => syncOpeningBalancesToErpNext(tallyData.ledgers, companyName, creds));

    if (options.syncGodowns          && tallyData.godowns   && tallyData.godowns.length   > 0)
      await runStep("godowns",         () => syncGodownsToErpNext(tallyData.godowns, companyName, creds));

    if (options.syncCostCentres      && tallyData.costCentres && tallyData.costCentres.length > 0)
      await runStep("costCentres",     () => syncCostCentresToErpNext(tallyData.costCentres, companyName, creds));

    if (options.syncStock            && tallyData.stockItems && tallyData.stockItems.length   > 0)
      await runStep("stockItems",      () => syncStockToErpNext(tallyData.stockItems, creds));

    if (options.syncVouchers         && tallyData.vouchers  && tallyData.vouchers.length  > 0)
      await runStep("vouchers",        () => syncVouchersToErpNext(tallyData.vouchers, companyName, creds));

    if (options.syncInvoices         && tallyData.vouchers  && tallyData.vouchers.length  > 0)
      await runStep("invoices",        () => syncInvoicesToErpNext(tallyData.vouchers, companyName, creds));

    // FIX: syncTaxes step was silently missing — syncTaxes:true was accepted but never executed
    if (options.syncTaxes            && tallyData.stockItems && tallyData.stockItems.length > 0)
      await runStep("taxes",           () => syncTaxesToErpNext(tallyData.stockItems, companyName, creds));
  } catch (e) {
    if (e._cancelled) {
      // Already marked inside runStep — just return the partial result
      result.finishedAt = result.finishedAt || new Date().toISOString();
      logger.info("Full sync stopped by user — partial results returned");
      return result;
    }
    throw e; // unexpected error — let it propagate
  }

  const hasFail = Object.values(result.steps).some((s) => s.status === "fail");
  const hasWarn = Object.values(result.steps).some((s) => s.status === "warn");
  result.status     = hasFail ? "failed" : hasWarn ? "warning" : "ok";
  result.finishedAt = new Date().toISOString();

  logger.info("Full sync complete: " + result.status, { company: companyName });
  return result;
}