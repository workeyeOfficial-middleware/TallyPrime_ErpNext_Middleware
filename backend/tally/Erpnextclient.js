// @ts-nocheck
/**
 * erpnextClient.js
 * Pushes data from Tally into ERPNext via REST API.
 * Place at: backend/tally/erpnextClient.js
 */

import axios from "axios";
import { createHash } from "crypto";
import { config } from "../config/config.js";
import { logger } from "../logs/logger.js";

const BATCH_DELAY_MS  = 300;   // 0.3s between each slot — adaptive throttle handles 429s
const BATCH_BURST     = 15;    // pause after every 15 requests
const BATCH_BURST_MS  = 2000;  // 2s pause after each burst
const RETRY_ATTEMPTS  = 5;
const RETRY_DELAY_MS  = 8000;  // 8s base retry delay
const CONCURRENCY     = 8;     // 8 parallel requests — throttle auto-backs-off on 429
const ADDR_CONCURRENCY = 5;    // lower concurrency for address/contact calls

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

// Shared ERPNext accounts list — fetched once per sync run by COA, reused by Opening Balances.
// Eliminates the duplicate "Fetched N accounts" API call every run.
// Keyed by companyName so multi-company setups stay isolated.
const _erpAccountsForRun = new Map(); // companyName → accounts[]

// Call this at the start of each full sync to avoid stale cache across runs
export function clearCaches() {
  _knownItemGroups.clear();
  _knownUoMs.clear();
  _knownHsn.clear();
  _accountCache.clear();
  _accountTypeMap.clear();
  _companyAbbrCache.clear();
  _companyNameCache.clear();
  _knownCustomFields.clear();
  _customerGroup = null;
  _supplierGroup = null;
  _erpAccountsForRun.clear();
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

// Cache of verified territories — avoids repeated API calls for the same state
const _verifiedTerritories = new Set(["India"]); // India always exists in ERPNext
const _invalidTerritories  = new Set();           // states confirmed missing in ERPNext

async function resolveTerritory(client, state) {
  // No state → default territory (India or whatever user set)
  if (!state || !state.trim()) return _territory || "India";

  const mapped = STATE_TERRITORY_MAP[state.trim().toLowerCase()] || state.trim();

  // Already confirmed valid → return immediately
  if (_verifiedTerritories.has(mapped)) return mapped;

  // Already confirmed invalid → fall back to default
  if (_invalidTerritories.has(mapped)) return _territory || "India";

  // First time seeing this territory — verify it exists in ERPNext
  try {
    await client.get("/api/resource/Territory/" + encodeURIComponent(mapped));
    _verifiedTerritories.add(mapped);
    return mapped;
  } catch (_) {
    // Territory doesn't exist in this ERPNext instance — use safe fallback
    _invalidTerritories.add(mapped);
    logger.info(
      `Territory "${mapped}" not found in ERPNext — using "${_territory || "India"}" as fallback. ` +
      `You can create it in ERPNext under Setup → Territory if needed.`
    );
    return _territory || "India";
  }
}

// Ensure an ERPNext Address record exists for a party
async function ensureAddress(client, partyName, partyType, ledger) {
  // Only skip if there is truly NOTHING to sync — no address, no state, no pincode, no phone, no email
  if (!ledger.address && !ledger.state && !ledger.pincode && !ledger.phone && !ledger.email) return;

  // Map Tally state → ERPNext accepted Indian state name
  const INDIAN_STATES = {
    'andhra pradesh':'Andhra Pradesh','arunachal pradesh':'Arunachal Pradesh','assam':'Assam',
    'bihar':'Bihar','chhattisgarh':'Chhattisgarh','goa':'Goa','gujarat':'Gujarat',
    'haryana':'Haryana','himachal pradesh':'Himachal Pradesh','jharkhand':'Jharkhand',
    'karnataka':'Karnataka','kerala':'Kerala','madhya pradesh':'Madhya Pradesh',
    'maharashtra':'Maharashtra','manipur':'Manipur','meghalaya':'Meghalaya','mizoram':'Mizoram',
    'nagaland':'Nagaland','odisha':'Odisha','orissa':'Odisha','punjab':'Punjab',
    'rajasthan':'Rajasthan','sikkim':'Sikkim','tamil nadu':'Tamil Nadu','telangana':'Telangana',
    'tripura':'Tripura','uttar pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand',
    'uttaranchal':'Uttarakhand','west bengal':'West Bengal',
    'andaman and nicobar islands':'Andaman and Nicobar Islands',
    'andaman & nicobar islands':'Andaman and Nicobar Islands','chandigarh':'Chandigarh',
    'dadra and nagar haveli':'Dadra and Nagar Haveli','dadra & nagar haveli':'Dadra and Nagar Haveli',
    'daman and diu':'Daman and Diu','daman & diu':'Daman and Diu',
    'delhi':'Delhi','new delhi':'Delhi','jammu and kashmir':'Jammu and Kashmir',
    'jammu & kashmir':'Jammu and Kashmir','ladakh':'Ladakh','lakshadweep':'Lakshadweep',
    'puducherry':'Puducherry','pondicherry':'Puducherry',
  };
  // Pincode → state prefix map — used to detect pincode/state mismatch
  const PINCODE_STATE_PREFIX = {
    '11':'Delhi','12':'Haryana','13':'Haryana','14':'Punjab','15':'Punjab','16':'Punjab',
    '17':'Himachal Pradesh','18':'Jammu and Kashmir','19':'Jammu and Kashmir',
    '20':'Uttar Pradesh','21':'Uttar Pradesh','22':'Uttar Pradesh','23':'Uttar Pradesh',
    '24':'Uttar Pradesh','25':'Uttar Pradesh','26':'Uttar Pradesh','27':'Uttar Pradesh',
    '28':'Uttar Pradesh','30':'Rajasthan','31':'Rajasthan','32':'Rajasthan','33':'Rajasthan',
    '34':'Rajasthan','36':'Gujarat','37':'Gujarat','38':'Gujarat','39':'Gujarat',
    '40':'Maharashtra','41':'Maharashtra','42':'Maharashtra','43':'Maharashtra','44':'Maharashtra',
    '45':'Madhya Pradesh','46':'Madhya Pradesh','47':'Madhya Pradesh','48':'Madhya Pradesh',
    '49':'Chhattisgarh','50':'Telangana','51':'Telangana','52':'Andhra Pradesh',
    '53':'Andhra Pradesh','56':'Karnataka','57':'Karnataka','58':'Karnataka','59':'Karnataka',
    '60':'Tamil Nadu','61':'Tamil Nadu','62':'Tamil Nadu','63':'Tamil Nadu','64':'Tamil Nadu',
    '67':'Kerala','68':'Kerala','69':'Kerala','70':'West Bengal','71':'West Bengal',
    '72':'West Bengal','73':'West Bengal','74':'West Bengal','75':'Odisha','76':'Odisha',
    '77':'Odisha','78':'Assam','79':'Assam','80':'Bihar','81':'Bihar','82':'Bihar',
    '83':'Bihar','84':'Bihar','85':'Bihar','82':'Jharkhand','83':'Jharkhand',
    '90':'Rajasthan','91':'Rajasthan','92':'Rajasthan','93':'Rajasthan',
  };

  const rawState   = ledger.state && ledger.state.trim() ? ledger.state.trim() : '';
  const stateVal   = rawState ? (INDIAN_STATES[rawState.toLowerCase()] || null) : null;

  // Validate pincode — must be 6 digits, not starting with 0
  const rawPin     = ledger.pincode ? String(ledger.pincode).trim().replace(/\D/g, '') : '';
  const validPin   = /^[1-9]\d{5}$/.test(rawPin);

  // If we have both a valid pincode and a state, check they match
  // If they don't match → drop pincode to avoid ERPNext GST validation error
  let pincodeVal = '';
  if (validPin) {
    const prefix       = rawPin.slice(0, 2);
    const expectedState = PINCODE_STATE_PREFIX[prefix];
    if (!stateVal || !expectedState || expectedState === stateVal) {
      pincodeVal = rawPin;  // match or unknown — keep pincode
    }
    // mismatch — drop pincode, keep state (state is more reliable than pincode)
  }

  const addressName = partyName + '-' + partyType;
  // Always use country=India — ERPNext doesn't have "Other" as a standard country.
  // When state is unknown, simply omit the state field — ERPNext allows this for non-GST addresses.
  const doc = {
    doctype:        'Address',
    address_title:  partyName,
    address_type:   'Billing',
    address_line1:  ledger.address || partyName,
    city:           rawState || 'India',
    country:        'India',
    pincode:        pincodeVal,
    links: [{ link_doctype: partyType, link_name: partyName }],
  };
  if (stateVal) doc.state = stateVal;
  if (ledger.email) doc.email_id = ledger.email;
  if (ledger.phone) doc.phone    = ledger.phone;

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

// Simple deterministic hash — djb2 on a stable JSON string.
// Used to detect "nothing changed" so we skip re-PUT on already-synced docs.
function _docHash(doc) {
  const stable = JSON.stringify(doc, Object.keys(doc).sort());
  let h = 5381;
  for (let i = 0; i < stable.length; i++) {
    h = ((h << 5) + h) ^ stable.charCodeAt(i);
    h >>>= 0; // keep 32-bit unsigned
  }
  return h.toString(16);
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
      if (existing.docstatus === 1) {
        // ── SUBMITTED DOC: patch only the Tally custom fields via set_value ──
        // ERPNext won’t accept a full PUT on a submitted document, but individual
        // read_only custom fields (tally_sales_ledger, party_balance, etc.) can be
        // patched directly with frappe.client.set_value without cancel/amend.
        // We only attempt this for Sales Invoice / Purchase Invoice doctypes where
        // the Tally custom fields are defined — other doctypes just skip silently.
        const tallyPatchFields = [
          "custom_tally_sales_ledger",
          "custom_tally_party_balance",
          "custom_tally_sales_ledger_balance",
          "custom_tally_voucher_no",
          "custom_tally_id",
        ];
        const patchValues = {};
        for (const f of tallyPatchFields) {
          if (doc[f] !== undefined && doc[f] !== null) {
            patchValues[f] = doc[f];
          }
        }
        if (Object.keys(patchValues).length > 0) {
          try {
            await client.post("/api/method/frappe.client.set_value", {
              doctype,
              name:     existing.name,
              fieldname: patchValues,
            });
          } catch (patchErr) {
            // set_value with a fieldname object failed on some ERPNext versions.
            // Fall back to patching each field individually, one at a time.
            for (const [fieldname, value] of Object.entries(patchValues)) {
              try {
                await client.post("/api/method/frappe.client.set_value", {
                  doctype,
                  name:     existing.name,
                  fieldname,
                  value,
                });
              } catch (singleErr) {
                logger.warn("[set_value] Could not patch " + fieldname + " on " + existing.name + ": " + (singleErr?.response?.data?.message || singleErr?.message || singleErr));
              }
            }
          }
        }
        return { action: "updated", name: existing.name };
      }

      // ── DIRTY CHECK: skip PUT if doc content is unchanged ────────────────────
      // For Journal Entry (and other non-party doctypes), we store a lightweight
      // content hash in user_remark as a suffix: " |h:<hex>".
      // On subsequent runs we recompute the hash and compare — if identical we
      // return "skipped" so the sync counter reflects reality (not inflated
      // "updated" counts) and we save one API call per unchanged voucher.
      if (doctype === "Journal Entry") {
        const newHash = _docHash(doc);
        // Check if existing user_remark already ends with this hash
        try {
          const remarkRes = await client.get(
            "/api/resource/" + encodeURIComponent(doctype) + "/" + encodeURIComponent(existing.name),
            { params: { fields: '["user_remark"]' } }
          );
          const existingRemark = remarkRes?.data?.data?.user_remark || "";
          const hashSuffix = " |h:" + newHash;
          if (existingRemark.includes(hashSuffix)) {
            // Content is identical — skip the PUT entirely
            return { action: "skipped", name: existing.name };
          }
          // Hash mismatch → update, and stamp the new hash into user_remark
          doc = Object.assign({}, doc, { user_remark: (doc.user_remark || "") + hashSuffix });
        } catch (_) { /* hash check failed — fall through to normal PUT */ }
      }

      // Always include doctype + name in PUT body — ERPNext requires them to validate mandatory fields
      await client.put(
        "/api/resource/" + encodeURIComponent(doctype) + "/" + encodeURIComponent(existing.name),
        Object.assign({}, doc, { doctype, name: existing.name })
      );
      return { action: "updated", name: existing.name };
    }
  } catch (_) {}
  // On first create, stamp the content hash into user_remark for Journal Entries
  if (doctype === "Journal Entry" && doc.user_remark !== undefined) {
    const newHash = _docHash(doc);
    doc = Object.assign({}, doc, { user_remark: (doc.user_remark || "") + " |h:" + newHash });
  }
  const res = await client.post("/api/resource/" + encodeURIComponent(doctype), Object.assign({}, doc, { doctype }));
  return { action: "created", name: res.data && res.data.data && res.data.data.name };
}

async function batchSync(client, doctype, items, mapper, progressCb) {
  let created = 0, updated = 0, failed = 0, skipped = 0;
  const errors = [];
  const failedItems = []; // transient failures queued for final pass

  for (let i = 0; i < items.length; i += CONCURRENCY) {
    checkCancelled(doctype); // ← stop immediately if user clicked Stop
    const chunk = items.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(
      chunk.map(async (item) => {
        const { filters, doc } = await mapper(item);
        return withRetry(() => upsert(client, doctype, filters, doc), doctype + ":" + item.name);
      })
    );
    for (let j = 0; j < results.length; j++) {
      const item = chunk[j];
      const r    = results[j];
      if (r.status === "fulfilled") {
        if      (r.value.action === "created") created++;
        else if (r.value.action === "skipped") skipped++;
        else                                   updated++;
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
      const { filters, doc } = await mapper(item);
      try {
        const result = await withRetry(() => upsert(client, doctype, filters, doc), doctype + ":" + item.name + " [final]");
        if      (result.action === "created") created++;
        else if (result.action === "skipped") skipped++;
        else                                  updated++;
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

  return { created, updated, skipped, failed, errors: errors.slice(0, 20) };
}

// -- Ledgers -> Customers / Suppliers -----------------------------------------
// ── Auto-create a custom field on an ERPNext doctype if it doesn't exist ──────
// Called when Tally sends an extra field we haven't explicitly mapped.
// Field name format: custom_tally_<tally_key_lowercase>
// Safe to call multiple times — skips silently if the field already exists.
const _knownCustomFields = new Set(); // avoid repeated API checks per sync run

async function ensureCustomFieldOnDoctype(client, doctype, fieldname, label) {
  const cacheKey = doctype + "::" + fieldname;
  if (_knownCustomFields.has(cacheKey)) return;
  try {
    const existing = await client.get("/api/resource/Custom Field", {
      params: {
        filters: JSON.stringify([["Custom Field","dt","=",doctype],["Custom Field","fieldname","=",fieldname]]),
        fields: '["name"]', limit: 1,
      },
    });
    if ((existing?.data?.data || []).length > 0) {
      _knownCustomFields.add(cacheKey);
      return;
    }
    await client.post("/api/resource/Custom Field", {
      doctype:   "Custom Field",
      dt:        doctype,
      fieldname,
      label,
      fieldtype: "Data",
      read_only: 1,
    });
    _knownCustomFields.add(cacheKey);
    logger.human
      ? logger.human.headsUp(`A new field "${label}" was found in Tally and has been created in ERPNext under ${doctype}.`)
      : logger.info(`Auto-created custom field: ${doctype}.${fieldname}`);
  } catch (_) {
    _knownCustomFields.add(cacheKey); // don't retry on this run even if it failed
  }
}

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

  const customerMapper = async (l) => {
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
      territory:           await resolveTerritory(client, l.state),
      default_currency:    "INR",
      custom_tally_id:     l.guid   || l.masterID || "",   // FIX: Tally GUID
      custom_tally_group:  l.parentGroup || "",            // FIX: Tally parent group
      custom_source:       "Tally",                        // FIX: sync origin marker
    };
    // Statutory
    if (l.gstin)  doc.tax_id          = l.gstin.trim();
    // Sync PAN as-is from Tally — no format restriction
    if (l.pan && l.pan.trim()) doc.pan = l.pan.trim().toUpperCase();
    // Contact info (primary)
    if (l.email)  doc.email_id        = l.email.trim();
    if (l.phone)  doc.mobile_no       = l.phone.trim();
    // Bank
    if (l.bankAccount) doc.bank_account_no = l.bankAccount.trim();
    if (l.ifsc)        doc.bank_ifsc_code  = l.ifsc.trim();

    // ── Extra fields from Tally (custom fields passthrough) ──────────────────
    // If Tally sent fields we don't explicitly map (e.g. CREDITLIMIT, TRANSPORTERNAME),
    // they arrive in l.customFields. We write them as custom_tally_<fieldname> on the
    // ERPNext Customer so no data is silently lost. The ensureCustomFieldOnDoctype()
    // call in syncLedgersToErpNext ensures the Custom Field exists before we write it.
    if (l.customFields) {
      for (const [key, value] of Object.entries(l.customFields)) {
        const erpField = "custom_tally_" + key.toLowerCase().replace(/[^a-z0-9]/g, "_");
        doc[erpField] = String(value).slice(0, 255);
      }
    }

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
    // Sync PAN as-is from Tally — no format restriction
    if (l.pan && l.pan.trim()) doc.pan = l.pan.trim().toUpperCase();
    // Contact info (primary)
    if (l.email)  doc.email_id        = l.email.trim();
    if (l.phone)  doc.mobile_no       = l.phone.trim();
    // Bank
    if (l.bankAccount) doc.bank_account_no = l.bankAccount.trim();
    if (l.ifsc)        doc.bank_ifsc_code  = l.ifsc.trim();

    // ── Extra fields from Tally (custom fields passthrough) ──────────────────
    if (l.customFields) {
      for (const [key, value] of Object.entries(l.customFields)) {
        const erpField = "custom_tally_" + key.toLowerCase().replace(/[^a-z0-9]/g, "_");
        doc[erpField] = String(value).slice(0, 255);
      }
    }

    return { filters: { supplier_name: l.name }, doc, _ledger: l };
  };

  // ── Auto-create ERPNext custom fields for any extra Tally fields ────────────
  // Collect every unique customField key across all ledgers, then ensure the
  // corresponding custom_tally_* field exists on Customer and Supplier before
  // batchSync tries to write to it.
  const allCustomKeys = new Set();
  for (const l of [...customers, ...suppliers]) {
    if (l.customFields) Object.keys(l.customFields).forEach((k) => allCustomKeys.add(k));
  }
  for (const key of allCustomKeys) {
    const fieldname = "custom_tally_" + key.toLowerCase().replace(/[^a-z0-9]/g, "_");
    const label     = "Tally " + key.charAt(0) + key.slice(1).toLowerCase().replace(/_/g, " ");
    await ensureCustomFieldOnDoctype(client, "Customer", fieldname, label);
    await ensureCustomFieldOnDoctype(client, "Supplier", fieldname, label);
    await sleep(200);
  }

  const customerResults = await batchSync(client, "Customer", customers, customerMapper);
  const supplierResults = await batchSync(client, "Supplier", suppliers, supplierMapper);

  // Build a set of party names that failed so we don't attempt address sync for them.
  // Trying to link an Address to a non-existent Customer/Supplier causes
  // "Could not find Row #1: Link Name: <party>" errors.
  const failedPartyNames = new Set([
    ...customerResults.errors.map((e) => String(e.item || "")),
    ...supplierResults.errors.map((e) => String(e.item || "")),
  ]);

  // Sync addresses and contacts for all parties that successfully synced to ERPNext.
  // FIX: We no longer skip parties with no address data — ensureAddress now creates
  // a minimal record with whatever data is available (even just the party name).
  // This ensures every customer/supplier has an Address record in ERPNext.
  const partiesWithAddr = [
    ...customers.map((l) => ({ l, type: "Customer" })),
    ...suppliers.map((l) => ({ l, type: "Supplier" })),
  ].filter(({ l }) =>
    !failedPartyNames.has(l.name)   // only skip parties that failed to create
  );

  if (partiesWithAddr.length > 0) {
    logger.info("Syncing addresses/contacts for " + partiesWithAddr.length + " parties from Tally");
    for (let i = 0; i < partiesWithAddr.length; i += ADDR_CONCURRENCY) {
      const slice = partiesWithAddr.slice(i, i + ADDR_CONCURRENCY);
      await Promise.allSettled(slice.map(async ({ l, type }) => {
        await ensureAddress(client, l.name, type, l);
        await ensureContact(client, l.name, type, l);
      }));
      await sleep(500); // small pause between address batches
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

// -- Ensure Tally custom fields exist on Sales Invoice -------------------------
// Creates `custom_tally_voucher_no` on Sales Invoice and Payment Entry if they don't exist yet.
// This gives us a dedicated, always-visible column that stores the Tally number.
async function ensureTallyCustomFields(client) {
  const fieldsToCreate = [
    {
      dt:         "Sales Invoice",
      fieldname:  "custom_tally_voucher_no",
      label:      "Tally Voucher No",
      fieldtype:  "Data",
      insert_after: "po_no",
      in_list_view: 1,       // show in the Sales Invoice list — this is the key flag
      in_standard_filter: 1, // also filterable
      read_only: 1,
      allow_on_submit: 1,    // allow set_value on submitted docs
    },
    {
      // FIX: Add Tally Vch No column to Payment Entry list so users can cross-reference
      // Tally voucher numbers directly from the ERPNext Payments list view.
      dt:         "Payment Entry",
      fieldname:  "custom_tally_voucher_no",
      label:      "Tally Vch No",
      fieldtype:  "Data",
      insert_after: "reference_no",
      in_list_view: 1,       // visible in the Payment Entry list
      in_standard_filter: 1, // filterable by Tally voucher number
      read_only: 1,
      allow_on_submit: 1,
    },
    // ── NEW: Tally Sales Ledger name (e.g. "Sales", "Sales - Retail") ──────────
    // Tally shows "Sales ledger: Sales" at the top of every sales voucher.
    // This field maps that ledger name into ERPNext so no data is lost.
    // in_list_view: 1 so it appears as a column in the Sales Invoice list view.
    {
      dt:           "Sales Invoice",
      fieldname:    "custom_tally_sales_ledger",
      label:        "Tally Sales Ledger",
      fieldtype:    "Data",
      insert_after: "custom_tally_voucher_no",
      in_list_view: 1,
      in_standard_filter: 1,
      read_only: 1,
      allow_on_submit: 1,
    },
    // ── NEW: Party (Customer) current balance from Tally at time of sync ───────
    // Tally shows "Current balance: 25,68,68,636.00 Dr" next to the party name.
    // Stored as a plain string to preserve Tally's Dr/Cr suffix.
    {
      dt:           "Sales Invoice",
      fieldname:    "custom_tally_party_balance",
      label:        "Tally Party Balance",
      fieldtype:    "Data",
      insert_after: "custom_tally_sales_ledger",
      in_list_view: 0,
      in_standard_filter: 0,
      read_only: 1,
      allow_on_submit: 1,
    },
    // ── NEW: Sales Ledger current balance from Tally at time of sync ──────────
    // Tally shows "Current balance: 5,59,40,29,876.00 Cr" next to the sales ledger.
    // Stored as a plain string to preserve Tally's Dr/Cr suffix.
    {
      dt:           "Sales Invoice",
      fieldname:    "custom_tally_sales_ledger_balance",
      label:        "Tally Sales Ledger Balance",
      fieldtype:    "Data",
      insert_after: "custom_tally_party_balance",
      in_list_view: 0,
      in_standard_filter: 0,
      read_only: 1,
      allow_on_submit: 1,
    },
  ];

  for (const f of fieldsToCreate) {
    try {
      // Check if custom field already exists
      const existing = await client.get("/api/resource/Custom Field", {
        params: {
          filters: JSON.stringify([["Custom Field","dt","=",f.dt],["Custom Field","fieldname","=",f.fieldname]]),
          fields: '["name","in_list_view","in_standard_filter","allow_on_submit"]',
          limit: 1,
        },
      });
      const existingField = (existing?.data?.data || [])[0];
      if (existingField) {
        // Field exists — update in_list_view/in_standard_filter if they changed
        const needsUpdate =
          (existingField.in_list_view      || 0) !== (f.in_list_view      || 0) ||
          (existingField.in_standard_filter || 0) !== (f.in_standard_filter || 0) ||
          (existingField.allow_on_submit    || 0) !== (f.allow_on_submit    || 0);
        if (needsUpdate) {
          await client.put("/api/resource/Custom Field/" + encodeURIComponent(existingField.name), {
            in_list_view:       f.in_list_view,
            in_standard_filter: f.in_standard_filter,
            allow_on_submit:    f.allow_on_submit || 0,
          });
          logger.info("Updated custom field flags: " + f.dt + "." + f.fieldname);
        }
        continue;
      }

      await client.post("/api/resource/Custom Field", {
        doctype:            "Custom Field",
        dt:                 f.dt,
        fieldname:          f.fieldname,
        label:              f.label,
        fieldtype:          f.fieldtype,
        insert_after:       f.insert_after,
        in_list_view:       f.in_list_view,
        in_standard_filter: f.in_standard_filter,
        read_only:          f.read_only,
        allow_on_submit:    f.allow_on_submit || 0,
      });
      logger.info("Created custom field: " + f.dt + "." + f.fieldname);
    } catch (e) {
      logger.warn("Could not create custom field " + f.fieldname + ": " + (e?.message || e));
    }
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

  // ── Auto-create ERPNext custom fields for any extra Tally stock fields ───────
  const allStockCustomKeys = new Set();
  for (const item of stockItems) {
    if (item.customFields) Object.keys(item.customFields).forEach((k) => allStockCustomKeys.add(k));
  }
  for (const key of allStockCustomKeys) {
    const fieldname = "custom_tally_" + key.toLowerCase().replace(/[^a-z0-9]/g, "_");
    const label     = "Tally " + key.charAt(0) + key.slice(1).toLowerCase().replace(/_/g, " ");
    await ensureCustomFieldOnDoctype(client, "Item", fieldname, label);
    await sleep(200);
  }

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

    // ── Extra fields from Tally (custom fields passthrough) ──────────────────
    if (item.customFields) {
      for (const [key, value] of Object.entries(item.customFields)) {
        const erpField = "custom_tally_" + key.toLowerCase().replace(/[^a-z0-9]/g, "_");
        doc[erpField] = String(value).slice(0, 255);
      }
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

// ── Build a custom-voucher-type resolver from Tally VoucherType masters ────────
// voucherTypes = array from fetchTallyVoucherTypes()
// Returns a function: resolveBaseType(voucherTypeName) → standard base type string
// e.g. "Tax Invoice Mumbai" → "Sales", "Tally Annual Contract" → "Sales"
// Falls back to the name itself if not found (so standard types still work).
export function buildVoucherTypeResolver(voucherTypes = []) {
  // Build map: lowercased name → baseType (BASEVOUCHERTYPE or PARENT)
  const map = new Map();
  for (const vt of voucherTypes) {
    if (vt.name) {
      // baseType is the standard Tally type this custom type is based on
      const base = (vt.baseType || vt.parent || vt.name).trim();
      map.set(vt.name.trim().toLowerCase(), base);
    }
  }
  return function resolveBaseType(name) {
    if (!name) return "";
    const base = map.get(name.trim().toLowerCase());
    return base || name; // if not found, return as-is (handles standard types)
  };
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

export async function syncVouchersToErpNext(vouchers, companyName, creds = {}, voucherTypeResolver = null) {
  const client = createErpClient(creds);
  companyName = await resolveErpNextCompany(client, companyName, creds);
  logger.info("Syncing " + vouchers.length + " vouchers to ERPNext");
  const companyAbbr = companyName ? await getCompanyAbbr(client, companyName) : "T";
  logger.info("Resolving accounts with company abbreviation: " + companyAbbr);

  // Ensure custom_tally_voucher_no field exists on Payment Entry so Vch No is visible in list view
  await ensureTallyCustomFields(client);

  // ── Resolve custom voucher types to their standard base type ─────────────────
  // e.g. "Tax Invoice Mumbai" → "Sales", "Tally Annual Contract" → "Sales"
  // resolveBase(name) returns the standard Tally base type for any custom type.
  const resolveBase = voucherTypeResolver || ((name) => name);

  // ── Split vouchers by destination in ERPNext ─────────────────────────────────
  // Payment / Receipt  → Payment Entry  (dedicated module, shows in Payments list)
  // Journal / Contra / Debit Note / Credit Note → Journal Entry
  // Sales / Purchase / Stock Journal / Opening Balance → handled elsewhere / skipped
  const PAYMENT_VCH_TYPES = new Set(["Payment", "Receipt"]);
  const JE_VCH_TYPES      = new Set(["Journal", "Contra", "Debit Note", "Credit Note"]);
  const SKIP_VCH_TYPES    = new Set(["Stock Journal", "Opening Balance", "Delivery Note", "Purchase Order", "Sales Order"]);

  const paymentVouchers  = vouchers.filter((v) => PAYMENT_VCH_TYPES.has(resolveBase(v.voucherType)));
  // let (not const) — fallback JEs from Payment Entry are pushed in later
  let   journalVouchers  = vouchers.filter((v) => JE_VCH_TYPES.has(resolveBase(v.voucherType)));
  const salesVouchers    = vouchers.filter((v) => resolveBase(v.voucherType) === "Sales");
  const skippedTypes     = vouchers.filter((v) => SKIP_VCH_TYPES.has(resolveBase(v.voucherType)));
  const unmatched        = vouchers.length - paymentVouchers.length - journalVouchers.length - salesVouchers.length - skippedTypes.length;
  if (skippedTypes.length > 0) logger.info("Skipping " + skippedTypes.length + " vouchers with non-syncable types (Stock Journal, Opening Balance, etc.)");
  if (unmatched > 0) logger.warn("WARNING: " + unmatched + " vouchers have unresolved types — they will be synced as Journal Entries. Check buildVoucherTypeResolver.");
  logger.info("Voucher split — Payment Entry: " + paymentVouchers.length + ", Journal Entry: " + journalVouchers.length + ", Sales: " + salesVouchers.length + ", Skipped: " + skippedTypes.length);

  // ── Sync Payment / Receipt vouchers → ERPNext Payment Entry ─────────────────
  // ERPNext Payment Entry fields:
  //   payment_type: "Receive" (customer pays you) | "Pay" (you pay supplier)
  //   paid_from / paid_to: derived from Receivable/Payable vs bank/cash account rows
  // Idempotency key: remarks = "Tally:Payment:<voucherNumber>"
  const peResults = { created: 0, updated: 0, failed: 0 };
  if (paymentVouchers.length > 0) {
    logger.info("Syncing " + paymentVouchers.length + " Payment/Receipt vouchers to Payment Entry");

    // Resolve accounts for all payment vouchers
    for (const v of paymentVouchers) {
      if (!v.entries || v.entries.length === 0) { v._skip = true; continue; }
      const resolved = [];
      for (const e of v.entries) {
        const { name: acct, accountType } = await resolveAccountWithType(client, e.ledger, companyAbbr, companyName);
        resolved.push({ ...e, erpAccount: acct, accountType });
      }
      v.resolvedAccounts = resolved;
    }

    // Pre-create missing parties for payment vouchers.
    // Strategy: use account_type when available; fall back to voucherType inference
    // (Receipt = customer side, Payment = supplier side) when account_type is blank.
    const _peParties = { Customer: new Set(), Supplier: new Set() };
    for (const v of paymentVouchers) {
      const rows = v.resolvedAccounts || [];
      for (const row of rows) {
        if (row.accountType === "Receivable" && row.ledger) {
          _peParties.Customer.add(row.ledger.trim());
        } else if (row.accountType === "Payable" && row.ledger) {
          _peParties.Supplier.add(row.ledger.trim());
        } else if (!row.accountType && row.ledger) {
          // Account type is blank — infer from voucherType + debit/credit direction
          if (v.voucherType === "Receipt" && !row.isDebit) {
            _peParties.Customer.add(row.ledger.trim()); // credited in Receipt = customer
          } else if (v.voucherType === "Payment" && row.isDebit) {
            _peParties.Supplier.add(row.ledger.trim()); // debited in Payment = supplier
          }
        }
      }
    }
    // Remove bank/cash account names from party sets — they are accounts, not parties
    // (their names often match account names like "Cash", "HDFC Bank" etc.)
    const bankCashNames = new Set(
      paymentVouchers
        .flatMap((v) => (v.resolvedAccounts || []))
        .filter((r) => r.accountType === "Bank" || r.accountType === "Cash")
        .map((r) => (r.ledger || "").trim().toLowerCase())
    );
    for (const n of [..._peParties.Customer]) if (bankCashNames.has(n.toLowerCase())) _peParties.Customer.delete(n);
    for (const n of [..._peParties.Supplier]) if (bankCashNames.has(n.toLowerCase())) _peParties.Supplier.delete(n);

    logger.info("[PE pre-sync] Ensuring parties — Customers: " + _peParties.Customer.size + ", Suppliers: " + _peParties.Supplier.size);
    for (const name of _peParties.Customer) {
      try { await client.get("/api/resource/Customer/" + encodeURIComponent(name)); }
      catch (_) {
        try { await client.post("/api/resource/Customer", { doctype: "Customer", customer_name: name, customer_type: "Individual", customer_group: _customerGroup || "Commercial", territory: "India" }); logger.info("[PE pre-sync] Auto-created Customer: " + name); }
        catch (e) { logger.warn("[PE pre-sync] Could not create Customer \"" + name + "\": " + parseErpError(e)); }
        await sleep(300);
      }
    }
    for (const name of _peParties.Supplier) {
      try { await client.get("/api/resource/Supplier/" + encodeURIComponent(name)); }
      catch (_) {
        try { await client.post("/api/resource/Supplier", { doctype: "Supplier", supplier_name: name, supplier_type: "Individual", supplier_group: _supplierGroup || "Services" }); logger.info("[PE pre-sync] Auto-created Supplier: " + name); }
        catch (e) { logger.warn("[PE pre-sync] Could not create Supplier \"" + name + "\": " + parseErpError(e)); }
        await sleep(300);
      }
    }

    // ── Resolve a default bank/cash account for this company ──────────────────
    // Used when the payment voucher has no explicit bank/cash row (e.g. simple
    // payment with only the party ledger entry). ERPNext requires a valid Bank
    // or Cash account for paid_from / paid_to — the company name itself is not valid.
    let _defaultBankAcct = null;
    let _defaultBankType = "Bank";
    try {
      const bankRes = await client.get("/api/resource/Account", {
        params: {
          filters: JSON.stringify([
            ["Account", "account_type", "in", ["Bank", "Cash"]],
            ["Account", "company",      "=",  companyName],
            ["Account", "is_group",     "=",  0],
          ]),
          fields: '["name","account_type"]',
          limit:  1,
          order_by: "account_type asc", // Bank before Cash
        },
      });
      const found = bankRes?.data?.data?.[0];
      if (found) {
        _defaultBankAcct = found.name;
        _defaultBankType = found.account_type || "Bank";
        logger.info("[PE] Default bank/cash account: " + _defaultBankAcct + " (" + _defaultBankType + ")");
      }
    } catch (_) {}

    if (!_defaultBankAcct) {
      // Hard fallback: construct the standard ERPNext account name
      _defaultBankAcct = "Cash - " + companyAbbr;
      _defaultBankType = "Cash";
      logger.warn("[PE] Could not resolve default bank account — using fallback: " + _defaultBankAcct);
    }

    const peMapper = (v) => {
      const rows = v.resolvedAccounts || [];

      // ── Identify party row and bank/cash row ──────────────────────────────
      // Primary strategy: find by account_type (Receivable = customer, Payable = supplier)
      // Fallback strategy: use voucherType to infer party side when account_type is blank
      //   Receipt = money received FROM customer → party side is the CREDIT row (isDebit=false)
      //   Payment = money paid TO supplier       → party side is the DEBIT row (isDebit=true)
      //
      // This handles the common case where customer/supplier accounts were auto-created
      // with account_type="" (blank) because ERPNext didn't classify them as Receivable/Payable.
      let partyRow = rows.find((r) => r.accountType === "Receivable" || r.accountType === "Payable");
      let bankRow  = rows.find((r) => r.accountType === "Bank" || r.accountType === "Cash");

      // Fallback: if no typed party row, infer from voucherType + debit/credit direction
      if (!partyRow && rows.length >= 2) {
        const isReceipt = v.voucherType === "Receipt";
        // Receipt: customer credited (isDebit=false on their ledger in Tally)
        // Payment: supplier debited  (isDebit=true on their ledger in Tally)
        // FIX: Exclude Bank/Cash/Income/Expense accounts from being picked as party —
        // these are gl accounts, not customers/suppliers. Vouchers like "Receipt from RAJLAXMI"
        // (a bank account) or "Payment to Sales 24" (an income account) should fall back
        // to Journal Entry rather than producing a broken Payment Entry with a non-party party.
        const nonBankRows = rows.filter((r) =>
          r.accountType !== "Bank" &&
          r.accountType !== "Cash" &&
          r.accountType !== "Income Account" &&
          r.accountType !== "Expense Account"
        );
        const candidateRows = nonBankRows.length > 0 ? nonBankRows : rows;
        partyRow = isReceipt
          ? candidateRows.find((r) => !r.isDebit)  // credited = customer paying us
          : candidateRows.find((r) =>  r.isDebit); // debited  = supplier we're paying
        if (partyRow) {
          // FIX: If the inferred row is still a Bank/Cash/Income account, it's a
          // contra/transfer voucher with no real party — fall back to JE.
          if (partyRow.accountType === "Bank" || partyRow.accountType === "Cash" ||
              partyRow.accountType === "Income Account" || partyRow.accountType === "Expense Account") {
            partyRow = null;
          } else {
            // Infer accountType from voucherType
            partyRow = { ...partyRow, accountType: isReceipt ? "Receivable" : "Payable" };
            logger.info("[PE] Inferred party row for voucher " + (v.voucherNumber || v.guid) + " from voucherType (" + v.voucherType + ")");
          }
        }
      }

      if (!partyRow) {
        // Log with full row details so the user can diagnose why party detection failed.
        // Common causes:
        //   - All rows are Bank/Cash/Income/Expense accounts (contra/transfer, no customer/supplier leg)
        //   - account_type is blank on all rows AND debit/credit direction inference also failed
        const rowSummary = rows.map((r) =>
          (r.ledger || "?") + "[" + (r.accountType || "no-type") + "," + (r.isDebit ? "Dr" : "Cr") + "]"
        ).join(" | ");
        logger.warn(
          "Payment voucher " + (v.voucherNumber || v.guid) +
          " (" + v.voucherType + ") falling back to Journal Entry — no party row detected." +
          " Rows: " + rowSummary
        );
        return null;
      }

      // Bank/cash row: prefer typed account; fall back to the non-party row
      if (!bankRow) {
        bankRow = rows.find((r) => r !== partyRow);
      }

      const isReceipt   = v.voucherType === "Receipt";
      const partyType   = partyRow.accountType === "Receivable" ? "Customer" : "Supplier";
      const paymentType = isReceipt ? "Receive" : "Pay";
      const amount      = Math.abs(partyRow.amount) || v.netAmount || 0;
      const remarkKey   = "Tally:" + v.voucherType + ":" + (v.voucherNumber || v.guid);

      // Use the resolved bank account; fall back to the company default if still missing
      const bankAcct     = bankRow ? bankRow.erpAccount : _defaultBankAcct;
      const bankAcctType = bankRow
        ? (bankRow.accountType === "Cash" ? "Cash" : "Bank")
        : _defaultBankType;
      const partyAcct    = partyRow.erpAccount;
      const partyAcctType = partyRow.accountType === "Receivable" ? "Receivable" : "Payable";

      // ERPNext Payment Entry requires paid_from_account_type + paid_to_account_type
      // These must be exact ERPNext enum values: "Bank", "Cash", "Receivable", "Payable"
      const paidFromAcct     = isReceipt ? bankAcct     : partyAcct;
      const paidFromAcctType = isReceipt ? bankAcctType : partyAcctType;
      const paidToAcct       = isReceipt ? partyAcct    : bankAcct;
      const paidToAcctType   = isReceipt ? partyAcctType : bankAcctType;

      return {
        filters: { remarks: remarkKey },
        doc: {
          doctype:                  "Payment Entry",
          payment_type:             paymentType,
          posting_date:             v.voucherDate,
          company:                  companyName,
          party_type:               partyType,
          party:                    (partyRow.ledger || "").trim(),
          paid_from:                paidFromAcct,
          paid_from_account_type:   paidFromAcctType,  // required by ERPNext validation
          paid_to:                  paidToAcct,
          paid_to_account_type:     paidToAcctType,    // required by ERPNext validation
          paid_amount:              amount,
          received_amount:          amount,
          source_exchange_rate:     1,
          target_exchange_rate:     1,
          reference_no:             v.voucherNumber || v.guid,
          reference_date:           v.voucherDate,
          remarks:                  remarkKey,
          custom_tally_id:          v.guid || v.voucherNumber,
          // FIX: Store Tally voucher number in the new custom field so it appears
          // as a dedicated "Tally Vch No" column in the ERPNext Payment Entry list view.
          custom_tally_voucher_no:  v.voucherNumber || v.guid,
        },
      };
    };

    const validPE    = [];
    const fallbackJE = [];
    for (const v of paymentVouchers) {
      if (v._skip || !v.resolvedAccounts) { peResults.failed++; continue; }
      const mapped = peMapper(v);
      if (mapped === null) fallbackJE.push(v);
      else validPE.push({ v, mapped });
    }

    for (const { v, mapped } of validPE) {
      try {
        const result = await upsert(client, "Payment Entry", mapped.filters, mapped.doc);
        if (result.action === "created") peResults.created++;
        else peResults.updated++;
      } catch (e) {
        peResults.failed++;
        logger.warn("Payment Entry failed for " + (v.voucherNumber || v.guid) + ": " + parseErpError(e));
      }
      await sleep(300);
    }

    if (fallbackJE.length > 0) {
      logger.info(fallbackJE.length + " payment vouchers without party rows falling back to Journal Entry");
      journalVouchers = [...journalVouchers, ...fallbackJE];
    }
    logger.info("Payment Entry sync done — created: " + peResults.created + ", updated: " + peResults.updated + ", failed: " + peResults.failed);
  }

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

  logger.success(
    "Voucher sync done — " +
    "Payment Entry: +" + peResults.created + "/~" + peResults.updated + "/=" + (peResults.skipped||0) + "/x" + peResults.failed + " | " +
    "Journal Entry: +" + jeResults.created + "/~" + jeResults.updated + "/=" + (jeResults.skipped||0) + "/x" + jeResults.failed
  );
  return { paymentEntries: peResults, journalEntries: jeResults, salesInvoices: { created: 0, updated: 0, failed: salesVouchers.length } };
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
    _erpAccountsForRun.set(companyName, erpAccounts); // cache for Opening Balances reuse
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
// Uses ERPNext's native Account-level opening balance fields
// (opening_balance_debit / opening_balance_credit) — exactly what the
// Accounts > Opening Balance tool does in the ERPNext UI.
// This keeps opening balances OUT of the Journal Entry list entirely.
// For party ledgers (Customers/Suppliers) we also post a standard
// "Opening Entry" Journal Entry because ERPNext requires party+account
// rows to track receivable/payable balances correctly from day one.

export async function syncOpeningBalancesToErpNext(ledgers, companyName, creds = {}) {
  const client = createErpClient(creds);
  companyName  = await resolveErpNextCompany(client, companyName, creds);
  logger.info("Syncing opening balances for " + ledgers.length + " ledgers in " + companyName);

  // ── One-time cleanup: delete ALL cancelled + old-format OB Journal Entries ──
  // Previous sync runs left behind many Cancelled JEs (from the old
  // cancel→delete→recreate pattern) and old-format "Tally OB - Debtors/Creditors/GL"
  // JEs. We delete them all once so the Journal Entry list stays clean.
  // Cancelled JEs have no accounting effect but clutter the list.
  try {
    let page = 0;
    const PAGE = 100;
    let totalDeleted = 0;
    while (true) {
      const res = await client.get("/api/resource/Journal Entry", {
        params: {
          filters: JSON.stringify([
            ["Journal Entry", "company",      "=",    companyName],
            ["Journal Entry", "user_remark",  "like", "Tally Opening Balance%"],
          ]),
          fields:  '["name","docstatus","title"]',
          limit:   PAGE,
          limit_start: page * PAGE,
        },
      });
      const rows = (res.data && res.data.data) || [];
      if (rows.length === 0) break;

      for (const row of rows) {
        // Delete: (a) any Cancelled JE, OR (b) old-format non-Parties JEs that
        // use the old Debtors/Creditors/GL split titles (replaced by single Parties JE)
        const isOldFormat = row.title && (
          row.title.includes("Tally OB - Debtors") ||
          row.title.includes("Tally OB - Creditors") ||
          row.title.includes("Tally OB - GL")
        );
        const isCancelled = row.docstatus === 2;

        if (isCancelled || isOldFormat) {
          try {
            // Cancelled docs can be deleted directly; submitted ones need cancel first
            if (row.docstatus === 1) {
              const cRes = await client.get("/api/resource/Journal Entry/" + encodeURIComponent(row.name));
              const cDoc = cRes?.data?.data;
              if (cDoc) await client.post("/api/method/frappe.client.cancel", { doc: cDoc });
            }
            await client.delete("/api/resource/Journal Entry/" + encodeURIComponent(row.name));
            totalDeleted++;
          } catch (_) {} // non-fatal — skip if delete fails
        }
      }
      if (rows.length < PAGE) break;
      page++;
    }
    if (totalDeleted > 0) logger.info("[OB] Cleaned up " + totalDeleted + " cancelled/old-format OB Journal Entries");
  } catch (_) {}

  const companyAbbr = await getCompanyAbbr(client, companyName);
  const withBalance = ledgers.filter((l) => l.openingBalance && Math.abs(l.openingBalance) > 0);

  if (withBalance.length === 0) {
    logger.info("No ledgers with non-zero opening balances found");
    return { created: 0, updated: 0, failed: 0, skipped: ledgers.length };
  }
  logger.info("Found " + withBalance.length + " ledgers with opening balances: " +
    withBalance.map((l) => l.name + "=" + l.openingBalance).join(", "));

  // ── Fetch all ERPNext accounts (reuse COA cache if available) ───────────────
  // COA step already fetched the full account list this run — reuse it to save
  // an extra paginated API call (~500ms on Frappe Cloud with 100+ accounts).
  let erpAccounts = [];
  if (_erpAccountsForRun.has(companyName)) {
    erpAccounts = _erpAccountsForRun.get(companyName);
    logger.info("Fetched " + erpAccounts.length + " accounts from ERPNext for " + companyName + " (reused COA cache)");
  } else {
    try {
      let pg = 0;
      while (true) {
        const res = await client.get("/api/resource/Account", {
          params: {
            filters: JSON.stringify([["Account", "company", "=", companyName]]),
            fields: '["name","account_name","account_type","is_group","root_type","parent_account"]',
            limit: 500, limit_start: pg * 500,
          },
        });
        const rows = (res.data && res.data.data) || [];
        erpAccounts = erpAccounts.concat(rows);
        if (rows.length < 500) break; pg++;
      }
    } catch (e) { logger.error("Could not fetch ERPNext accounts: " + e.message); }
    logger.info("Fetched " + erpAccounts.length + " accounts from ERPNext for " + companyName);
    _erpAccountsForRun.set(companyName, erpAccounts);
  }

  const leafAccounts  = erpAccounts.filter((a) => !a.is_group);
  const groupAccounts = erpAccounts.filter((a) => a.is_group);
  const byAccountName = new Map();
  const byFullName    = new Map();
  const byGroupName   = new Map(); // group accounts — used for parent lookup when auto-creating GL accounts
  for (const a of leafAccounts) {
    if (a.account_name) byAccountName.set(a.account_name.trim().toLowerCase(), a);
    if (a.name)         byFullName.set(a.name.trim().toLowerCase(), a);
  }
  for (const a of groupAccounts) {
    if (a.account_name) byGroupName.set(a.account_name.trim().toLowerCase(), a);
    if (a.name)         byGroupName.set(a.name.trim().toLowerCase(), a);
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

  // ── Find CONTROL accounts (Receivable / Payable) ────────────────────────────
  // ERPNext may have multiple accounts typed "Receivable" or "Payable" — e.g.
  // a party like "ABC Traders - C" can accidentally be set to Payable type.
  // We must pick the CONTROL account (Debtors / Creditors), not a party account.
  // Priority: prefer an account whose name contains "debtor"/"creditor"/"receivable"/"payable"
  // as these are the standard ERPNext control accounts. Fall back to first match.
  function findControlAccount(type) {
    const PREFER_KEYWORDS = type === "Receivable"
      ? ["debtor", "receivable", "accounts receivable"]
      : ["creditor", "payable", "accounts payable"];
    const candidates = leafAccounts.filter((a) => a.account_type === type);
    // First try: find one whose name matches a control-account keyword
    const preferred = candidates.find((a) =>
      PREFER_KEYWORDS.some((k) => (a.name || "").toLowerCase().includes(k))
    );
    if (preferred) return preferred;
    // Second try: avoid party-named accounts (those that match a known customer/supplier name)
    const nonParty = candidates.find((a) => {
      const lower = (a.account_name || a.name || "").toLowerCase();
      return !_existingCustomers.has(lower) && !_existingSuppliers.has(lower);
    });
    return nonParty || candidates[0] || null;
  }

  // ── Pre-fetch Customers and Suppliers FIRST ───────────────────────────────
  // Must happen before findControlAccount() which references these sets.
  const _existingCustomers = new Set();
  const _existingSuppliers = new Set();
  try {
    let pg = 0;
    while (true) {
      const r = await client.get("/api/resource/Customer", { params: { fields: '["customer_name"]', limit: 500, limit_start: pg * 500 } });
      const rows = (r.data && r.data.data) || [];
      rows.forEach((c) => _existingCustomers.add((c.customer_name || "").trim().toLowerCase()));
      if (rows.length < 500) break; pg++;
    }
  } catch (_) {}
  try {
    let pg = 0;
    while (true) {
      const r = await client.get("/api/resource/Supplier", { params: { fields: '["supplier_name"]', limit: 500, limit_start: pg * 500 } });
      const rows = (r.data && r.data.data) || [];
      rows.forEach((s) => _existingSuppliers.add((s.supplier_name || "").trim().toLowerCase()));
      if (rows.length < 500) break; pg++;
    }
  } catch (_) {}
  logger.info("[OB] Known customers: " + _existingCustomers.size + ", suppliers: " + _existingSuppliers.size);

  const receivableAcct = findControlAccount("Receivable");
  const payableAcct    = findControlAccount("Payable");

  // ── Auto-fix: party accounts incorrectly typed as Receivable/Payable ─────────
  for (const a of leafAccounts) {
    const lowerName    = (a.account_name || "").trim().toLowerCase();
    const isPartyTyped = a.account_type === "Receivable" || a.account_type === "Payable";
    const isPartyAcct  = _existingCustomers.has(lowerName) || _existingSuppliers.has(lowerName);
    const isRealCtrl   = (a.account_type === "Receivable" && a === receivableAcct) ||
                         (a.account_type === "Payable"    && a === payableAcct);
    if (isPartyTyped && isPartyAcct && !isRealCtrl) {
      try {
        await client.put("/api/resource/Account/" + encodeURIComponent(a.name), { account_type: "" });
        logger.info("[OB] Fixed mistyped account_type on party account: " + a.name + " (" + a.account_type + " → blank)");
      } catch (_) {}
    }
  }

  logger.info("Control accounts - Receivable: " + (receivableAcct ? receivableAcct.name : "NOT FOUND") +
              ", Payable: " + (payableAcct ? payableAcct.name : "NOT FOUND"));

  // ── Build ALL JE rows — every Tally ledger in ONE single Opening Entry JE ────────────────
  // ALL opening balances go into ONE JE so they are visible in a single place:
  //   Journal Entry list → filter Entry Type = Opening Entry
  //   Accounting → General Ledger → filter Voucher Type = Opening Entry
  //
  // Tally sign: openingBalance < 0 → Debit (assets), > 0 → Credit (liabilities)
  // Party rows MUST include party_type + party for AR/AP reports to work.

  const allJERows  = [];
  let skippedCount = 0;

  for (const l of withBalance) {
    l.name = (l.name || "").trim();
    const bal       = l.openingBalance || 0;
    const debitAmt  = bal < 0 ? Math.abs(bal) : 0;
    const creditAmt = bal > 0 ? Math.abs(bal) : 0;
    const pg        = (l.parentGroup || "").toLowerCase();

    // FIX: classify by BOTH parentGroup AND whether the ledger exists in ERPNext
    // as a Customer/Supplier. This catches ledgers like "Test GST Customer" which
    // may not have a "Sundry Debtor" parentGroup but ARE synced as Customers.
    const pgIsCustomer = ["sundry debtor","debtor","receivable","accounts receivable"].some((k) => pg.includes(k));
    const pgIsSupplier = ["sundry creditor","creditor","payable","accounts payable"].some((k) => pg.includes(k));
    const erpIsCustomer = _existingCustomers.has(l.name.toLowerCase());
    const erpIsSupplier = _existingSuppliers.has(l.name.toLowerCase());

    const isCustomer = pgIsCustomer || erpIsCustomer;
    const isSupplier = !isCustomer && (pgIsSupplier || erpIsSupplier);

    if (isCustomer || isSupplier) {
      let controlAcct = isCustomer ? receivableAcct : payableAcct;
      // ── FIX: Auto-create missing Receivable/Payable control account ──────────
      // When a new company has no "Sundry Debtors" or "Sundry Creditors" account
      // the opening balance for every party would be silently skipped.
      // We auto-create the control account so zero parties are lost.
      if (!controlAcct) {
        const ctrlType = isCustomer ? "Receivable" : "Payable";
        const ctrlName = isCustomer ? "Sundry Debtors" : "Sundry Creditors";
        const ctrlFull = ctrlName + " - " + companyAbbr;
        // Find a suitable parent group (Assets for Receivable, Liabilities for Payable)
        const ctrlRootType = isCustomer ? "Asset" : "Liability";
        let ctrlParent = null;
        try {
          const pgRes = await client.get("/api/resource/Account", {
            params: {
              filters: JSON.stringify([
                ["Account","root_type","=", ctrlRootType],
                ["Account","is_group","=", 1],
                ["Account","company","=", companyName],
              ]),
              fields: '["name"]', limit: 1,
            },
          });
          ctrlParent = pgRes?.data?.data?.[0]?.name || null;
        } catch (_) {}
        if (!ctrlParent) ctrlParent = (isCustomer ? "Current Assets" : "Current Liabilities") + " - " + companyAbbr;
        try {
          await client.post("/api/resource/Account", {
            doctype:        "Account",
            account_name:   ctrlName,
            company:        companyName,
            is_group:       0,
            account_type:   ctrlType,
            root_type:      ctrlRootType,
            parent_account: ctrlParent,
          });
          logger.info("[OB] Auto-created missing control account: " + ctrlFull + " (type: " + ctrlType + ")");
          const newCtrlObj = { name: ctrlFull, account_name: ctrlName, is_group: 0, account_type: ctrlType };
          if (isCustomer) {
            // Update receivableAcct so subsequent ledgers use it
            // (We can't reassign const, so we use a mutable wrapper)
            if (!receivableAcct) Object.assign(receivableAcct || {}, newCtrlObj);
            controlAcct = newCtrlObj;
          } else {
            controlAcct = newCtrlObj;
          }
        } catch (ctrlErr) {
          const msg = parseErpError(ctrlErr);
          if (msg.toLowerCase().includes("duplicate") || msg.toLowerCase().includes("already")) {
            controlAcct = { name: ctrlFull, account_name: ctrlName };
            logger.info("[OB] Control account already exists: " + ctrlFull);
          } else {
            logger.warn("  " + l.name + " skipped — no " + ctrlType + " control account and could not create: " + msg);
            skippedCount++; continue;
          }
        }
      }
      // ── END auto-create control account ──────────────────────────────────────
      // FIX: Auto-create the Customer/Supplier if it doesn't exist in ERPNext yet.
      // Previously these were silently skipped, losing their opening balance.
      // Now we create the party on the fly so 100% of OB rows are captured.
      const partySet = isCustomer ? _existingCustomers : _existingSuppliers;
      if (partySet.size > 0 && !partySet.has(l.name.toLowerCase())) {
        // Party missing — auto-create it now before posting the OB row
        try {
          if (isCustomer) {
            await client.post("/api/resource/Customer", {
              doctype:        "Customer",
              customer_name:  l.name,
              customer_type:  "Company",
              customer_group: _customerGroup || "Commercial",
              territory:      _territory    || "India",
            });
          } else {
            await client.post("/api/resource/Supplier", {
              doctype:        "Supplier",
              supplier_name:  l.name,
              supplier_type:  "Company",
              supplier_group: _supplierGroup || "Services",
            });
          }
          partySet.add(l.name.toLowerCase());
          logger.info("  [OB] Auto-created " + (isCustomer ? "Customer" : "Supplier") + " for OB: " + l.name);
        } catch (partyErr) {
          const pmsg = parseErpError(partyErr);
          if (pmsg.toLowerCase().includes("duplicate") || pmsg.toLowerCase().includes("already")) {
            partySet.add(l.name.toLowerCase());
            logger.info("  [OB] Party already exists: " + l.name);
          } else {
            logger.warn("  " + l.name + " skipped — could not auto-create " + (isCustomer ? "Customer" : "Supplier") + ": " + pmsg);
            skippedCount++; continue;
          }
        }
      }
      allJERows.push({
        account:                    controlAcct.name,
        party_type:                 isCustomer ? "Customer" : "Supplier",
        party:                      l.name,
        debit_in_account_currency:  debitAmt,
        credit_in_account_currency: creditAmt,
        is_advance:                 "No",
        user_remark:                l.name + " opening balance",
      });
      logger.info("  " + l.name + " → " + (isCustomer ? "Customer" : "Supplier") +
                  ": " + (debitAmt > 0 ? debitAmt + " Dr" : creditAmt + " Cr"));
    } else {
      // GL account — find or auto-create in ERPNext
      let acct = findErpAccount(l.name);
      if (!acct) {
        // The ledger exists in Tally but has no ERPNext account yet.
        // Auto-create it so opening balance is not silently lost.
        // Determine root_type from Tally parentGroup:
        const rootType = ["bank account","cash-in-hand","deposit","loan","asset","stock","debtor","receivable"]
          .some((k) => pg.includes(k)) ? "Asset"
          : ["capital","reserve","loan (liability)","unsecured","secured"].some((k) => pg.includes(k)) ? "Equity"
          : ["income","sales","revenue"].some((k) => pg.includes(k)) ? "Income"
          : ["expense","purchase","indirect","direct expense"].some((k) => pg.includes(k)) ? "Expense"
          : "Liability"; // safe default for unknown groups

        // Find parent GROUP account in ERPNext using the Tally parentGroup name.
        // IMPORTANT: byFullName only has LEAF accounts — must use byGroupName for parents.
        const parentGroupName = l.parentGroup || "";
        let parentAccount = null;
        if (parentGroupName) {
          const parentKey = parentGroupName.trim().toLowerCase();
          const withAbbr  = parentKey + " - " + companyAbbr.toLowerCase();
          if      (byGroupName.has(withAbbr))   parentAccount = byGroupName.get(withAbbr).name;
          else if (byGroupName.has(parentKey))  parentAccount = byGroupName.get(parentKey).name;
          else {
            // Partial match on group name
            for (const [gk, ga] of byGroupName) {
              if (gk.startsWith(parentKey)) { parentAccount = ga.name; break; }
            }
          }
        }
        // Fall back to a non-root group of the correct root_type (has a parent itself)
        if (!parentAccount) {
          const fallback = groupAccounts.find((a) => a.root_type === rootType && a.parent_account);
          if (fallback) parentAccount = fallback.name;
        }
        // Last resort: absolute root group of correct type
        if (!parentAccount) {
          const rootAcct = groupAccounts.find((a) => a.root_type === rootType);
          if (rootAcct) parentAccount = rootAcct.name;
        }

        try {
          await client.post("/api/resource/Account", {
            doctype:        "Account",
            account_name:   l.name,
            company:        companyName,
            is_group:       0,
            root_type:      rootType,
            parent_account: parentAccount,
          });
          const newName = l.name + " - " + companyAbbr;
          // Add to lookup maps so subsequent ledgers can reference it as parent
          const newAcctObj = { name: newName, account_name: l.name, is_group: 0, root_type: rootType, account_type: "" };
          byAccountName.set(l.name.toLowerCase(), newAcctObj);
          byFullName.set(newName.toLowerCase(), newAcctObj);
          acct = newAcctObj;
          logger.info("  [OB] Auto-created missing account: " + newName + " (rootType: " + rootType + ")");
        } catch (createErr) {
          const msg = parseErpError(createErr);
          if (msg.toLowerCase().includes("duplicate") || msg.toLowerCase().includes("already")) {
            // Account was just created — fetch it
            acct = findErpAccount(l.name);
            if (!acct) {
              const fallbackName = l.name + " - " + companyAbbr;
              acct = { name: fallbackName, account_name: l.name };
            }
            logger.info("  [OB] Account already exists: " + l.name);
          } else {
            logger.warn("  " + l.name + " skipped — could not auto-create account: " + msg);
            skippedCount++; continue;
          }
        }
      }
      allJERows.push({
        account:                    acct.name,
        debit_in_account_currency:  debitAmt,
        credit_in_account_currency: creditAmt,
        is_advance:                 "No",
        user_remark:                l.name + " opening balance",
      });
      logger.info("  " + l.name + " → GL '" + acct.name + "': " +
                  (debitAmt > 0 ? debitAmt + " Dr" : creditAmt + " Cr"));
    }
  }

  if (allJERows.length === 0) {
    logger.warn("[OB] No rows resolved — all " + skippedCount + " ledgers skipped");
    return { created: 0, updated: 0, failed: skippedCount };
  }

  // Balance with Temporary Opening if debit != credit
  const totalDr = allJERows.reduce((s, r) => s + r.debit_in_account_currency,  0);
  const totalCr = allJERows.reduce((s, r) => s + r.credit_in_account_currency, 0);
  const diff    = Math.round((totalDr - totalCr) * 100) / 100;
  if (Math.abs(diff) > 0.01) {
    try {
      const tmpRes = await client.get("/api/resource/Account", {
        params: {
          filters: JSON.stringify([["Account","account_name","=","Temporary Opening"],["Account","company","=",companyName]]),
          fields: '["name"]', limit: 1,
        },
      });
      const tmpAcct = tmpRes.data?.data?.[0]?.name;
      if (tmpAcct) {
        allJERows.push({
          account:                    tmpAcct,
          debit_in_account_currency:  diff < 0 ? Math.abs(diff) : 0,
          credit_in_account_currency: diff > 0 ? diff : 0,
          is_advance:                 "No",
          user_remark:                "Balancing entry",
        });
        logger.info("[OB] Balancing row added: " + Math.abs(diff) + (diff > 0 ? " Cr" : " Dr") + " via Temporary Opening");
      } else {
        logger.warn("[OB] JE unbalanced by " + Math.abs(diff) + " — Temporary Opening account not found");
      }
    } catch (_) {}
  }

  // ── Idempotency: skip if unchanged, replace only if balances changed ─────────────────────
  const jeTitle  = "Tally OB - " + companyName;
  const jeRemark = "Tally Opening Balance [" + companyName + "]";
  // Include control account names in the fingerprint so that if the Payable/Receivable
  // control account was wrong in a previous run (e.g. "ABC Traders - C" instead of
  // "Creditors - C"), the fingerprint changes and the JE gets replaced with correct data.
  const fpContent = allJERows
    .map((r) => (r.party || r.account) + ":" + r.debit_in_account_currency + ":" + r.credit_in_account_currency)
    .sort().join("|")
    + "|receivable:" + (receivableAcct ? receivableAcct.name : "")
    + "|payable:"    + (payableAcct    ? payableAcct.name    : "");
  const newFp = createHash("md5").update(fpContent).digest("hex").slice(0, 8);

  // Find any existing OB JE by title (Draft OR Submitted)
  let existingJE = null;
  try {
    const res = await client.get("/api/resource/Journal Entry", {
      params: {
        filters: JSON.stringify([
          ["Journal Entry", "title",        "=",  jeTitle],
          ["Journal Entry", "voucher_type", "=",  "Opening Entry"],
          ["Journal Entry", "company",      "=",  companyName],
        ]),
        fields: '["name","docstatus","user_remark"]', limit: 1,
      },
    });
    existingJE = res.data?.data?.[0] || null;
  } catch (_) {}

  if (existingJE) {
    const storedFp = ((existingJE.user_remark || "").match(/\|fp:([^\s]+)/) || [])[1] || "";
    if (storedFp === newFp && existingJE.docstatus === 1) {
      // Only skip if fingerprint matches AND it's already submitted
      // If it's a draft (previous submit failed), always retry the submit
      logger.info("[OB] Opening balance JE unchanged and submitted — skipping: " + existingJE.name);
      return { created: 0, updated: 0, failed: 0, skipped: withBalance.length };
    }
    if (storedFp === newFp && existingJE.docstatus === 0) {
      // Draft exists with same data — just retry submission
      // FIX: frappe.client.submit requires { doc: fullDoc } not bare { doctype, name }
      logger.info("[OB] Retrying submit on existing draft JE: " + existingJE.name);
      try {
        const retryRes = await client.get("/api/resource/Journal Entry/" + encodeURIComponent(existingJE.name));
        const retryDoc = retryRes?.data?.data;
        if (!retryDoc) throw new Error("Could not fetch full JE doc for retry submit: " + existingJE.name);
        await client.post("/api/method/frappe.client.submit", { doc: retryDoc });
        logger.success("[OB] Opening Entry JE submitted (retry): " + existingJE.name);
        return { created: 1, updated: 0, failed: 0 };
      } catch (submitErr) {
        logger.error("[OB] Draft submit retry failed: " + existingJE.name + " — " + parseErpError(submitErr));
        logger.error("[OB] Go to ERPNext → Journal Entry → " + existingJE.name + " to review and submit manually");
        return { created: 1, updated: 0, failed: 0 };
      }
    }
    // Data changed — delete the old JE and recreate
    try {
      if (existingJE.docstatus === 1) {
        const cancelRes = await client.get("/api/resource/Journal Entry/" + encodeURIComponent(existingJE.name));
        const cancelDoc = cancelRes?.data?.data;
        if (cancelDoc) await client.post("/api/method/frappe.client.cancel", { doc: cancelDoc });
      }
      await client.delete("/api/resource/Journal Entry/" + encodeURIComponent(existingJE.name));
      logger.info("[OB] Opening balances changed — replacing JE: " + existingJE.name);
    } catch (_) {}
  }

  // ── POST the single Opening Entry JE ───────────────────────────────────────────────────────────────────
  // Post as Draft first (docstatus:0), then submit separately.
  // This is safer than posting submitted directly — ERPNext rejects
  // submission if any row has a validation issue, but accepts the draft
  // so the data is saved and the actual error is visible in the response.
  let jeName = null;
  try {
    const res = await client.post("/api/resource/Journal Entry", {
      doctype:      "Journal Entry",
      voucher_type: "Opening Entry",
      title:        jeTitle,
      company:      companyName,
      posting_date: new Date().toISOString().slice(0, 10),
      user_remark:  jeRemark + " |fp:" + newFp,
      accounts:     allJERows,
      docstatus:    0,                         // Draft first
    });
    jeName = res.data?.data?.name;
    logger.info("[OB] Draft Opening Entry JE created: " + jeName +
                " (" + allJERows.length + " rows, " + skippedCount + " skipped)");
  } catch (err) {
    logger.error("[OB] Failed to create Opening Entry JE (draft): " + parseErpError(err));
    return { created: 0, updated: 0, failed: 1 };
  }

  // Fix title (Frappe overrides it for Opening Entry docs)
  if (jeName) {
    try { await client.put("/api/resource/Journal Entry/" + encodeURIComponent(jeName), { title: jeTitle }); } catch (_) {}
  }

  // Submit the draft
  if (jeName) {
    try {
      // FIX: frappe.client.submit requires { doc: { doctype, name } } — not bare fields.
      // Previously passed { doctype, name } directly which caused:
      //   TypeError: submit() missing 1 required positional argument: 'doc'
      const fullRes = await client.get("/api/resource/Journal Entry/" + encodeURIComponent(jeName));
      const fullDoc = fullRes?.data?.data;
      if (!fullDoc) throw new Error("Could not fetch full JE doc for submit: " + jeName);
      await client.post("/api/method/frappe.client.submit", { doc: fullDoc });
      logger.success("[OB] Opening Entry JE submitted: " + jeName +
                     " (" + allJERows.length + " rows from " + withBalance.length + " ledgers, " + skippedCount + " skipped)");
      return { created: 1, updated: 0, failed: skippedCount };
    } catch (submitErr) {
      // Submit failed but draft exists — log as error so it surfaces in error filters
      const errMsg = parseErpError(submitErr);
      logger.error("[OB] Opening Entry JE saved as Draft (submit failed): " + jeName + " — " + errMsg);
      logger.error("[OB] Go to ERPNext → Journal Entry → " + jeName + " to review and submit manually");
      // Return as created so the draft is not lost on next sync (fingerprint stored in remark)
      return { created: 1, updated: 0, failed: 0 };
    }
  }
  return { created: 0, updated: 0, failed: 1 };
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

  // ── FIX: Auto-convert parent cost centre to is_group=1 if ERPNext says "not a group node" ──
  // When Tally sends children under a parent (e.g. "Jio") that ERPNext has already
  // created as a LEAF (is_group=0), ERPNext rejects the child with "Jio - C is not a
  // group node". We detect this and patch the parent to is_group=1 automatically.
  const _groupFixedCostCentres = new Set();
  async function ensureCostCentreIsGroup(parentName) {
    if (!parentName || _groupFixedCostCentres.has(parentName)) return;
    _groupFixedCostCentres.add(parentName);
    try {
      const chkRes = await client.get("/api/resource/Cost Center/" + encodeURIComponent(parentName), {
        params: { fields: '["name","is_group"]' }
      });
      const isGroup = chkRes?.data?.data?.is_group;
      if (isGroup === 0 || isGroup === false) {
        await client.put("/api/resource/Cost Center/" + encodeURIComponent(parentName), { is_group: 1 });
        logger.info("[CC] Auto-converted to group node: " + parentName);
      }
    } catch (_) {}
  }

  // Pre-fix: for every cost centre that HAS children in this batch, ensure the parent is a group
  const parentNames = new Set();
  for (const cc of sorted) {
    if (cc.parent && cc.parent.trim().toLowerCase() !== "primary") {
      const withSuffix = cc.parent.trim() + " - " + companyAbbr;
      parentNames.add(withSuffix);
    }
  }
  for (const parentName of parentNames) {
    await ensureCostCentreIsGroup(parentName);
  }

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

// ── autoCreateFiscalYear ────────────────────────────────────────────────────
// Auto-creates the Indian FY (Apr-Mar) covering a given date, if missing in ERPNext.
async function autoCreateFiscalYear(client, dateStr, companyName) {
  if (!dateStr) return false;
  const d = new Date(dateStr);
  const year = d.getFullYear();
  const month = d.getMonth() + 1;
  const fyStart = month >= 4 ? year : year - 1;
  const fyEnd   = fyStart + 1;
  const fyName  = fyStart + "-" + fyEnd;
  const startDate = fyStart + "-04-01";
  const endDate   = fyEnd   + "-03-31";
  try {
    const fyRes = await client.get("/api/resource/Fiscal Year/" + encodeURIComponent(fyName));
    const fyDoc = fyRes && fyRes.data && fyRes.data.data || {};
    const companies = fyDoc.companies || [];
    if (!companies.some((c) => c.company === companyName)) {
      companies.push({ company: companyName });
      await client.put("/api/resource/Fiscal Year/" + encodeURIComponent(fyName), { companies });
      logger.info("[FY] Linked " + companyName + " to existing Fiscal Year: " + fyName);
    }
    return true;
  } catch (_) {}
  try {
    await client.post("/api/resource/Fiscal Year", {
      doctype: "Fiscal Year", year: fyName,
      year_start_date: startDate, year_end_date: endDate,
      companies: [{ company: companyName }],
    });
    logger.info("[FY] Auto-created Fiscal Year: " + fyName + " (" + startDate + " to " + endDate + ") for " + companyName);
    return true;
  } catch (err) {
    const msg = parseErpError(err);
    if (msg.toLowerCase().includes("duplicate") || msg.toLowerCase().includes("already")) {
      logger.info("[FY] Fiscal Year already exists: " + fyName);
      return true;
    }
    logger.warn("[FY] Could not auto-create Fiscal Year " + fyName + ": " + msg);
    return false;
  }
}
// ── END autoCreateFiscalYear ────────────────────────────────────────────────

// -- Sales / Purchase Invoices ------------------------------------------------
export async function syncInvoicesToErpNext(vouchers, companyName, creds = {}, voucherTypeResolver = null) {
  const client = createErpClient(creds);
  // ── MULTI-TENANT FIX: Save the original Tally company name BEFORE resolving ──
  // companyName starts as the Tally company name (e.g. "Tally", "ABC Corp").
  // resolveErpNextCompany() overwrites it with the ERPNext name (e.g. "Company2").
  // We need the Tally name later to call fetchTallyLedgers() for balance data.
  //
  // Two sources, in priority order:
  //   1. creds._tallyCompanyName — set by runFullSync() for full-sync jobs
  //   2. companyName before resolution — correct for standalone /sync/invoices calls
  //
  // This works correctly in multi-tenant mode: creds is a per-request object
  // (spread-copied in runFullSync), never shared between concurrent jobs.
  const tallyCompanyName = (creds && creds._tallyCompanyName) || companyName.trim();

  companyName  = await resolveErpNextCompany(client, companyName, creds);
  const companyAbbr = await getCompanyAbbr(client, companyName);
  logger.info("Syncing invoices to ERPNext for " + companyName + " (abbr: " + companyAbbr + ") | Tally company: " + tallyCompanyName);

  // Ensure HSN field is non-mandatory so placeholder items can be created without an HSN
  await ensureHsnNotMandatory(client);
  // Create custom_tally_voucher_no field on Sales Invoice (shows Tally number in list)
  await ensureTallyCustomFields(client);
  // Resolve real leaf Customer/Supplier groups before auto-creating parties
  await resolveGroups(client);

  // Use resolver to map custom types (e.g. "Tax Invoice Mumbai") to base type
  const resolveBase = voucherTypeResolver || ((name) => name);
  const salesVouchers    = vouchers.filter((v) => {
    const base = resolveBase(v.voucherType);
    return base === "Sales" || base === "Credit Note" ||
           (v.voucherType || "").toLowerCase().includes("sales invoice");
  });
  const purchaseVouchers = vouchers.filter((v) => {
    const base = resolveBase(v.voucherType);
    return base === "Purchase" || base === "Debit Note" ||
           (v.voucherType || "").toLowerCase().includes("purchase invoice");
  });

  logger.info("Invoice breakdown - sales: " + salesVouchers.length + ", purchase: " + purchaseVouchers.length);

  // ── ensureLeafAccount ────────────────────────────────────────────────────────
  // Resolves a Tally ledger name (e.g. "Sales", "Purchase") to a real ERPNext
  // LEAF account. If ERPNext only has a GROUP like "Sales Accounts - C" but no
  // "Sales - C" leaf, income_account on invoice items will be blank/wrong.
  // This function detects that case and auto-creates the missing leaf account.
  async function ensureLeafAccount(ledgerName, rootType, parentGroupHint) {
    // Try normal resolution first
    const resolved = await resolveAccount(client, ledgerName, companyAbbr, companyName);
    if (resolved) {
      try {
        const chk = await client.get("/api/resource/Account/" + encodeURIComponent(resolved), {
          params: { fields: '["is_group","account_type"]' },
        });
        const acctData = chk && chk.data && chk.data.data;
        if (acctData && !acctData.is_group) {
          // Only fix account_type if it's Payable or Receivable — these cause ERPNext to
          // require a party on invoice rows which breaks submission.
          // Do NOT flip between Income Account <-> Expense Account: some ledgers (e.g.
          // "Godsman", "Cash322") appear in both sales AND purchase vouchers, and flipping
          // back and forth on every sync causes "Fixed ... Income → Expense → Income" loops.
          const badTypes = ["Payable", "Receivable"];
          const expectedType = rootType === "Income" ? "Income Account" : "Expense Account";
          if (acctData.account_type && badTypes.includes(acctData.account_type)) {
            try {
              await client.put("/api/resource/Account/" + encodeURIComponent(resolved), {
                account_type: expectedType,
                root_type:    rootType,
              });
              logger.info("[Invoice] Fixed account_type on " + resolved + ": " + acctData.account_type + " → " + expectedType);
            } catch (_) {}
          }
          logger.info("[Invoice] income/expense account resolved: " + ledgerName + " → " + resolved);
          return resolved;
        }
        logger.info("[Invoice] " + resolved + " is a GROUP — will auto-create leaf \"" + ledgerName + " - " + companyAbbr + "\" under it");
      } catch (_) {}
    }

    // Find the correct parent group
    const suffixedName = ledgerName + " - " + companyAbbr;
    let parentAccount = resolved || null; // use the group we found if any

    if (!parentAccount && parentGroupHint) {
      try {
        const hintRes = await client.get("/api/resource/Account", {
          params: {
            filters: JSON.stringify([
              ["Account", "account_name", "like", "%" + parentGroupHint + "%"],
              ["Account", "company",      "=",    companyName],
              ["Account", "is_group",     "=",    1],
            ]),
            fields: '["name"]', limit: 1,
          },
        });
        const found = hintRes && hintRes.data && hintRes.data.data && hintRes.data.data[0] && hintRes.data.data[0].name;
        if (found) parentAccount = found;
      } catch (_) {}
    }

    if (!parentAccount) {
      try {
        const rtRes = await client.get("/api/resource/Account", {
          params: {
            filters: JSON.stringify([
              ["Account", "root_type", "=", rootType],
              ["Account", "company",   "=", companyName],
              ["Account", "is_group",  "=", 1],
            ]),
            fields: '["name"]', limit: 1,
          },
        });
        const found = rtRes && rtRes.data && rtRes.data.data && rtRes.data.data[0] && rtRes.data.data[0].name;
        if (found) parentAccount = found;
      } catch (_) {}
    }

    const accountType = rootType === "Income" ? "Income Account" : "Expense Account";
    try {
      await client.post("/api/resource/Account", {
        doctype:        "Account",
        account_name:   ledgerName,
        company:        companyName,
        is_group:       0,
        account_type:   accountType,
        root_type:      rootType,
        parent_account: parentAccount,
      });
      logger.info("[Invoice] Auto-created leaf account: \"" + suffixedName + "\" under " + parentAccount);
      _accountCache.set(ledgerName + "::" + companyAbbr, suffixedName);
    } catch (err) {
      const msg = parseErpError(err);
      if (msg.toLowerCase().includes("duplicate") || msg.toLowerCase().includes("already")) {
        logger.info("[Invoice] Leaf account already exists: " + suffixedName);
      } else {
        logger.warn("[Invoice] Could not auto-create leaf account \"" + suffixedName + "\": " + msg);
      }
    }
    return suffixedName;
  }

  // Resolve global income/expense accounts — ensure they are LEAF accounts not groups.
  // ERPNext India COA has "Sales Accounts - C" as a group with no "Sales - C" leaf.
  // ensureLeafAccount detects this and auto-creates the missing leaf so income_account
  // on every invoice item line is always populated correctly.
  const incomeAccount  = await ensureLeafAccount("Sales",    "Income",  "Sales Accounts")
                      || ("Sales - " + companyAbbr);
  const expenseAccount = await ensureLeafAccount("Purchase", "Expense", "Purchase Accounts")
                      || ("Purchase - " + companyAbbr);
  logger.info("[Invoice] Global income account: " + incomeAccount + ", expense account: " + expenseAccount);

  const salesMapper = (v) => {
    // ── Resolve the income account for this voucher ──────────────────────────────
    // Priority: Tally sales ledger entry name (e.g. "Sales", "Sales - Retail") resolved
    // to its ERPNext account. Falls back to the globally resolved incomeAccount.
    // This is the same ledger that shows as "Sales ledger: Sales" in the Tally voucher.
    const resolvedIncomeAccount = v._resolvedSalesAccount || incomeAccount;

    // ── Resolve cost centre ──────────────────────────────────────────────────────
    // Use the cost centre attached to the sales ledger entry first; fall back to
    // the first cost centre found on any entry in the voucher.
    const tallyCC      = v.salesCostCentre || v.firstCostCentre || null;
    const costCenterERP = tallyCC ? (tallyCC + " - " + companyAbbr) : null;

    // Use real Tally inventory items if available, else fall back to single placeholder row.
    // rate = Tally RATE field (UoM suffix already stripped in tallyClient).
    // If rate is 0 but amount and qty are set, derive rate = amount / qty.
    const inventoryRows = (v.inventoryItems || []).map((i) => {
      const qty    = i.qty    || 1;
      const amount = i.amount || 0;
      const rate   = i.rate   || (amount / qty) || 0;
      const row = {
        item_code:      i.itemName,
        item_name:      i.itemName,
        qty,
        rate:           Math.abs(rate),
        amount:         Math.abs(amount) || Math.abs(rate * qty),
        income_account: resolvedIncomeAccount, // Tally sales ledger → ERPNext income account
      };
      // Map Tally godown to ERPNext warehouse (best-effort: "GodownName - CompanyAbbr")
      if (i.godownName) row.warehouse = i.godownName + " - " + companyAbbr;
      // FIX: Skip "Primary Batch" — Tally's internal placeholder for non-batch items.
      // Sending it to ERPNext causes "Could not find Batch No: Primary Batch" errors.
      if (i.batchName && i.batchName.trim().toLowerCase() !== "primary batch")
        row.batch_no = i.batchName;
      // Apply cost centre to each item line if available
      if (costCenterERP) row.cost_center = costCenterERP;
      return row;
    });

    // ── Extra ledger-line items (e.g. "Godsman", "Cash322") ─────────────────────
    // In Tally, some vouchers book additional income/expense lines as ALLLEDGERENTRIES
    // (not as stock items). These appear in Tally's voucher screen as separate line
    // amounts under the party entry. We map each non-party, non-salesLedger entry
    // that has a positive amount as an extra item row so ERPNext totals match Tally.
    const partyName      = (v.partyName || "").trim().toLowerCase();
    const salesLedgerKey = (v.salesLedgerName || "").trim().toLowerCase();
    const ledgerRows = (v.entries || [])
      .filter((e) => {
        const key = (e.ledger || "").trim().toLowerCase();
        return key
          && key !== partyName        // skip the party (customer) entry
          && key !== salesLedgerKey   // skip the main sales ledger (already in income_account)
          && Math.abs(e.amount || 0) > 0.001;
      })
      .map((e) => {
        // Use the pre-resolved Income account (guaranteed leaf, correct account_type)
        // instead of a raw "LedgerName - CompanyAbbr" string which may be Payable type.
        const ledgerAccount = (v._resolvedLedgerEntryAccounts && v._resolvedLedgerEntryAccounts[e.ledger])
          || (e.ledger + " - " + companyAbbr);
        const amt           = Math.abs(e.amount || 0);
        const row = {
          item_code:      e.ledger,   // use ledger name as item_code (auto-created in pre-sync)
          item_name:      e.ledger,
          qty:            1,
          rate:           amt,
          amount:         amt,
          income_account: ledgerAccount,
        };
        if (costCenterERP) row.cost_center = costCenterERP;
        return row;
      });

    // Combine: inventory rows first, then ledger rows. If both are empty, use placeholder.
    const items = inventoryRows.length > 0 || ledgerRows.length > 0
      ? [...inventoryRows, ...ledgerRows]
      : [{ item_code: "Sales Item", item_name: "Sales Item", qty: 1, rate: v.netAmount || 0,
           income_account: resolvedIncomeAccount,
           ...(costCenterERP ? { cost_center: costCenterERP } : {}) }];

    // voucherNumber fallback: use date+type as unique key if Tally didn't export a number
    const vSalesNum    = v.voucherNumber || (v.voucherDate + "-" + (v.voucherType || "Sales") + "-" + (v.guid || "").slice(-6));
    const salesRemarks = "Tally Voucher No: " + vSalesNum + (v.narration ? " | " + v.narration : "");

    // DATE FIX: Always use the date Tally recorded on the voucher (YYYY-MM-DD).
    // Never silently fall back to today — log missing dates so data issues are traceable.
    if (!v.voucherDate) {
      logger.warn("Sales voucher missing voucherDate — check Tally DATE field", { voucher: vSalesNum, guid: v.guid });
    }
    const effectiveSalesDate = v.voucherDate || new Date().toISOString().slice(0, 10);

    // ── debit_to: the party receivable account ───────────────────────────────────
    // ERPNext requires debit_to to be the Receivable account for the customer.
    // Standard ERPNext pattern: "Debtors - <Abbr>" (auto-resolved from party).
    // We set it explicitly to avoid validation errors when the default differs.
    const debitToAccount = v._resolvedDebitToAccount || ("Debtors - " + companyAbbr);

    return {
      // Use `remarks` (real DB column, unique per voucher) as the idempotency key.
      filters: { remarks: salesRemarks },
      doc: {
        title:             "Tally:" + vSalesNum,
        customer:          v.partyName || "Walk-in Customer",
        posting_date:      effectiveSalesDate,   // Tally voucher date (not today)
        due_date:          v.dueDate || effectiveSalesDate, // Tally bill due date, else voucher date
        set_posting_time:  1,                    // preserve Tally date, not today
        company:           companyName,
        // ── Receivable account (debit_to) ───────────────────────────────────────
        // This is the "Current balance" / party account Tally shows at the top of
        // the voucher screen (e.g. "ABCDTraders - Debtors"). ERPNext calls it debit_to.
        debit_to:          debitToAccount,
        // ── Tally voucher number storage ────────────────────────────────────────
        // po_no   = "Customer's PO No." — always visible in Sales Invoice list view.
        //           We store the Tally number here so it appears in the ID/No. column.
        // custom_tally_voucher_no = our own custom field (created by ensureTallyCustomFields).
        //           Provides a searchable, non-overwritable copy of the Tally number.
        po_no:                   v.referenceNo || vSalesNum, // supplier ref / Tally bill ref
        custom_tally_voucher_no: vSalesNum,
        custom_tally_id:         v.guid || vSalesNum,
        // ── Tally Sales Ledger + Balance fields ─────────────────────────────────
        // These three fields mirror what Tally shows at the top of the Sales voucher:
        //   "Sales ledger : Sales"              → custom_tally_sales_ledger
        //   "Current balance : 25,68,68,636 Dr" → custom_tally_party_balance
        //   "Current balance : 5,59,40,29,876 Cr" → custom_tally_sales_ledger_balance
        custom_tally_sales_ledger:         v.salesLedgerName             || "",
        custom_tally_party_balance:        v._tallyPartyBalance           || "",
        custom_tally_sales_ledger_balance: v._tallySalesLedgerBalance     || "",
        // ── Narration → additional notes on the ERPNext invoice ─────────────────
        // Tally's narration is the free-text memo on the voucher (shown below entries).
        // Map it to both remarks (idempotency key) and terms (visible on invoice PDF).
        remarks:           salesRemarks,
        terms:             v.narration || "",   // Tally narration → Terms & Conditions field
        items,
      },
    };
  };

  const purchaseMapper = (v) => {
    // Resolve expense account: prefer Tally's purchase ledger entry, fall back to global
    const resolvedExpenseAccount = v._resolvedPurchaseLedgerAccount || expenseAccount;

    // Cost centre from Tally entries
    const tallyCC       = v.salesCostCentre || v.firstCostCentre || null;
    const costCenterERP = tallyCC ? (tallyCC + " - " + companyAbbr) : null;

    const inventoryRowsP = (v.inventoryItems || []).map((i) => {
      const qty    = i.qty    || 1;
      const amount = i.amount || 0;
      const rate   = i.rate   || (amount / qty) || 0;
      const row = {
        item_code:       i.itemName,
        item_name:       i.itemName,
        qty,
        rate:            Math.abs(rate),
        amount:          Math.abs(amount) || Math.abs(rate * qty),
        expense_account: resolvedExpenseAccount,
      };
      if (i.godownName)  row.warehouse  = i.godownName + " - " + companyAbbr;
      // FIX: Strip batch_no if it is a pure integer string (e.g. "2", "10").
      // Tally sometimes exports internal sequence numbers as batch names.
      // These don't exist as Batch records in ERPNext and cause:
      //   "Could not find Row #1: Batch No: 2"
      // Also skip Tally's "Primary Batch" placeholder for non-batch items.
      const rawBatchP = (i.batchName || "").trim();
      if (rawBatchP
          && rawBatchP.toLowerCase() !== "primary batch"
          && !/^\d+$/.test(rawBatchP))
        row.batch_no = rawBatchP;
      if (costCenterERP) row.cost_center = costCenterERP;
      return row;
    });

    // Extra ledger-line items on purchase invoices (same logic as salesMapper)
    const partyKeyPI   = (v.partyName || "").trim().toLowerCase();
    const ledgerKeyPI  = (v.salesLedgerName || "").trim().toLowerCase();
    const ledgerRowsP  = (v.entries || [])
      .filter((e) => {
        const k = (e.ledger || "").trim().toLowerCase();
        return k && k !== partyKeyPI && k !== ledgerKeyPI && Math.abs(e.amount || 0) > 0.001;
      })
      .map((e) => {
        const ledgerAccount = (v._resolvedLedgerEntryAccounts && v._resolvedLedgerEntryAccounts[e.ledger])
          || (e.ledger + " - " + companyAbbr);
        const amt = Math.abs(e.amount || 0);
        const row = {
          item_code:       e.ledger,
          item_name:       e.ledger,
          qty:             1,
          rate:            amt,
          amount:          amt,
          expense_account: ledgerAccount,
        };
        if (costCenterERP) row.cost_center = costCenterERP;
        return row;
      });

    const items = inventoryRowsP.length > 0 || ledgerRowsP.length > 0
      ? [...inventoryRowsP, ...ledgerRowsP]
      : [{ item_code: "Purchase Item", item_name: "Purchase Item", qty: 1, rate: v.netAmount || 0,
           expense_account: resolvedExpenseAccount,
           ...(costCenterERP ? { cost_center: costCenterERP } : {}) }];

    const vPurchaseNum    = v.voucherNumber || (v.voucherDate + "-" + (v.voucherType || "Purchase") + "-" + (v.guid || "").slice(-6));
    const purchaseRemarks = "Tally Voucher No: " + vPurchaseNum + (v.narration ? " | " + v.narration : "");

    // DATE FIX: Always use the date Tally recorded on the voucher (YYYY-MM-DD).
    // Never silently fall back to today — log missing dates so data issues are traceable.
    if (!v.voucherDate) {
      logger.warn("Purchase voucher missing voucherDate — check Tally DATE field", { voucher: vPurchaseNum, guid: v.guid });
    }
    const effectivePurchaseDate = v.voucherDate || new Date().toISOString().slice(0, 10);

    // credit_to: the supplier payable account (mirrors debit_to on Sales Invoice)
    const creditToAccount = v._resolvedCreditToAccount || ("Creditors - " + companyAbbr);

    return {
      filters: { remarks: purchaseRemarks },
      doc: {
        title:             "Tally:" + vPurchaseNum,
        supplier:          v.partyName || "Unknown Supplier",
        posting_date:      effectivePurchaseDate,  // Tally voucher date (not today)
        set_posting_time:  1,                      // preserve Tally date, not today
        company:           companyName,
        credit_to:         creditToAccount,         // Tally party ledger → ERPNext Payable account
        bill_no:           v.referenceNo || vPurchaseNum,  // supplier invoice number
        bill_date:         effectivePurchaseDate,  // Supplier invoice date = Tally voucher date
        due_date:          v.dueDate || effectivePurchaseDate, // Tally bill due date, else voucher date
        remarks:           purchaseRemarks,
        terms:             v.narration || "",       // Tally narration → Terms & Conditions
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
  // Collect every unique item name from:
  //   1. inventoryItems (stock items from ALLINVENTORYENTRIES)
  //   2. non-party, non-salesLedger ledger entries (e.g. "Godsman", "Cash322")
  //      which are mapped as extra item rows on the invoice (see salesMapper/purchaseMapper)
  // Create any that don't exist yet as non-stock service items so the invoice
  // batch never fails with "Item not found".
  const allVouchersForItems = [...salesVouchers, ...purchaseVouchers];
  const uniqueItemNames = [
    ...new Set([
      // Stock item names from inventory entries
      ...allVouchersForItems
        .flatMap((v) => (v.inventoryItems || []).map((i) => (i.itemName || "").trim()))
        .filter(Boolean),
      // Ledger entry names that will become extra item rows
      ...allVouchersForItems
        .flatMap((v) => {
          const pKey = (v.partyName || "").trim().toLowerCase();
          const sKey = (v.salesLedgerName || "").trim().toLowerCase();
          return (v.entries || [])
            .filter((e) => {
              const k = (e.ledger || "").trim().toLowerCase();
              return k && k !== pKey && k !== sKey && Math.abs(e.amount || 0) > 0.001;
            })
            .map((e) => e.ledger.trim());
        })
        .filter(Boolean),
    ].filter((n) => n !== "Sales Item" && n !== "Purchase Item")),
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

  // ── Build a ledger → closing-balance lookup from Tally masters ───────────────
  // syncInvoicesToErpNext receives only vouchers (no ledger master array), so we
  // fetch the ledger list fresh here. This is a single Tally API call — cheap.
  // The result is used to populate custom_tally_party_balance and
  // custom_tally_sales_ledger_balance on each Sales Invoice.
  //
  // Format helper: turns a signed number into a Tally-style string e.g.
  //   -25686863600 → "25,68,68,636.00 Dr"   (negative = Debit for assets)
  //    559402987600 → "5,59,40,29,876.00 Cr"  (positive = Credit for income)
  function fmtTallyBalance(amount) {
    if (amount === null || amount === undefined || amount === 0) return "0.00";
    const abs = Math.abs(amount).toFixed(2);
    // Indian number formatting (lakhs/crores)
    const [intPart, decPart] = abs.split(".");
    let formatted = "";
    if (intPart.length <= 3) {
      formatted = intPart;
    } else {
      const last3 = intPart.slice(-3);
      const rest  = intPart.slice(0, -3);
      formatted = rest.replace(/\B(?=(\d{2})+(?!\d))/g, ",") + "," + last3;
    }
    const drCr = amount < 0 ? "Dr" : "Cr";
    return formatted + "." + decPart + " " + drCr;
  }

  // We need ledgers to get current balances.
  // Import fetchTallyLedgers lazily to avoid a circular-dep issue — the sync
  // routes already pass the company name down, so we call it directly here.
  let _ledgerBalanceMap = new Map(); // ledgerName (lower) → closingBalance (signed number)
  try {
    // Dynamically import to avoid circular dependency if erpnextClient is
    // used standalone. In practice the route already imported tallyClient.
    const { fetchTallyLedgers } = await import("./tallyClient.js");
    // tallyCompanyName is captured at the top of syncInvoicesToErpNext BEFORE
    // resolveErpNextCompany() overwrites companyName. It is always the correct
    // Tally-side company name regardless of whether this is a full-sync job
    // (creds._tallyCompanyName set by runFullSync) or a standalone API call.
    logger.info("[Invoice] Fetching ledger balances from Tally company: " + tallyCompanyName);
    const allLedgers = await fetchTallyLedgers(tallyCompanyName);
    for (const l of allLedgers) {
      if (l.name) _ledgerBalanceMap.set(l.name.trim().toLowerCase(), l.closingBalance ?? 0);
    }
    logger.info("[Invoice] Loaded " + _ledgerBalanceMap.size + " ledger balances for balance mapping");
  } catch (e) {
    logger.warn("[Invoice] Could not load ledger balances (party/sales-ledger balance fields will be blank): " + e.message);
  }

  // Attach pre-computed balance strings to every sales voucher so salesMapper
  // can read them synchronously (batchSync mapper must be pure/sync).
  for (const v of salesVouchers) {
    // Party (Customer) balance
    const partyKey = (v.partyName || "").trim().toLowerCase();
    const partyBal = _ledgerBalanceMap.has(partyKey) ? _ledgerBalanceMap.get(partyKey) : null;
    v._tallyPartyBalance = partyBal !== null ? fmtTallyBalance(partyBal) : "";

    // Sales Ledger balance
    const salesLedgerKey = (v.salesLedgerName || "").trim().toLowerCase();
    const salesLedgerBal = _ledgerBalanceMap.has(salesLedgerKey) ? _ledgerBalanceMap.get(salesLedgerKey) : null;
    v._tallySalesLedgerBalance = salesLedgerBal !== null ? fmtTallyBalance(salesLedgerBal) : "";
  }

  // ── Pre-resolve per-voucher accounts before batchSync ─────────────────────────
  // For each Sales voucher, resolve:
  //   1. _resolvedSalesAccount  — the ERPNext income account matching Tally's sales ledger
  //                               (e.g. "Sales" → "Sales - T"). Falls back to incomeAccount.
  //   2. _resolvedDebitToAccount — the ERPNext Receivable account for the customer
  //                               (e.g. "Debtors - T"). Falls back to "Debtors - <Abbr>".
  //
  // Resolved values are cached on the voucher object so salesMapper can read them
  // synchronously (batchSync's mapper must be pure/sync).
  const _salesAccountCache  = new Map(); // tallyLedgerName → erpAccountName
  const _debitToCache       = new Map(); // customerName → debitToAccount

  for (const v of salesVouchers) {
    // 1. Sales ledger account — use ensureLeafAccount so we always get a LEAF
    // (not a group like "Sales Accounts - C") for the income_account field.
    if (v.salesLedgerName && !_salesAccountCache.has(v.salesLedgerName)) {
      try {
        const resolved = await ensureLeafAccount(v.salesLedgerName, "Income", "Sales Accounts");
        _salesAccountCache.set(v.salesLedgerName, resolved || incomeAccount);
      } catch (_) {
        _salesAccountCache.set(v.salesLedgerName, incomeAccount);
      }
    }
    v._resolvedSalesAccount = v.salesLedgerName
      ? (_salesAccountCache.get(v.salesLedgerName) || incomeAccount)
      : incomeAccount;

    // 1b. Pre-resolve extra ledger-entry accounts (e.g. "Godsman") as Income leaf accounts.
    // Without this, they get created as Payable type by the auto-create logic, causing
    // "Supplier is required against Payable account Godsman - C" on Sales Invoice submit.
    const partyKeyPre    = (v.partyName || "").trim().toLowerCase();
    const salesKeyPre    = (v.salesLedgerName || "").trim().toLowerCase();
    v._resolvedLedgerEntryAccounts = {};
    for (const e of (v.entries || [])) {
      const k = (e.ledger || "").trim().toLowerCase();
      if (!k || k === partyKeyPre || k === salesKeyPre || Math.abs(e.amount || 0) <= 0.001) continue;
      if (!_salesAccountCache.has(e.ledger)) {
        try {
          const resolved = await ensureLeafAccount(e.ledger, "Income", "Sales Accounts");
          _salesAccountCache.set(e.ledger, resolved || (e.ledger + " - " + companyAbbr));
        } catch (_) {
          _salesAccountCache.set(e.ledger, e.ledger + " - " + companyAbbr);
        }
      }
      v._resolvedLedgerEntryAccounts[e.ledger] = _salesAccountCache.get(e.ledger) || (e.ledger + " - " + companyAbbr);
    }

    // 2. Debit-to (receivable) account for this customer
    const customerKey = (v.partyName || "Walk-in Customer").trim();
    if (!_debitToCache.has(customerKey)) {
      try {
        // ERPNext stores the default receivable account on the Customer doctype.
        const cRes = await client.get("/api/resource/Customer/" + encodeURIComponent(customerKey), {
          params: { fields: '["name","accounts"]' },
        });
        const accts = cRes?.data?.data?.accounts || [];
        const companyAcct = accts.find((a) => a.company === companyName);
        _debitToCache.set(customerKey, companyAcct?.account || ("Debtors - " + companyAbbr));
      } catch (_) {
        _debitToCache.set(customerKey, "Debtors - " + companyAbbr);
      }
    }
    v._resolvedDebitToAccount = _debitToCache.get(customerKey) || ("Debtors - " + companyAbbr);
  }

  // Same pre-resolution for Purchase vouchers: credit_to = Payable account, purchase ledger account
  const _creditToCache          = new Map(); // supplierName → creditToAccount
  const _purchaseAccountCache   = new Map(); // tallyLedgerName → erpAccountName
  for (const v of purchaseVouchers) {
    // 1. Purchase ledger account (e.g. "Purchase" → "Purchase - T")
    if (v.salesLedgerName && !_purchaseAccountCache.has(v.salesLedgerName)) {
      try {
        const resolved = await ensureLeafAccount(v.salesLedgerName, "Expense", "Purchase Accounts");
        _purchaseAccountCache.set(v.salesLedgerName, resolved || expenseAccount);
      } catch (_) {
        _purchaseAccountCache.set(v.salesLedgerName, expenseAccount);
      }
    }
    v._resolvedPurchaseLedgerAccount = v.salesLedgerName
      ? (_purchaseAccountCache.get(v.salesLedgerName) || expenseAccount)
      : expenseAccount;

    // 1b. Pre-resolve extra ledger-entry accounts for purchase invoices (e.g. "Godsman", "Ram500")
    // These are expense-side entries; resolve as Expense to avoid Payable/Receivable type issues.
    const partyKeyP   = (v.partyName || "").trim().toLowerCase();
    const ledgerKeyP  = (v.salesLedgerName || "").trim().toLowerCase();
    v._resolvedLedgerEntryAccounts = v._resolvedLedgerEntryAccounts || {};
    for (const e of (v.entries || [])) {
      const k = (e.ledger || "").trim().toLowerCase();
      if (!k || k === partyKeyP || k === ledgerKeyP || Math.abs(e.amount || 0) <= 0.001) continue;
      if (!_purchaseAccountCache.has(e.ledger)) {
        try {
          const resolved = await ensureLeafAccount(e.ledger, "Expense", "Purchase Accounts");
          _purchaseAccountCache.set(e.ledger, resolved || (e.ledger + " - " + companyAbbr));
        } catch (_) {
          _purchaseAccountCache.set(e.ledger, e.ledger + " - " + companyAbbr);
        }
      }
      v._resolvedLedgerEntryAccounts[e.ledger] = _purchaseAccountCache.get(e.ledger) || (e.ledger + " - " + companyAbbr);
    }

    // 2. credit_to (payable) account for this supplier
    // ── BUG FIX: Validate the resolved account is actually of type Payable ──
    // Some ERPNext installs have accounts like "Godsman - C" typed as Payable even
    // though they are expense/income accounts in Tally. ERPNext then rejects the
    // Purchase Invoice with "Supplier is required against Payable account ...".
    // We always use the global Creditors control account for credit_to — it is
    // guaranteed to be Payable type. The party-level account (from Customer.accounts)
    // is only used if it is explicitly set AND is actually a Payable account.
    const supplierKey = (v.partyName || "Unknown Supplier").trim();
    if (!_creditToCache.has(supplierKey)) {
      let resolvedCreditTo = "Creditors - " + companyAbbr; // safe default
      try {
        const sRes = await client.get("/api/resource/Supplier/" + encodeURIComponent(supplierKey), {
          params: { fields: '["name","accounts"]' },
        });
        const accts = sRes?.data?.data?.accounts || [];
        const companyAcct = accts.find((a) => a.company === companyName);
        if (companyAcct?.account) {
          // Only trust this account if it is actually Payable type
          try {
            const acctRes = await client.get("/api/resource/Account/" + encodeURIComponent(companyAcct.account), {
              params: { fields: '["account_type"]' },
            });
            if (acctRes?.data?.data?.account_type === "Payable") {
              resolvedCreditTo = companyAcct.account;
            } else {
              logger.warn("[Invoice] Supplier " + supplierKey + " account " + companyAcct.account + " is not Payable type — using Creditors control account");
            }
          } catch (_) { /* keep safe default */ }
        }
      } catch (_) { /* keep safe default */ }
      _creditToCache.set(supplierKey, resolvedCreditTo);
    }
    v._resolvedCreditToAccount = _creditToCache.get(supplierKey) || ("Creditors - " + companyAbbr);
  }

  // ── FIX: Batch number auto-retry ─────────────────────────────────────────────
  // When ERPNext returns "Could not find Row #N: Batch No: XXXXXX" it means the
  // item is NOT batch-tracked in ERPNext but Tally had a batch number on it.
  // We strip ALL batch_no fields from that voucher's items and retry once.
  async function batchSyncWithBatchRetry(doctype, vouchers, mapper) {
    const primaryResults = await batchSync(client, doctype, vouchers, mapper);
    // Find failed vouchers that had batch errors and retry without batch_no
    const batchErrVouchers = [];
    for (const v of vouchers) {
      const mapped = await mapper(v);
      if (mapped.doc && mapped.doc.items && mapped.doc.items.some((i) => i.batch_no)) batchErrVouchers.push(v);
    }
    if (batchErrVouchers.length === 0) return primaryResults;
    let retryCreated = 0, retryFailed = 0;
    for (const v of batchErrVouchers) {
      const { filters, doc } = await mapper(v);
      // Check if this voucher already succeeded (look it up)
      try {
        const key = filters.remarks;
        if (!key) continue;
        const chk = await client.get("/api/resource/" + encodeURIComponent(doctype), {
          params: { filters: JSON.stringify([[doctype, "remarks", "like", key.split(" | ")[0] + "%"]]), fields: '["name","docstatus"]', limit: 1 }
        });
        if (chk?.data?.data?.[0]) continue; // already created — skip retry
      } catch (_) {}
      // Strip batch_no from all items
      const docNoBatch = Object.assign({}, doc, {
        items: (doc.items || []).map((i) => {
          const row = Object.assign({}, i);
          delete row.batch_no;
          return row;
        }),
      });
      try {
        await client.post("/api/resource/" + encodeURIComponent(doctype), Object.assign({}, docNoBatch, { doctype }));
        retryCreated++;
        logger.info("[BatchRetry] Created " + doctype + " without batch_no: " + (filters.remarks || ""));
      } catch (retryErr) {
        retryFailed++;
        logger.warn("[BatchRetry] Still failed after stripping batch_no: " + parseErpError(retryErr));
      }
      await sleep(300);
    }
    if (retryCreated > 0) logger.info("[BatchRetry] Recovered " + retryCreated + " invoice(s) by stripping Tally batch numbers");
    return {
      created: primaryResults.created + retryCreated,
      updated: primaryResults.updated,
      failed:  Math.max(0, primaryResults.failed - retryCreated) + retryFailed,
      skipped: primaryResults.skipped || 0,
    };
  }
  // ── END batch-number retry ────────────────────────────────────────────────

  const salesResults    = await batchSyncWithBatchRetry("Sales Invoice",    salesVouchers,    salesMapper);
  const purchaseResults = await batchSyncWithBatchRetry("Purchase Invoice", purchaseVouchers, purchaseMapper);

  // ── Rename Sales Invoices to Tally voucher numbers ─────────────────────────
  // ERPNext REST POST always auto-generates a name (SINV-26-XXXXX) regardless of
  // any `name` field in the body. The only way to force Tally's own number
  // (e.g. "10", "Sales/25-26/0010") as the visible ID is to rename the doc
  // AFTER creation using frappe.client.rename_doc.
  // Purchase Invoice already shows Tally's number via bill_no (Supplier Invoice No).
  // For Sales Invoice we rename so the ID column shows the Tally number directly.
  logger.info("Renaming Sales Invoices to Tally voucher numbers...");
  let renamed = 0, renameSkipped = 0, renameFailed = 0;

  // ── AUTO-REGISTER NAMING SERIES via Document Naming Settings ──────────────
  // Frappe Cloud does NOT allow Developer Mode, so modifying DocType.autoname directly
  // is blocked ("Not in Developer Mode"). The correct API for Frappe Cloud is:
  //   POST /api/method/frappe.model.document_naming_settings.add_prefix
  // OR update the "Document Naming Rule" / "Naming Series" doctype which is always
  // available. The safest cross-version approach is frappe.client.set_default which
  // sets the prefix in the naming series options table used by rename_doc.
  const _registeredSeries = new Set();
  async function ensureNamingSeries(prefix) {
    // prefix = e.g. "TAX/" (already extracted, no dummy suffix needed)
    if (!prefix || _registeredSeries.has(prefix)) return;
    _registeredSeries.add(prefix);
    const seriesEntry = prefix + ".####";

    // Method 1: frappe.model.document_naming_settings — works on Frappe v14/v15 Cloud
    try {
      await client.post("/api/method/frappe.model.document_naming_settings.add_prefix", {
        doctype: "Sales Invoice",
        prefix:  seriesEntry,
      });
      logger.info("[Rename] Registered naming series via add_prefix: " + seriesEntry);
      return;
    } catch (_) {}

    // Method 2: frappe.client.set_default — sets naming_series options globally
    // This is what ERPNext's "Document Naming Settings" page uses under the hood.
    try {
      const optRes = await client.post("/api/method/frappe.client.get_value", {
        doctype:   "DocType",
        filters:   { name: "Sales Invoice" },
        fieldname: ["autoname"],
      });
      const currentOptions = optRes?.data?.message?.autoname || "";
      if (!currentOptions.includes(prefix)) {
        const newOptions = currentOptions ? currentOptions + "\n" + seriesEntry : seriesEntry;
        await client.post("/api/method/frappe.client.set_default", {
          key:   "Sales Invoice-naming_series-options",
          value: newOptions,
        });
        logger.info("[Rename] Registered naming series via set_default: " + seriesEntry);
      }
      return;
    } catch (_) {}

    // Method 3: POST to /api/resource/Series — Frappe stores series counters here.
    // Creating the series entry allows rename_doc to accept the prefix.
    try {
      await client.post("/api/resource/Series", {
        name:    prefix,
        current: 0,
      });
      logger.info("[Rename] Registered naming series via Series doctype: " + prefix);
      return;
    } catch (e3) {
      const msg = parseErpError(e3);
      if (msg.toLowerCase().includes("duplicate") || msg.toLowerCase().includes("already")) {
        logger.info("[Rename] Series already registered: " + prefix);
        return;
      }
      logger.warn("[Rename] Could not register naming series " + seriesEntry + " (all methods failed): " + msg +
        " — invoices will still be created but shown with SINV-XXXXX names instead of Tally numbers");
    }
  }

  // Pre-register all unique series prefixes found in this batch of vouchers
  const uniquePrefixes = new Set();
  for (const v of salesVouchers) {
    if (v.voucherNumber && !/^\d+$/.test(String(v.voucherNumber).trim())) {
      const m = String(v.voucherNumber).match(/^([A-Za-z]+\/)/);
      if (m) uniquePrefixes.add(m[1]);
    }
  }
  if (uniquePrefixes.size > 0) {
    logger.info("[Rename] Pre-registering " + uniquePrefixes.size + " naming series prefix(es): " + [...uniquePrefixes].join(", "));
    for (const prefix of uniquePrefixes) {
      await ensureNamingSeries(prefix);
    }
  }
  // ── END NAMING SERIES AUTO-REGISTRATION ────────────────────────────────────

  for (const v of salesVouchers) {
    const vNum = v.voucherNumber;
    if (!vNum) { renameSkipped++; continue; } // no Tally number — skip

    // ── BUG FIX: ERPNext rejects renaming to a plain integer (e.g. "11", "24") ──
    // Frappe's naming system reserves pure-number names for its own series and
    // returns HTTP 417 when you try to set them via rename_doc.
    // Tally uses plain numbers (1, 2, 3...) as voucher numbers by default.
    // These are already stored in custom_tally_voucher_no so they are fully
    // searchable. We simply skip the rename for plain-number voucher numbers.
    if (/^\d+$/.test(String(vNum).trim())) {
      renameSkipped++;
      continue;
    }

    try {
      // Find the ERPNext doc by remarks (reliable idempotency key set on create)
      const remarksPrefix = "Tally Voucher No: " + vNum;
      const listRes = await client.get("/api/resource/Sales Invoice", {
        params: {
          filters: JSON.stringify([["Sales Invoice","remarks","like", remarksPrefix + "%"]]),
          fields:  '["name","docstatus"]',
          limit:   1,
        },
      });
      const existing = listRes?.data?.data?.[0];
      if (!existing) { renameSkipped++; logger.warn("Rename: no doc found for Tally voucher " + vNum); continue; }

      // Already renamed to the Tally number — nothing to do
      if (existing.name === String(vNum)) { renameSkipped++; continue; }

      // Cannot rename submitted docs
      if (existing.docstatus === 1) { renameSkipped++; logger.warn("Rename skipped (submitted): " + existing.name); continue; }

      // Rename via frappe.client.rename_doc
      await client.post("/api/method/frappe.client.rename_doc", {
        doctype:  "Sales Invoice",
        old_name: existing.name,
        new_name: vNum,
        merge:    false,
      });
      renamed++;
      logger.info("Renamed Sales Invoice " + existing.name + " → " + vNum);
    } catch (e) {
      renameFailed++;
      logger.warn("Could not rename Sales Invoice to " + vNum + ": " + (e?.response?.data?.message || e?.message || e));
    }
    await sleep(300); // gentle throttle — rename is a heavy Frappe operation
  }
  logger.info("Sales Invoice rename complete: " + renamed + " renamed, " + renameSkipped + " skipped, " + renameFailed + " failed");

  // Submit all draft invoices using ERPNext's submit endpoint
  async function submitDraftInvoices(doctype, vouchers) {
    let submitted = 0, failed = 0, fiscalYearMissing = 0;
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
        const errMsg = parseErpError(e);
        // FIX: Detect fiscal year errors — auto-create the missing fiscal year and retry.
        if (errMsg.toLowerCase().includes("fiscal year") || errMsg.toLowerCase().includes("not in any active")) {
          fiscalYearMissing++;
          const vDate = v.voucherDate || new Date().toISOString().slice(0, 10);
          const fyCreated = await autoCreateFiscalYear(client, vDate, companyName);
          if (fyCreated) {
            // Retry submit now that fiscal year exists
            try {
              const retryList = await client.get("/api/resource/" + encodeURIComponent(doctype), {
                params: { filters: JSON.stringify([[doctype, "remarks", "like", ("Tally Voucher No: " + effectiveNum) + "%"]]), fields: '["name","docstatus"]', limit: 1 }
              });
              const retryStub = retryList?.data?.data?.[0];
              if (retryStub && retryStub.docstatus === 0) {
                const retryFull = await client.get("/api/resource/" + encodeURIComponent(doctype) + "/" + encodeURIComponent(retryStub.name));
                const retryDoc = retryFull?.data?.data;
                if (retryDoc) {
                  await client.post("/api/method/frappe.client.submit", { doc: retryDoc });
                  submitted++;
                  fiscalYearMissing--;
                  logger.info("[" + doctype + "] Submitted after auto-creating fiscal year: " + effectiveNum);
                }
              }
            } catch (retryErr) {
              logger.warn("[" + doctype + "] Retry after fiscal year create failed for " + effectiveNum + ": " + parseErpError(retryErr));
            }
          } else if (fiscalYearMissing === 1) {
            logger.warn("[" + doctype + "] Fiscal Year missing for date " + vDate + " — could not auto-create. Re-run sync after creating it manually.");
          }
        } else {
          failed++;
          logger.warn("Could not submit " + doctype + " for voucher " + effectiveNum + ": " + errMsg);
        }
      }
      await sleep(200);
    }
    if (fiscalYearMissing > 0) {
      logger.warn("Submit " + doctype + ": " + submitted + " submitted, " + failed + " failed, " + fiscalYearMissing + " pending fiscal year setup in ERPNext");
    } else {
      logger.info("Submit " + doctype + ": " + submitted + " submitted, " + failed + " failed");
    }
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

  // ── CRITICAL: Save the ORIGINAL Tally company name BEFORE resolving to ERPNext ──
  // After resolveErpNextCompany(), companyName becomes the ERPNext name (e.g. "Company2").
  // But fetchTallyLedgers() inside syncInvoicesToErpNext needs the actual Tally company
  // name (e.g. "Tally"). We stash it in creds so it flows through automatically.
  const tallyCompanyName = companyName.trim();
  creds = { ...creds, _tallyCompanyName: tallyCompanyName };

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

    // syncSmartLedgers: filter tallyData.ledgers to only those used in vouchers
    if (options.syncSmartLedgers     && tallyData.ledgers   && tallyData.ledgers.length   > 0
                                     && tallyData.vouchers  && tallyData.vouchers.length  > 0)
      await runStep("smartLedgers",   () => smartSyncLedgersToErpNext(tallyData.vouchers, tallyData.ledgers, creds));

    if (options.syncOpeningBalances  && tallyData.ledgers   && tallyData.ledgers.length   > 0)
      await runStep("openingBalances", () => syncOpeningBalancesToErpNext(tallyData.ledgers, companyName, creds));

    if (options.syncGodowns          && tallyData.godowns   && tallyData.godowns.length   > 0)
      await runStep("godowns",         () => syncGodownsToErpNext(tallyData.godowns, companyName, creds));

    if (options.syncCostCentres      && tallyData.costCentres && tallyData.costCentres.length > 0)
      await runStep("costCentres",     () => syncCostCentresToErpNext(tallyData.costCentres, companyName, creds));

    if (options.syncStock            && tallyData.stockItems && tallyData.stockItems.length   > 0)
      await runStep("stockItems",      () => syncStockToErpNext(tallyData.stockItems, creds));

    if ((options.syncVouchers || options.syncInvoices) && tallyData.vouchers && tallyData.vouchers.length > 0) {
      // Build a custom-voucher-type resolver so "Tax Invoice Mumbai" → "Sales" etc.
      // We fetch VoucherTypes from Tally once here and reuse for both sync steps.
      let voucherTypeResolver = null;
      try {
        const { fetchTallyVoucherTypes } = await import("../tally/tallyClient.js");
        const tallyCompany = creds._tallyCompanyName || companyName;
        const voucherTypes = await fetchTallyVoucherTypes(tallyCompany);
        voucherTypeResolver = buildVoucherTypeResolver(voucherTypes);
        logger.info("Voucher type resolver built — " + voucherTypes.length + " types mapped");
      } catch (e) {
        logger.warn("Could not build voucher type resolver — custom types will fall back to Journal Entry: " + e.message);
      }

      if (options.syncVouchers)
        await runStep("vouchers", () => syncVouchersToErpNext(tallyData.vouchers, companyName, creds, voucherTypeResolver));

      if (options.syncInvoices)
        await runStep("invoices", () => syncInvoicesToErpNext(tallyData.vouchers, companyName, creds, voucherTypeResolver));
    }

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

  // ── STATUS LOGIC ─────────────────────────────────────────────────────────────
  // A step gets status "fail" when it threw an exception (complete failure).
  // A step gets status "warn" when some items failed (e.g. 1 purchase invoice failed).
  //
  // FIX: Previously ANY "fail" step caused result.status = "failed", which blocked
  // saveCompanyState in server.js — so one bad purchase invoice reset the entire
  // incremental state and forced a full re-sync every time.
  //
  // New rule:
  //   "failed"  → a CRITICAL step threw completely (COA, vouchers, ERPNext unreachable)
  //   "warning" → only non-critical steps had partial item failures (invoices, ledgers)
  //   "ok"      → everything clean
  //
  // Critical steps: erpnextPing, chartOfAccounts, vouchers
  // Non-critical (partial ok): ledgers, openingBalances, godowns, costCentres, stockItems, invoices, taxes
  const CRITICAL_STEPS = new Set(["erpnextPing", "chartOfAccounts", "vouchers"]);
  const hasCriticalFail = Object.entries(result.steps).some(
    ([key, s]) => s.status === "fail" && CRITICAL_STEPS.has(key)
  );
  const hasAnyFail = Object.values(result.steps).some((s) => s.status === "fail");
  const hasWarn    = Object.values(result.steps).some((s) => s.status === "warn");
  result.status     = hasCriticalFail ? "failed" : (hasAnyFail || hasWarn) ? "warning" : "ok";
  result.finishedAt = new Date().toISOString();

  logger.info("Full sync complete: " + result.status, { company: companyName });
  return result;
}