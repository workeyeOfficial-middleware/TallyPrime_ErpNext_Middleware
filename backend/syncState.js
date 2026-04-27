/**
 * syncState.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Manages persistent incremental sync state so every run only processes
 * new / changed data instead of re-syncing everything from scratch.
 *
 * State file location: <project_root>/data/sync_state.json
 *
 * FIX: State is now keyed by  "<company>::<erpnextUrl>"  instead of just
 * "<company>".  This means each ERPNext instance (URL) keeps its own
 * independent sync checkpoint.  Switching to a new ERPNext account no longer
 * reuses the old account's state and incorrectly reports "already up to date".
 *
 * Structure per company+url key:
 * {
 *   "Rajlaxmi Solutions::https://site.frappe.cloud": {
 *     lastVoucherSyncDate : "2026-04-21",
 *     lastMasterSyncAt    : "2026-04-21T...",
 *     ledgerAlterIds      : { "Cash": "42", ... },
 *     stockAlterIds       : { "Item A": "7", ... },
 *     groupAlterIds       : { "Sundry Debtors": "3", ... },
 *   }
 * }
 *
 * OVERLAP_DAYS (3) — re-sync the last 3 days of vouchers on every run.
 */

import fs   from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { logger } from "./logs/logger.js";

const __dirname  = path.dirname(fileURLToPath(import.meta.url));
const STATE_DIR  = path.join(__dirname, "data");
const STATE_FILE = path.join(STATE_DIR, "sync_state.json");
const OVERLAP_DAYS = 3;

// ── File helpers ──────────────────────────────────────────────────────────────

function ensureDir() {
  if (!fs.existsSync(STATE_DIR)) fs.mkdirSync(STATE_DIR, { recursive: true });
}

function loadState() {
  ensureDir();
  if (!fs.existsSync(STATE_FILE)) return {};
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
  } catch {
    logger.warn("syncState: could not parse sync_state.json — starting fresh");
    return {};
  }
}

function saveState(state) {
  ensureDir();
  const tmp = STATE_FILE + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2), "utf8");
  fs.renameSync(tmp, STATE_FILE);
}

// ── Composite key helper ──────────────────────────────────────────────────────

/**
 * stateKey(company, erpnextUrl)
 *
 * Builds a composite key that is unique per (Tally company, ERPNext instance).
 * Normalising the URL (strip trailing slash, lowercase) prevents accidental
 * duplicates from minor formatting differences.
 *
 * Examples:
 *   "Tally::https://site1.frappe.cloud"
 *   "Tally::https://site2.frappe.cloud"
 */
function stateKey(company, erpnextUrl) {
  const url = (erpnextUrl || "default").replace(/\/+$/, "").toLowerCase();
  return `${company}::${url}`;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * getCompanyState(company, erpnextUrl)
 * Returns the persisted state object for this company+url, or an empty default.
 */
export function getCompanyState(company, erpnextUrl) {
  const all = loadState();
  const key = stateKey(company, erpnextUrl);
  return all[key] || {
    lastVoucherSyncDate:  null,
    lastMasterSyncAt:     null,
    ledgerAlterIds:       {},
    stockAlterIds:        {},
    groupAlterIds:        {},
    costCentreAlterIds:   {},
    godownAlterIds:       {},
  };
}

/**
 * saveCompanyState(company, partial, erpnextUrl)
 * Merges `partial` into the stored state for this company+url.
 */
export function saveCompanyState(company, partial, erpnextUrl) {
  const all  = loadState();
  const key  = stateKey(company, erpnextUrl);
  const prev = all[key] || {};
  all[key]   = Object.assign({}, prev, partial);

  // Deep-merge alterIds maps so individual keys don't get wiped
  if (partial.ledgerAlterIds) {
    all[key].ledgerAlterIds = Object.assign({}, prev.ledgerAlterIds || {}, partial.ledgerAlterIds);
  }
  if (partial.stockAlterIds) {
    all[key].stockAlterIds = Object.assign({}, prev.stockAlterIds || {}, partial.stockAlterIds);
  }
  if (partial.groupAlterIds) {
    all[key].groupAlterIds = Object.assign({}, prev.groupAlterIds || {}, partial.groupAlterIds);
  }
  if (partial.costCentreAlterIds) {
    all[key].costCentreAlterIds = Object.assign({}, prev.costCentreAlterIds || {}, partial.costCentreAlterIds);
  }
  if (partial.godownAlterIds) {
    all[key].godownAlterIds = Object.assign({}, prev.godownAlterIds || {}, partial.godownAlterIds);
  }

  saveState(all);
  logger.info(`syncState: saved state for "${company}" → ${stateKey(company, erpnextUrl)}`, {
    lastVoucherSyncDate: all[key].lastVoucherSyncDate,
    lastMasterSyncAt:    all[key].lastMasterSyncAt,
  });
}

/**
 * resetCompanyState(company, erpnextUrl)
 * Clears all incremental state for a company+url — forces a full re-sync next run.
 */
export function resetCompanyState(company, erpnextUrl) {
  const all = loadState();
  const key = stateKey(company, erpnextUrl);
  delete all[key];
  saveState(all);
  logger.info(`syncState: reset state for "${key}" — next sync will be full`);
}

/**
 * getIncrementalVoucherDates(company, requestedFromDate, requestedToDate, erpnextUrl)
 *
 * Returns { fromDate, toDate, isIncremental } to use for the voucher fetch.
 *
 * Logic:
 *  - First ever sync           → use requestedFromDate / requestedToDate as-is
 *  - Subsequent syncs          → start from (lastVoucherSyncDate − OVERLAP_DAYS)
 *  - If user explicitly passed a fromDate earlier than our checkpoint
 *    (e.g. they want to re-sync a specific old range) → honour the user's date
 */
export function getIncrementalVoucherDates(company, requestedFromDate, requestedToDate, erpnextUrl) {
  const state = getCompanyState(company, erpnextUrl);
  const today = new Date().toISOString().slice(0, 10);
  const toDate = requestedToDate || today;

  if (!state.lastVoucherSyncDate) {
    return { fromDate: requestedFromDate || toDate, toDate, isIncremental: false };
  }

  const lastDate  = new Date(state.lastVoucherSyncDate);
  lastDate.setDate(lastDate.getDate() - OVERLAP_DAYS);
  const checkpoint = lastDate.toISOString().slice(0, 10);

  const fromDate = requestedFromDate && requestedFromDate < checkpoint
    ? requestedFromDate
    : checkpoint;

  const isIncremental = fromDate > (requestedFromDate || "1900-01-01") || !!state.lastVoucherSyncDate;

  logger.info(`syncState: incremental voucher window → ${fromDate} to ${toDate}` +
    ` (last sync was ${state.lastVoucherSyncDate}, overlap=${OVERLAP_DAYS}d)`);

  return { fromDate, toDate, isIncremental };
}

/**
 * filterChangedMasters(items, storedAlterIds, keyField = "name")
 *
 * Compares Tally items against the last-known alterIds map.
 * Returns { toSync: [...], unchanged: number }
 */
export function filterChangedMasters(items, storedAlterIds, keyField = "name") {
  if (!storedAlterIds || Object.keys(storedAlterIds).length === 0) {
    return { toSync: items, unchanged: 0 };
  }

  const toSync  = [];
  let unchanged = 0;

  for (const item of items) {
    const key     = item[keyField];
    const alterId = item.alterId != null ? String(item.alterId) : null;

    if (alterId === null) { toSync.push(item); continue; }
    if (!(key in storedAlterIds)) { toSync.push(item); continue; }
    if (storedAlterIds[key] !== alterId) { toSync.push(item); continue; }

    unchanged++;
  }

  return { toSync, unchanged };
}

/**
 * buildAlterIdMap(items, keyField = "name")
 * Builds a { name → alterId } map from a list of Tally master objects.
 */
export function buildAlterIdMap(items, keyField = "name") {
  const map = {};
  for (const item of items) {
    if (item[keyField] && item.alterId != null) {
      map[item[keyField]] = String(item.alterId);
    }
  }
  return map;
}