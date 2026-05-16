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
import os   from "os";
import { logger } from "./logs/logger.js";

// ── ONLY THIS CHANGES: write to AppData so it's always writable ──────────────
// Always write to AppData — works correctly in Electron, PKG exe, and dev.
// process.pkg is only set by PKG bundler (not Electron), so we can't rely on it.
const STATE_DIR = path.join(
  os.homedir(), "AppData", "Roaming", "TallyERPNextIntegration", "data"
);

const STATE_FILE = path.join(STATE_DIR, "sync_state.dat");
const OVERLAP_DAYS = 3;

// ── File helpers ──────────────────────────────────────────────────────────────

function ensureDir() {
  if (!fs.existsSync(STATE_DIR)) fs.mkdirSync(STATE_DIR, { recursive: true });
}

function loadState() {
  ensureDir();
  if (!fs.existsSync(STATE_FILE)) return {};

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const raw = fs.readFileSync(STATE_FILE, "utf8");
      if (!raw.trim()) return {};
      return JSON.parse(raw);
    } catch (err) {
      if (attempt < 3) {
        const until = Date.now() + 50;
        while (Date.now() < until) { /* spin */ }
      } else {
        logger.warn(
          "syncState: could not parse sync_state.json after 3 attempts — starting fresh" +
          " (" + err.message + ")"
        );
        try {
          fs.copyFileSync(STATE_FILE, STATE_FILE + ".corrupt." + Date.now());
        } catch (_) { /* best-effort */ }
        return {};
      }
    }
  }
  return {};
}

function saveState(state) {
  ensureDir();
  const tmp = STATE_FILE + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2), "utf8");
  fs.renameSync(tmp, STATE_FILE);
}

// ── Composite key helper ──────────────────────────────────────────────────────

function stateKey(company, erpnextUrl) {
  const url = (erpnextUrl || "default").replace(/\/+$/, "").toLowerCase();
  return `${company}::${url}`;
}

// ── Public API ────────────────────────────────────────────────────────────────

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

export function saveCompanyState(company, partial, erpnextUrl) {
  const all  = loadState();
  const key  = stateKey(company, erpnextUrl);
  const prev = all[key] || {};
  all[key]   = Object.assign({}, prev, partial);

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

export function resetCompanyState(company, erpnextUrl) {
  const all = loadState();
  const key = stateKey(company, erpnextUrl);
  delete all[key];
  saveState(all);
  logger.info(`syncState: reset state for "${key}" — next sync will be full`);
}

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

export function buildAlterIdMap(items, keyField = "name") {
  const map = {};
  for (const item of items) {
    if (item[keyField] && item.alterId != null) {
      map[item[keyField]] = String(item.alterId);
    }
  }
  return map;
}