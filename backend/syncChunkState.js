/**
 * syncChunkState.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Tracks chunk-level progress for large first-time syncs.
 *
 * Multi-tenant: keyed by "<company>::<erpnextUrl>" — same pattern as syncState.js
 * So each (Tally company + ERPNext account) pair has its own independent
 * chunk progress. Switching ERPNext accounts never reuses another account's state.
 *
 * Chunk progress file: data/chunk_progress.dat
 *
 * Structure:
 * {
 *   "Rajlaxmi::https://site1.frappe.cloud": {
 *     fullSyncComplete: false,
 *     vouchers: {
 *       "2016-04": { from: "2016-04-01", to: "2016-04-30", count: 45, doneAt: "..." },
 *       "2016-05": { from: "2016-05-01", to: "2016-05-31", count: 82, doneAt: "..." },
 *     },
 *     ledgers: {
 *       "batch-0": { from: 0, to: 500, count: 500, doneAt: "..." },
 *       "batch-1": { from: 500, to: 1000, count: 500, doneAt: "..." },
 *     }
 *   },
 *   "OtherCompany::https://site2.frappe.cloud": { ... }
 * }
 */

import fs   from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { logger } from "./logs/logger.js";

const __dirname   = path.dirname(fileURLToPath(import.meta.url));
const STATE_DIR   = path.join(__dirname, "data");
const CHUNK_FILE  = path.join(STATE_DIR, "chunk_progress.dat");

// ── File helpers (same pattern as syncState.js) ───────────────────────────────

function ensureDir() {
  if (!fs.existsSync(STATE_DIR)) fs.mkdirSync(STATE_DIR, { recursive: true });
}

function loadChunkState() {
  ensureDir();
  if (!fs.existsSync(CHUNK_FILE)) return {};
  try {
    const raw = fs.readFileSync(CHUNK_FILE, "utf8");
    if (!raw.trim()) return {};
    return JSON.parse(raw);
  } catch (err) {
    logger.warn("syncChunkState: could not parse chunk_progress.dat — starting fresh (" + err.message + ")");
    try {
      fs.copyFileSync(CHUNK_FILE, CHUNK_FILE + ".corrupt." + Date.now());
    } catch (_) { /* best-effort */ }
    return {};
  }
}

function saveChunkState(state) {
  ensureDir();
  const tmp = CHUNK_FILE + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2), "utf8");
  fs.renameSync(tmp, CHUNK_FILE);
}

// ── Composite key — same pattern as syncState.js ──────────────────────────────
// Key: "<companyName>::<erpnextUrl>"
// Examples:
//   "Rajlaxmi Solutions::https://site1.frappe.cloud"
//   "Rajlaxmi Solutions::https://site2.frappe.cloud"  ← different ERPNext = separate state
//   "OtherCompany::https://site1.frappe.cloud"        ← different Tally company = separate state

function stateKey(company, erpnextUrl) {
  const url = (erpnextUrl || "default").replace(/\/+$/, "").toLowerCase();
  return `${company}::${url}`;
}

// ── Get full state for a tenant ───────────────────────────────────────────────

function getTenantState(company, erpnextUrl) {
  const all = loadChunkState();
  const key = stateKey(company, erpnextUrl);
  return all[key] || {
    fullSyncComplete: false,
    startedAt:        null,
    vouchers:         {},
    ledgers:          {},
  };
}

function saveTenantState(company, erpnextUrl, tenantState) {
  const all = loadChunkState();
  const key = stateKey(company, erpnextUrl);
  all[key]  = tenantState;
  saveChunkState(all);
}

// ── PUBLIC API ────────────────────────────────────────────────────────────────

/**
 * isFullSyncComplete(company, erpnextUrl)
 * Returns true if the first full sync has ever completed for this tenant.
 * Once true → all subsequent syncs are incremental.
 */
export function isFullSyncComplete(company, erpnextUrl) {
  return getTenantState(company, erpnextUrl).fullSyncComplete === true;
}

/**
 * markFullSyncComplete(company, erpnextUrl)
 * Called once ALL chunks (vouchers + ledgers) are done.
 * Next sync will be incremental.
 */
export function markFullSyncComplete(company, erpnextUrl) {
  const state = getTenantState(company, erpnextUrl);
  state.fullSyncComplete = true;
  state.completedAt      = new Date().toISOString();
  saveTenantState(company, erpnextUrl, state);
  logger.info(`syncChunkState: full sync marked complete for "${stateKey(company, erpnextUrl)}"`);
}

/**
 * isChunkDone(company, erpnextUrl, dataType, chunkId)
 * dataType = "vouchers" | "ledgers"
 * chunkId  = "2016-04" for vouchers, "batch-0" for ledgers
 */
export function isChunkDone(company, erpnextUrl, dataType, chunkId) {
  const state = getTenantState(company, erpnextUrl);
  return !!(state[dataType]?.[chunkId]?.doneAt);
}

/**
 * markChunkDone(company, erpnextUrl, dataType, chunkId, meta)
 * Saves progress after each chunk completes.
 * meta = { from, to, count } for vouchers
 *        { from, to, count } for ledgers (from/to are batch index numbers)
 */
export function markChunkDone(company, erpnextUrl, dataType, chunkId, meta = {}) {
  const state = getTenantState(company, erpnextUrl);
  if (!state[dataType]) state[dataType] = {};
  if (!state.startedAt)  state.startedAt = new Date().toISOString();

  state[dataType][chunkId] = {
    ...meta,
    doneAt: new Date().toISOString(),
  };

  saveTenantState(company, erpnextUrl, state);
  logger.info(
    `syncChunkState: chunk done — ${stateKey(company, erpnextUrl)} / ${dataType} / ${chunkId}`,
    meta
  );
}

/**
 * getChunkProgress(company, erpnextUrl)
 * Returns a summary of chunk progress for this tenant.
 * Useful for a progress UI or /api/sync/chunk-progress endpoint.
 */
export function getChunkProgress(company, erpnextUrl) {
  const state   = getTenantState(company, erpnextUrl);
  const vDone   = Object.keys(state.vouchers || {}).length;
  const lDone   = Object.keys(state.ledgers  || {}).length;
  return {
    fullSyncComplete: state.fullSyncComplete || false,
    startedAt:        state.startedAt        || null,
    completedAt:      state.completedAt      || null,
    vouchers: {
      chunksCompleted: vDone,
      chunks:          state.vouchers || {},
    },
    ledgers: {
      chunksCompleted: lDone,
      chunks:          state.ledgers  || {},
    },
  };
}

/**
 * resetChunkProgress(company, erpnextUrl)
 * Clears ALL chunk progress for this tenant.
 * Next sync will start from scratch (full sync again).
 * Use this when user explicitly wants to re-sync everything.
 */
export function resetChunkProgress(company, erpnextUrl) {
  const all = loadChunkState();
  const key = stateKey(company, erpnextUrl);
  delete all[key];
  saveChunkState(all);
  logger.info(`syncChunkState: reset chunk progress for "${key}" — next sync will be full`);
}

/**
 * listAllTenants()
 * Returns all (company, erpnextUrl) pairs that have chunk progress.
 * Useful for admin/debug endpoint.
 */
export function listAllTenants() {
  const all = loadChunkState();
  return Object.keys(all).map((key) => {
    const [company, url] = key.split("::");
    return {
      key,
      company,
      erpnextUrl:       url,
      fullSyncComplete: all[key].fullSyncComplete || false,
      startedAt:        all[key].startedAt        || null,
      completedAt:      all[key].completedAt       || null,
    };
  });
}