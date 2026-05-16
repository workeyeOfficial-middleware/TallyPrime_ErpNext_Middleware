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

const CHUNK_FILE  = path.join(STATE_DIR, "chunk_progress.dat");

// ── File helpers ───────────────────────────────────────────────────────────────

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

// ── Composite key ──────────────────────────────────────────────────────────────

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

export function isFullSyncComplete(company, erpnextUrl) {
  return getTenantState(company, erpnextUrl).fullSyncComplete === true;
}

export function markFullSyncComplete(company, erpnextUrl) {
  const state = getTenantState(company, erpnextUrl);
  state.fullSyncComplete = true;
  state.completedAt      = new Date().toISOString();
  saveTenantState(company, erpnextUrl, state);
  logger.info(`syncChunkState: full sync marked complete for "${stateKey(company, erpnextUrl)}"`);
}

export function isChunkDone(company, erpnextUrl, dataType, chunkId) {
  const state = getTenantState(company, erpnextUrl);
  return !!(state[dataType]?.[chunkId]?.doneAt);
}

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

export function resetChunkProgress(company, erpnextUrl) {
  const all = loadChunkState();
  const key = stateKey(company, erpnextUrl);
  delete all[key];
  saveChunkState(all);
  logger.info(`syncChunkState: reset chunk progress for "${key}" — next sync will be full`);
}

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