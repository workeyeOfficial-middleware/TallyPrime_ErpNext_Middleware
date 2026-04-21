import fs from "fs";
import path from "path";

const LOGS_DIR   = path.resolve("./logs");
const MAX_DAYS   = 15;        // auto-delete logs older than 15 days
const MAX_MEMORY = 1000;      // cap in-memory entries per company bucket

// ── Ensure ./logs/ directory exists ──────────────────────────────────────────
fs.mkdirSync(LOGS_DIR, { recursive: true });

// ── Helpers ───────────────────────────────────────────────────────────────────

// "Rajlaxmi Solutions Pvt Ltd" → "rajlaxmi-solutions-pvt-ltd.log"
function companyToFilename(company) {
  if (!company) return "_global.log";
  return (
    company
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 80) +
    ".log"
  );
}

function logFilePath(company) {
  return path.join(LOGS_DIR, companyToFilename(company));
}

// ── Per-company in-memory cache  (Map<bucket, Entry[]>) ──────────────────────
const cache = new Map();

function loadBucket(bucket) {
  if (cache.has(bucket)) return cache.get(bucket);
  const file = logFilePath(bucket);
  let entries = [];
  try {
    if (fs.existsSync(file)) entries = JSON.parse(fs.readFileSync(file, "utf-8"));
  } catch { entries = []; }
  cache.set(bucket, entries);
  return entries;
}

function saveBucket(bucket, entries) {
  try { fs.writeFileSync(logFilePath(bucket), JSON.stringify(entries, null, 2)); }
  catch { /* never crash over log writes */ }
}

function pruneEntries(entries) {
  const cutoff = Date.now() - MAX_DAYS * 24 * 60 * 60 * 1000;
  return entries.filter(e => new Date(e.ts).getTime() > cutoff);
}

// ── Core writer ───────────────────────────────────────────────────────────────
function addLog(level, message, meta = {}) {
  const company = meta?.company || null;

  const entry = {
    id:      Date.now() + Math.random(),
    ts:      new Date().toISOString(),
    level,
    message,
    meta,
    company,   // top-level for fast filtering
  };

  // Write to company-specific bucket
  const bucket = company || "_global";
  let rows = loadBucket(bucket);
  rows.unshift(entry);
  if (rows.length > MAX_MEMORY) rows.splice(MAX_MEMORY);
  rows = pruneEntries(rows);
  cache.set(bucket, rows);
  saveBucket(bucket, rows);

  // Also write to _global so admin can see everything in one place
  if (bucket !== "_global") {
    let global = loadBucket("_global");
    global.unshift(entry);
    if (global.length > MAX_MEMORY) global.splice(MAX_MEMORY);
    global = pruneEntries(global);
    cache.set("_global", global);
    saveBucket("_global", global);
  }

  // Console output
  const tag = level === "error" ? "✗" : level === "warn" ? "⚠" : level === "success" ? "✓" : "·";
  console.log(`[${entry.ts.slice(11, 19)}] ${tag} ${message}`, Object.keys(meta).length ? meta : "");

  return entry;
}

// ── Query ─────────────────────────────────────────────────────────────────────
/**
 * getLogs(options)
 *   company  — filter to one company (null = all)
 *   fromDate — "YYYY-MM-DD" inclusive
 *   toDate   — "YYYY-MM-DD" inclusive
 *   level    — "info"|"success"|"warn"|"error" (null = all)
 *   limit    — default 200
 */
function getLogs({ company = null, fromDate = null, toDate = null, level = null, limit = 200 } = {}) {
  // Legacy compat: if called as getLogs(100) (plain number), treat as limit
  if (typeof arguments[0] === "number") return getLogs({ limit: arguments[0] });

  const bucket = company || "_global";
  let entries = loadBucket(bucket);

  if (fromDate) {
    const from = new Date(fromDate).getTime();
    entries = entries.filter(e => new Date(e.ts).getTime() >= from);
  }
  if (toDate) {
    const to = new Date(toDate).getTime() + 86_400_000; // include full day
    entries = entries.filter(e => new Date(e.ts).getTime() < to);
  }
  if (level) {
    entries = entries.filter(e => e.level === level);
  }

  return entries.slice(0, limit);
}

/** List all companies that have log files (for admin/tenant selector) */
function listCompanies() {
  try {
    return fs.readdirSync(LOGS_DIR)
      .filter(f => f.endsWith(".log") && f !== "_global.log")
      .map(f => f.replace(/\.log$/, ""));
  } catch { return []; }
}

/** Prune expired entries from every log file */
function pruneAllLogs() {
  try {
    const files = fs.readdirSync(LOGS_DIR).filter(f => f.endsWith(".log"));
    for (const file of files) {
      const bucket = file.replace(/\.log$/, "");
      let entries = loadBucket(bucket);
      const before = entries.length;
      entries = pruneEntries(entries);
      if (entries.length !== before) {
        cache.set(bucket, entries);
        saveBucket(bucket, entries);
      }
    }
  } catch { /* silent */ }
}

// Prune on startup + every 6 hours
pruneAllLogs();
setInterval(pruneAllLogs, 6 * 60 * 60 * 1000);

// ── Public API ────────────────────────────────────────────────────────────────
export const logger = {
  info:    (msg, meta) => addLog("info",    msg, meta),
  warn:    (msg, meta) => addLog("warn",    msg, meta),
  error:   (msg, meta) => addLog("error",   msg, meta),
  success: (msg, meta) => addLog("success", msg, meta),

  summary: (companyName, dateFrom, dateTo, counts = {}) =>
    addLog("success", `Sync summary — ${companyName} (${dateFrom} → ${dateTo})`, {
      type: "sync_summary", company: companyName,
      date_from: dateFrom, date_to: dateTo,
      ...counts, synced_at: new Date().toISOString(),
    }),

  getLogs,
  listCompanies,
  pruneAllLogs,

  getSummaries: (company = null) =>
    getLogs({ company, limit: 500 }).filter(e => e.meta?.type === "sync_summary"),

  clear: (company = null) => {
    const bucket = company || "_global";
    cache.set(bucket, []);
    saveBucket(bucket, []);
  },
};