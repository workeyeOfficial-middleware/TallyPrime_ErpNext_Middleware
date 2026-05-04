import express from "express";
import cors from "cors";
import { config } from "./config/config.js";
import router from "./routes/index.js";
import { logger } from "./logs/logger.js";
import {
  fetchTallyLedgers,
  fetchTallyStockItems,
  fetchTallyVouchers,
  fetchTallyVouchersChunked,
  fetchTallyLedgersChunked,
  fetchTallyGroups,
  fetchTallyGodowns,
  fetchTallyCostCentres,
  fetchTallyCompanies,
} from "./tally/tallyClient.js";
import { runFullSync, smartSyncLedgersToErpNext } from "./tally/Erpnextclient.js";
import {
  getCompanyState,
  saveCompanyState,
  getIncrementalVoucherDates,
  filterChangedMasters,
  buildAlterIdMap,
  resetCompanyState,
} from "./syncState.js";
import {
  isFullSyncComplete,
  markFullSyncComplete,
  markChunkDone,
} from "./syncChunkState.js";

const app = express();

app.use(cors());
app.use(express.json());
app.use("/api", router);

// ─────────────────────────────────────────────────────────────────────────────
// AUTO-SYNC CONFIG — persisted to data/auto_sync_config.json
// Survives server restarts and nodemon reloads.
// The user sets it via POST /api/auto-sync/configure.
// ─────────────────────────────────────────────────────────────────────────────
import fs   from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname      = path.dirname(fileURLToPath(import.meta.url));

// DATA_DIR lives OUTSIDE the project folder so nodemon never watches it.
// Default: a sibling folder "<project>-data" next to your project root.
// Override by setting DATA_DIR in your .env file.
const DATA_DIR_DEFAULT = path.join(__dirname, "..", "tally-erp-data");
const CONFIG_DIR     = process.env.DATA_DIR || DATA_DIR_DEFAULT;
const CONFIG_FILE    = path.join(CONFIG_DIR, "auto_sync_config.json");
const CREDS_FILE     = path.join(CONFIG_DIR, "auto_sync_creds.json");  // gitignore this

const DEFAULT_CONFIG = {
  enabled:       false,
  // FIX: Read from .env instead of hardcoding 24h / 30 days.
  // Set AUTO_SYNC_INTERVAL_MINUTES and AUTO_SYNC_FROM_DAYS in your .env file.
  intervalMs:    (parseInt(process.env.AUTO_SYNC_INTERVAL_MINUTES, 10) || 1440) * 60 * 1000,
  intervalLabel: process.env.AUTO_SYNC_INTERVAL_MINUTES ? `${process.env.AUTO_SYNC_INTERVAL_MINUTES}m` : "daily",
  options: {
    syncChartOfAccounts: false,
    syncLedgers:         true,
    syncSmartLedgers:    false,
    syncOpeningBalances: false,
    syncGodowns:         false,
    syncCostCentres:     false,
    syncStock:           true,
    syncVouchers:        true,
    syncInvoices:        false,
    syncTaxes:           false,
  },
  creds: {
    url:            "",
    apiKey:         "",
    apiSecret:      "",
    erpnextCompany: "",
  },
  companyName: "",
  // FIX: Read fromDays from .env — not hardcoded to 30
  fromDays:    parseInt(process.env.AUTO_SYNC_FROM_DAYS, 10) || 30,
};

function loadAutoSyncConfig() {
  try {
    if (!fs.existsSync(CONFIG_FILE)) return { ...DEFAULT_CONFIG };
    const raw = fs.readFileSync(CONFIG_FILE, "utf8");
    const saved = JSON.parse(raw);
    // Load saved creds separately (apiKey + apiSecret live in their own file)
    let savedCreds = {};
    try {
      if (fs.existsSync(CREDS_FILE)) {
        savedCreds = JSON.parse(fs.readFileSync(CREDS_FILE, "utf8"));
        logger.info("Auto-sync: loaded ERPNext credentials from creds file");
      }
    } catch (e) {
      logger.warn("Could not load auto-sync creds file: " + e.message);
    }
    return {
      ...DEFAULT_CONFIG,
      ...saved,
      options: { ...DEFAULT_CONFIG.options, ...(saved.options || {}) },
      creds:   { ...DEFAULT_CONFIG.creds, ...(saved.creds || {}), ...savedCreds },
    };
  } catch (err) {
    logger.warn("Could not load auto-sync config — using defaults. (" + err.message + ")");
    return { ...DEFAULT_CONFIG };
  }
}

// ── Debounced persist — prevents rapid consecutive writes from triggering
// multiple nodemon reloads. Also add  "ignore": ["data/*"]  to your
// nodemon.json (or package.json nodemonConfig) to stop the restart loop.
let _persistTimer = null;
function persistAutoSyncConfig(immediate = false) {
  if (_persistTimer) { clearTimeout(_persistTimer); _persistTimer = null; }
  const doWrite = () => {
    _persistTimer = null;
    _writeAutoSyncConfig();
  };
  if (immediate) { doWrite(); return; }
  _persistTimer = setTimeout(doWrite, 500); // batch writes within 500ms
}
function _writeAutoSyncConfig() {
  try {
    if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR, { recursive: true });
    // Main config — no secrets
    const toSave = {
      ..._autoSyncConfig,
      creds: {
        url:            _autoSyncConfig.creds.url,
        erpnextCompany: _autoSyncConfig.creds.erpnextCompany,
      },
    };
    const tmp = CONFIG_FILE + ".bak";
    fs.writeFileSync(tmp, JSON.stringify(toSave, null, 2), "utf8");
    fs.renameSync(tmp, CONFIG_FILE);
    // Creds file — saved separately so scheduler survives server restarts
    // Add data/auto_sync_creds.json to your .gitignore
    if (_autoSyncConfig.creds.apiKey && _autoSyncConfig.creds.apiSecret) {
      const credsToSave = {
        url:            _autoSyncConfig.creds.url,
        apiKey:         _autoSyncConfig.creds.apiKey,
        apiSecret:      _autoSyncConfig.creds.apiSecret,
        erpnextCompany: _autoSyncConfig.creds.erpnextCompany,
      };
      fs.writeFileSync(CREDS_FILE, JSON.stringify(credsToSave, null, 2), "utf8");
    }
  } catch (err) {
    logger.warn("Could not save auto-sync config: " + err.message);
  }
} // end _writeAutoSyncConfig

// Load persisted config on startup — restores enabled/interval/options/company
// across nodemon restarts and production server restarts
let _autoSyncConfig = loadAutoSyncConfig();

let _autoSyncTimer   = null;   // setInterval handle
let _syncRunning     = false;  // concurrency guard
let _lastAutoSync    = null;   // last run result, served by /status
let _nextRunAt       = null;   // timestamp of next scheduled run, exposed in /status
let _autoSyncRunCount = 0;     // monotonically incrementing run counter for log messages

// ─────────────────────────────────────────────────────────────────────────────
// CORE AUTO-SYNC RUNNER
// This is intentionally identical to what a manual sync route does:
//   1. Resolve fromDate/toDate (incremental)
//   2. Fetch only changed masters
//   3. Fetch vouchers for the date window
//   4. Call runFullSync (same function manual sync calls)
//   5. Save state on success/warning
// ─────────────────────────────────────────────────────────────────────────────
async function runAutoSync(triggeredBy = "interval") {
  // ── Concurrency guard ────────────────────────────────────────────────────
  if (_syncRunning) {
    logger.human.headsUp("Auto-sync skipped — previous run is still in progress.");
    return;
  }

  const { companyName, options, creds, fromDays } = _autoSyncConfig;

  // ── Pre-flight checks (same as manual sync would fail fast on) ───────────
  logger.info(`[auto-sync] Pre-flight check — companyName="${companyName}" erpUrl="${creds.url || config.erpnext.url}" hasKey=${!!(creds.apiKey || config.erpnext.apiKey)}`);

  if (!companyName) {
    logger.human.headsUp("Auto-sync skipped — no Tally company name is set. Please go to Sync settings and enter your company name.");
    return;
  }
  const erpUrl = creds.url || config.erpnext.url;
  const erpKey = creds.apiKey || config.erpnext.apiKey;
  if (!erpUrl || !erpKey) {
    logger.human.headsUp("Auto-sync skipped — ERPNext connection details are missing. Please go to Sync settings and enter your ERPNext URL and API key.");
    return;
  }

  _syncRunning = true;
  _autoSyncRunCount++;
  const _runNum = _autoSyncRunCount;

  const now    = new Date();
  const toDate = now.toISOString().slice(0, 10);

  // Merge runtime creds with .env fallbacks — identical to manual sync
  const effectiveCreds = {
    url:            erpUrl,
    apiKey:         erpKey,
    apiSecret:      creds.apiSecret || config.erpnext.apiSecret,
    erpnextCompany: creds.erpnextCompany || "",
  };

  const erpnextUrl  = effectiveCreds.url || "default";
  const state       = getCompanyState(companyName, erpnextUrl);
  const isFirstSync = !state.lastVoucherSyncDate && !state.lastMasterSyncAt;

  // ── Determine fallback fromDate for first-ever sync ───────────────────────
  // Priority:
  //   1. Tally company's booksFrom date  (the actual opening of books — correct for new ERPNext)
  //   2. fromDays setting                (user preference, e.g. "last 30 days")
  //   3. today - 30 days                 (safe hard fallback)
  //
  // This is what was broken: on a fresh ERPNext account the old code used
  // today - fromDays (e.g. 3 days), so it missed all historical vouchers.
  // Manual sync works because the user explicitly picks a fromDate.
  // Now auto-sync does the same thing automatically on first run.
  let fallbackFromDate;
  if (isFirstSync) {
    try {
      const companies = await fetchTallyCompanies();
      const match = companies.find(
        (c) => c.name && c.name.trim().toLowerCase() === companyName.trim().toLowerCase()
      ) || companies[0];
      if (match) {
        // startingFrom = the date the company was first created in Tally (covers full history).
        // booksFrom    = current year's opening date only — misses prior-year vouchers.
        // Always prefer startingFrom so a state reset doesn't silently skip historical data.
        const companyStart = match.startingFrom || match.booksFrom;
        if (companyStart) {
          fallbackFromDate = companyStart;
          logger.human.step(`First sync — using Tally company start date: ${fallbackFromDate}`);
        }
      }
    } catch (e) {
      logger.warn(`Could not fetch Tally company start date: ${e.message}`);
    }
  }
  if (!fallbackFromDate) {
    // Not first sync OR company-date fetch failed — use fromDays setting
    const d = new Date(now);
    d.setDate(d.getDate() - fromDays);
    fallbackFromDate = d.toISOString().slice(0, 10);
  }

  // ── Log the trigger so Live Logs shows the run starting immediately ──
  logger.human.autoSyncTriggered(companyName, _autoSyncConfig.intervalLabel, _runNum);
  logger.human.syncStarted(companyName, effectiveCreds.url);
  _lastAutoSync = {
    startedAt:    now.toISOString(),
    status:       "running",
    company:      companyName,
    triggeredBy,
    fromDate:     state.lastVoucherSyncDate || fallbackFromDate,
    toDate,
    isIncremental: !isFirstSync,
  };

  try {
    // ── 1. MASTERS — fetch all, sync only changed (same as manual) ──────────
    let groups      = [];
    let costCentres = [];
    let godowns     = [];
    let ledgers     = [];
    let stockItems  = [];

    if (options.syncChartOfAccounts || options.syncOpeningBalances) {
      logger.human.syncStepStarting("Chart of Accounts — reading account groups from Tally");
      const allGroups = await fetchTallyGroups(companyName);
      const { toSync: changedGroups, unchanged: unchangedGroups } =
        filterChangedMasters(allGroups, state.groupAlterIds);
      logger.human.masterSyncResult("Groups", changedGroups.length, unchangedGroups);
      if (changedGroups.length === 0) logger.human.syncStepSkipped("Chart of Accounts", unchangedGroups);
      groups = changedGroups;
      saveCompanyState(companyName, { groupAlterIds: buildAlterIdMap(allGroups) }, erpnextUrl);
    }

    if (options.syncCostCentres) {
      logger.human.syncStepStarting("Cost Centres — reading from Tally");
      const allCostCentres = await fetchTallyCostCentres(companyName);
      const { toSync: changedCostCentres, unchanged: unchangedCostCentres } =
        filterChangedMasters(allCostCentres, state.costCentreAlterIds);
      logger.human.masterSyncResult("Cost Centres", changedCostCentres.length, unchangedCostCentres);
      if (changedCostCentres.length === 0) logger.human.syncStepSkipped("Cost Centres", unchangedCostCentres);
      costCentres = changedCostCentres;
      saveCompanyState(companyName, { costCentreAlterIds: buildAlterIdMap(allCostCentres) }, erpnextUrl);
    }

    if (options.syncGodowns) {
      logger.human.syncStepStarting("Godowns — reading warehouses from Tally");
      const allGodowns = await fetchTallyGodowns(companyName);
      const { toSync: changedGodowns, unchanged: unchangedGodowns } =
        filterChangedMasters(allGodowns, state.godownAlterIds);
      logger.human.masterSyncResult("Godowns", changedGodowns.length, unchangedGodowns);
      if (changedGodowns.length === 0) logger.human.syncStepSkipped("Godowns", unchangedGodowns);
      godowns = changedGodowns;
      saveCompanyState(companyName, { godownAlterIds: buildAlterIdMap(allGodowns) }, erpnextUrl);
    }

    if (options.syncLedgers || options.syncOpeningBalances || options.syncSmartLedgers) {
      const stepLabel = options.syncSmartLedgers && !options.syncLedgers
        ? "Smart Ledgers — reading all ledgers from Tally (will filter by voucher usage)"
        : "Ledgers — reading from Tally";
      logger.human.syncStepStarting(stepLabel);
      const allLedgers = await fetchTallyLedgers(companyName);
      const { toSync: changedLedgers, unchanged: unchangedLedgers } =
        filterChangedMasters(allLedgers, state.ledgerAlterIds);
      logger.human.masterSyncResult("Ledgers", changedLedgers.length, unchangedLedgers);
      if (changedLedgers.length === 0 && !options.syncSmartLedgers)
        logger.human.syncStepSkipped("Ledgers", unchangedLedgers);
      // For smart-ledger-only mode pass the full list so runFullSync filters by
      // voucher usage. For regular ledger sync pass only changed ones (incremental).
      ledgers = (options.syncSmartLedgers && !options.syncLedgers) ? allLedgers : changedLedgers;
      saveCompanyState(companyName, { ledgerAlterIds: buildAlterIdMap(allLedgers) }, erpnextUrl);
    }

    if (options.syncStock || options.syncTaxes) {
      logger.human.syncStepStarting("Stock Items — reading from Tally");
      const allStock = await fetchTallyStockItems(companyName);
      const { toSync: changedStock, unchanged: unchangedStock } =
        filterChangedMasters(allStock, state.stockAlterIds);
      logger.human.masterSyncResult("Stock Items", changedStock.length, unchangedStock);
      if (changedStock.length === 0) logger.human.syncStepSkipped("Stock Items", unchangedStock);
      stockItems = changedStock;
      saveCompanyState(companyName, { stockAlterIds: buildAlterIdMap(allStock) }, erpnextUrl);
    }

    // ── 2. VOUCHERS — incremental or first-sync chunked ─────────────────────
    let vouchers = [];
    if (options.syncVouchers || options.syncInvoices || options.syncSmartLedgers) {
      const fullSyncDone  = isFullSyncComplete(companyName, erpnextUrl);
      const needsFullSync = isFirstSync || !fullSyncDone;

      if (needsFullSync) {
        // First sync — fetch ALL vouchers in monthly chunks, resumable
        // Use company start date already resolved above in fallbackFromDate
        const chunkFrom = fallbackFromDate;
        const chunkTo   = toDate;
        logger.human.syncStepStarting(`Vouchers (FIRST SYNC) — reading monthly chunks from ${chunkFrom} to ${chunkTo}`);
        vouchers = await fetchTallyVouchersChunked(
          companyName,
          chunkFrom,
          chunkTo,
          erpnextUrl,
          (chunkId, count, totalChunks, idx, skipped) => {
            logger.human.step(
              `Voucher chunk ${chunkId} (${idx}/${totalChunks}): ` +
              `${skipped ? "already synced, skipping" : count + " vouchers fetched"}`
            );
          }
        );
        logger.human.step(`First sync: ${vouchers.length} total vouchers fetched across all chunks`);
      } else {
        // Incremental — only new vouchers since last sync
        const { fromDate, toDate: vToDate } =
          getIncrementalVoucherDates(companyName, fallbackFromDate, toDate, erpnextUrl);
        logger.human.syncStepStarting(`Vouchers — reading from ${fromDate} to ${vToDate}`);
        vouchers = await fetchTallyVouchers(companyName, fromDate, vToDate);
        logger.human.step(`Read ${vouchers.length} voucher${vouchers.length !== 1 ? "s" : ""} from Tally (${fromDate} → ${vToDate})`);
      }
    }

    // ── 3. Run the sync — EXACT same call as manual sync ────────────────────
    const result = await runFullSync(
      companyName,
      { groups, ledgers, stockItems, vouchers, godowns, costCentres },
      options,
      effectiveCreds
    );

    // ── 4. Save incremental state (same logic as manual) ────────────────────
    const shouldSaveState = result.status === "ok" || result.status === "warning";
    if (shouldSaveState) {
      saveCompanyState(companyName, {
        lastVoucherSyncDate: toDate,
        lastMasterSyncAt:    now.toISOString(),
      }, erpnextUrl);
      logger.human.stateSaved(companyName);

      // If this was the first full chunked sync, mark it complete so next run is incremental
      const fullSyncDone  = isFullSyncComplete(companyName, erpnextUrl);
      const needsFullSync = isFirstSync || !fullSyncDone;
      if (needsFullSync && (options.syncVouchers || options.syncInvoices)) {
        markFullSyncComplete(companyName, erpnextUrl);
        logger.human.step(`Full sync complete — future auto-syncs for "${companyName}" will be incremental ✅`);
      }
    } else {
      logger.human.headsUp(
        `Progress was not saved for "${companyName}" because a critical step failed. ` +
        `Fix the issue and run again.`
      );
    }

    _lastAutoSync = {
      ...result,
      steps:        result.steps || {},
      triggeredBy,
      fromDate:     state.lastVoucherSyncDate || fallbackFromDate,
      toDate,
      isIncremental: !isFirstSync,
    };

    logger.human.syncDone(
      companyName,
      state.lastVoucherSyncDate || fallbackFromDate,
      toDate,
      result.totalSynced ?? 0
    );
    logger.human.autoSyncRunSummary(
      companyName,
      state.lastVoucherSyncDate || fallbackFromDate,
      toDate,
      result.status || "ok",
      _runNum
    );

  } catch (err) {
    _lastAutoSync = {
      status:    "failed",
      error:     err.message,
      startedAt: now.toISOString(),
      company:   companyName,
      fromDate:  state.lastVoucherSyncDate || fallbackFromDate,
      toDate,
      triggeredBy,
    };
    logger.human.syncFailed(companyName, err.message);
  } finally {
    _syncRunning = false;
    // Log when the next run is scheduled so Live Logs shows the countdown
    if (_autoSyncConfig.enabled && _nextRunAt) {
      logger.human.autoSyncNextScheduled(
        _nextRunAt.toLocaleTimeString("en-IN", { hour12: false }),
        _autoSyncConfig.intervalLabel
      );
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SCHEDULER HELPERS
// ─────────────────────────────────────────────────────────────────────────────
function stopScheduler() {
  if (_autoSyncTimer) {
    clearInterval(_autoSyncTimer);
    _autoSyncTimer = null;
  }
}

function startScheduler(silent = false) {
  stopScheduler();
  if (!_autoSyncConfig.enabled) return;
  _nextRunAt = new Date(Date.now() + _autoSyncConfig.intervalMs);
  _autoSyncTimer = setInterval(() => { _nextRunAt = new Date(Date.now() + _autoSyncConfig.intervalMs); runAutoSync("interval"); }, _autoSyncConfig.intervalMs);
  if (!silent) {
    logger.human.autoSyncScheduled(
      Object.entries(_autoSyncConfig.options)
        .filter(([, v]) => v)
        .map(([k]) => k)
        .join(", "),
      _autoSyncConfig.intervalLabel
    );
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// AUTO-SYNC API ENDPOINTS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * POST /api/auto-sync/configure
 *
 * Set everything the scheduler needs — mirrors the manual Sync UI fields.
 *
 * Body (all fields optional, merged into current config):
 * {
 *   enabled:      true,
 *   interval:     "6h" | "12h" | "24h" | "48h" | number (minutes),
 *   companyName:  "My Tally Company",
 *   fromDays:     30,
 *   options: {
 *     syncLedgers: true, syncStock: true, syncVouchers: true, ...
 *   },
 *   creds: {
 *     url:            "https://myerpnext.frappe.cloud",
 *     apiKey:         "xxx",
 *     apiSecret:      "yyy",
 *     erpnextCompany: "My ERPNext Company"
 *   }
 * }
 */
app.post("/api/auto-sync/configure", (req, res) => {
  const body = req.body || {};

  // Parse interval — accepts human labels (from the UI buttons) or raw minutes
  if (body.interval !== undefined) {
    const PRESETS = {
      // UI button labels → minutes
      "15 min": 15,  "15min": 15,  "15m": 15,
      "30 min": 30,  "30min": 30,  "30m": 30,
      "1 hour": 60,  "1hour": 60,  "1h": 60,
      "2 hours": 120, "2h": 120,
      "4 hours": 240, "4h": 240,
      "8 hours": 480, "8h": 480,
      "daily":  1440, "24h": 1440, "1 day": 1440,
    };
    const raw = String(body.interval).trim().toLowerCase();
    let minutes;
    if (PRESETS[raw] !== undefined) {
      minutes = PRESETS[raw];
      _autoSyncConfig.intervalLabel = body.interval; // keep original label
    } else {
      minutes = parseInt(raw, 10);
      _autoSyncConfig.intervalLabel = `${minutes}m`;
    }
    if (!isNaN(minutes) && minutes > 0) {
      _autoSyncConfig.intervalMs = minutes * 60 * 1000;
    } else {
      return res.status(400).json({ ok: false, error: `Invalid interval "${body.interval}". Use '15 min', '30 min', '1 hour', '4 hours', '8 hours', 'daily' or a number of minutes.` });
    }
  }

  if (body.enabled      !== undefined) _autoSyncConfig.enabled     = !!body.enabled;
  if (body.companyName  !== undefined) _autoSyncConfig.companyName = body.companyName.trim();
  if (body.fromDays     !== undefined) _autoSyncConfig.fromDays    = parseInt(body.fromDays, 10) || 30;

  if (body.options && typeof body.options === "object") {
    _autoSyncConfig.options = { ..._autoSyncConfig.options, ...body.options };
  }

  if (body.creds && typeof body.creds === "object") {
    _autoSyncConfig.creds = { ..._autoSyncConfig.creds, ...body.creds };
  }

  // Only restart (and log) the scheduler if a scheduling-relevant field changed.
  // When the frontend re-injects creds on mount it sends only {creds} — in that
  // case we do NOT restart the scheduler so the countdown timer is not reset.
  const scheduleFieldChanged = (
    body.enabled !== undefined ||
    body.interval !== undefined ||
    body.companyName !== undefined ||
    body.options !== undefined
  );
  if (scheduleFieldChanged) {
    startScheduler();
  }
  persistAutoSyncConfig();

  res.json({
    ok:      true,
    message: _autoSyncConfig.enabled
      ? `Auto-sync enabled — runs every ${_autoSyncConfig.intervalLabel}`
      : "Auto-sync configured but not enabled (set enabled: true to start)",
    config:  _safeConfig(),
  });
});

/**
 * GET /api/auto-sync/status
 * Returns current scheduler config + last run result.
 */
app.get("/api/auto-sync/status", (_req, res) => {
  res.json({
    config:    _safeConfig(),
    running:   _syncRunning,
    lastSync:  _lastAutoSync,
    nextRunIn: _autoSyncConfig.enabled && _autoSyncTimer
      ? `up to ${_autoSyncConfig.intervalLabel}`
      : "not scheduled",
    nextRunAt: _nextRunAt ? _nextRunAt.toISOString() : null,
  });
});

/**
 * POST /api/auto-sync/run-now
 * Trigger an immediate run using the current config (same as the interval would).
 * Respects the same concurrency guard — won't double-run.
 */
app.post("/api/auto-sync/run-now", async (_req, res) => {
  if (_syncRunning) {
    return res.status(409).json({ ok: false, error: "A sync is already running. Wait for it to finish." });
  }
  res.json({ ok: true, message: "Auto-sync triggered manually — running in background" });
  runAutoSync("manual");
});

/**
 * POST /api/auto-sync/enable   — quick toggle on
 * POST /api/auto-sync/disable  — quick toggle off
 */
app.post("/api/auto-sync/enable", (_req, res) => {
  _autoSyncConfig.enabled = true;
  startScheduler();
  const what = Object.entries(_autoSyncConfig.options).filter(([,v])=>v).map(([k])=>k).join(", ");
  logger.human.autoSyncEnabled(_autoSyncConfig.intervalLabel, what);
  persistAutoSyncConfig();
  res.json({ ok: true, message: `Auto-sync enabled — runs every ${_autoSyncConfig.intervalLabel}` });
});

app.post("/api/auto-sync/disable", (_req, res) => {
  _autoSyncConfig.enabled = false;
  stopScheduler();
  logger.human.autoSyncDisabled();
  persistAutoSyncConfig();
  res.json({ ok: true, message: "Auto-sync disabled" });
});

/**
 * POST /api/auto-sync/reset-state
 * Clears incremental state — next run will do a full sync from scratch.
 */
app.post("/api/auto-sync/reset-state", (req, res) => {
  const company = (req.body || {}).company || _autoSyncConfig.companyName;
  if (!company) return res.status(400).json({ ok: false, error: "company required" });
  const erpnextUrl = _autoSyncConfig.creds.url || config.erpnext.url || "default";
  resetCompanyState(company, erpnextUrl);
  res.json({ ok: true, message: `Incremental state cleared for "${company}" — next sync will be a full sync` });
});

/**
 * GET /api/auto-sync/state
 * Returns the raw incremental sync state for the configured company.
 */
app.get("/api/auto-sync/state", (req, res) => {
  const company = req.query.company || _autoSyncConfig.companyName;
  if (!company) return res.status(400).json({ ok: false, error: "company required" });
  const erpnextUrl = _autoSyncConfig.creds.url || config.erpnext.url || "default";
  const state = getCompanyState(company, erpnextUrl);
  res.json({ ok: true, company, state });
});

// Strip secrets from config before sending to client
function _safeConfig() {
  return {
    enabled:       _autoSyncConfig.enabled,
    interval:      _autoSyncConfig.intervalLabel,
    intervalMs:    _autoSyncConfig.intervalMs,
    companyName:   _autoSyncConfig.companyName,
    fromDays:      _autoSyncConfig.fromDays,
    options:       _autoSyncConfig.options,
    erpnextUrl:    _autoSyncConfig.creds.url || config.erpnext.url || "",
    erpnextCompany: _autoSyncConfig.creds.erpnextCompany || "",
    // Never expose apiKey / apiSecret
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// ── Auto-resume scheduler on startup if it was enabled before restart ───────
// This is what makes it survive nodemon reloads and production restarts.
if (_autoSyncConfig.enabled) {
  startScheduler(true); // silent=true — startup log comes from app.listen below
}

app.listen(config.port, () => {
  logger.human.serverReady(`http://localhost:${config.port}`, {
    enabled:  _autoSyncConfig.enabled,
    interval: _autoSyncConfig.intervalLabel,
  });
  logger.human.tallyConnected(config.tally.url);
  logger.human.checkReady();
  if (!_autoSyncConfig.enabled) {
    logger.human.headsUp(
      "Auto-sync is off. You can turn it on from the Sync settings page and choose how often it should run."
    );
  }
});