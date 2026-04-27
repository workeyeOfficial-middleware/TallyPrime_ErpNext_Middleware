import express from "express";
import cors from "cors";
import cron from "node-cron";
import { config } from "./config/config.js";
import router from "./routes/index.js";
import { logger } from "./logs/logger.js";
import { fetchTallyLedgers, fetchTallyStockItems, fetchTallyVouchers, fetchTallyGroups, fetchTallyGodowns, fetchTallyCostCentres } from "./tally/tallyClient.js";
import { runFullSync } from "./tally/Erpnextclient.js";
import {
  getCompanyState,
  saveCompanyState,
  getIncrementalVoucherDates,
  filterChangedMasters,
  buildAlterIdMap,
  resetCompanyState,
} from "./syncState.js";

const app = express();

app.use(cors());
app.use(express.json());
app.use("/api", router);

// ── Auto-Sync Scheduler ────────────────────────────────────────────────────
// Configure via .env:
//   AUTO_SYNC_ENABLED=true          (default: false)
//   AUTO_SYNC_CRON="0 2 * * *"     (default: daily at 2 AM)
//   AUTO_SYNC_OPTIONS=ledgers,stock,vouchers   (comma-separated, default: ledgers,stock,vouchers)
//   AUTO_SYNC_FROM_DAYS=30          (how many days back to sync vouchers, default: 30)

const AUTO_SYNC_ENABLED  = process.env.AUTO_SYNC_ENABLED === "true";
const AUTO_SYNC_CRON     = process.env.AUTO_SYNC_CRON || "0 2 * * *";   // daily 2 AM
const AUTO_SYNC_OPTIONS  = (process.env.AUTO_SYNC_OPTIONS || "ledgers,stock,vouchers").split(",").map(s => s.trim());
const AUTO_SYNC_FROM_DAYS = parseInt(process.env.AUTO_SYNC_FROM_DAYS || "30", 10);

let _autoSyncJob = null;
let _lastAutoSync = null;

async function runAutoSync() {
  const companyName = config.tally.companyName;
  if (!companyName) {
    logger.warn("Auto-sync skipped: TALLY_COMPANY_NAME not set in .env");
    return;
  }
  if (!config.erpnext.url || !config.erpnext.apiKey) {
    logger.warn("Auto-sync skipped: ERPNext credentials not set in .env");
    return;
  }

  const now    = new Date();
  const toDate = now.toISOString().slice(0, 10);

  // ── Compute fallback fromDate (only used on first-ever sync) ──────────────
  const fallbackFrom = new Date(now);
  fallbackFrom.setDate(fallbackFrom.getDate() - AUTO_SYNC_FROM_DAYS);
  const fallbackFromDate = fallbackFrom.toISOString().slice(0, 10);

  const opts = {
    syncChartOfAccounts:  AUTO_SYNC_OPTIONS.includes("chart-of-accounts"),
    syncLedgers:          AUTO_SYNC_OPTIONS.includes("ledgers"),
    syncOpeningBalances:  AUTO_SYNC_OPTIONS.includes("opening-balances"),
    syncGodowns:          AUTO_SYNC_OPTIONS.includes("godowns"),
    syncCostCentres:      AUTO_SYNC_OPTIONS.includes("cost-centres"),
    syncStock:            AUTO_SYNC_OPTIONS.includes("stock"),
    syncVouchers:         AUTO_SYNC_OPTIONS.includes("vouchers"),
    syncInvoices:         AUTO_SYNC_OPTIONS.includes("invoices"),
    syncTaxes:            AUTO_SYNC_OPTIONS.includes("taxes"),
  };

  // ── Load incremental state for this company ───────────────────────────────
  const erpnextUrl = config.erpnext.url || "default";
  const state = getCompanyState(companyName, erpnextUrl);
  const isFirstSync = !state.lastVoucherSyncDate && !state.lastMasterSyncAt;

  logger.info(
    `Auto-sync started for "${companyName}" — ${isFirstSync ? "FULL (first run)" : "INCREMENTAL"}`,
    opts
  );
  _lastAutoSync = { startedAt: now.toISOString(), status: "running", company: companyName };

  try {
    // ── 1. MASTERS — fetch all, but only sync changed ones ─────────────────
    let groups      = [];
    let costCentres = [];
    let godowns     = [];
    let ledgers     = [];
    let stockItems  = [];

    if (opts.syncChartOfAccounts || opts.syncOpeningBalances) {
      const allGroups = await fetchTallyGroups(companyName);
      const { toSync: changedGroups, unchanged: unchangedGroups } =
        filterChangedMasters(allGroups, state.groupAlterIds);
      logger.info(`Groups: ${changedGroups.length} to sync, ${unchangedGroups} unchanged`);
      groups = changedGroups;
      // Save updated alterId map regardless so new records are tracked
      saveCompanyState(companyName, { groupAlterIds: buildAlterIdMap(allGroups) }, erpnextUrl);
    }

    if (opts.syncCostCentres) {
      costCentres = await fetchTallyCostCentres(companyName);
    }

    if (opts.syncGodowns) {
      godowns = await fetchTallyGodowns(companyName);
    }

    if (opts.syncLedgers || opts.syncOpeningBalances) {
      const allLedgers = await fetchTallyLedgers(companyName);
      const { toSync: changedLedgers, unchanged: unchangedLedgers } =
        filterChangedMasters(allLedgers, state.ledgerAlterIds);
      logger.info(`Ledgers: ${changedLedgers.length} to sync, ${unchangedLedgers} unchanged`);
      ledgers = changedLedgers;
      saveCompanyState(companyName, { ledgerAlterIds: buildAlterIdMap(allLedgers) }, erpnextUrl);
    }

    if (opts.syncStock || opts.syncTaxes) {
      const allStock = await fetchTallyStockItems(companyName);
      const { toSync: changedStock, unchanged: unchangedStock } =
        filterChangedMasters(allStock, state.stockAlterIds);
      logger.info(`Stock: ${changedStock.length} to sync, ${unchangedStock} unchanged`);
      stockItems = changedStock;
      saveCompanyState(companyName, { stockAlterIds: buildAlterIdMap(allStock) }, erpnextUrl);
    }

    // ── 2. VOUCHERS — only fetch new/amended date window ──────────────────
    let vouchers = [];
    if (opts.syncVouchers || opts.syncInvoices) {
      const { fromDate, toDate: vToDate, isIncremental } =
        getIncrementalVoucherDates(companyName, fallbackFromDate, toDate, erpnextUrl);

      logger.info(
        `Vouchers: fetching ${isIncremental ? "incremental" : "full"} range ${fromDate} → ${vToDate}`
      );
      vouchers = await fetchTallyVouchers(companyName, fromDate, vToDate);
    }

    // ── 3. Run the sync ────────────────────────────────────────────────────
    const result = await runFullSync(
      companyName,
      { groups, ledgers, stockItems, vouchers, godowns, costCentres },
      opts
    );

    // ── 4. Save state only on success ─────────────────────────────────────
    if (result.status !== "failed") {
      saveCompanyState(companyName, {
        lastVoucherSyncDate: toDate,
        lastMasterSyncAt:    now.toISOString(),
      }, erpnextUrl);
      logger.info(`syncState: checkpoint saved → vouchers up to ${toDate}`);
    }

    _lastAutoSync = {
      ...result,
      triggeredBy:   "cron",
      fromDate:      state.lastVoucherSyncDate || fallbackFromDate,
      toDate,
      isIncremental: !isFirstSync,
    };
    logger.info(`Auto-sync complete: ${result.status}`);
  } catch (err) {
    _lastAutoSync = { status: "failed", error: err.message, startedAt: now.toISOString() };
    logger.error(`Auto-sync failed: ${err.message}`);
  }
}

if (AUTO_SYNC_ENABLED) {
  if (!cron.validate(AUTO_SYNC_CRON)) {
    logger.warn(`Invalid AUTO_SYNC_CRON expression: "${AUTO_SYNC_CRON}" — auto-sync disabled`);
  } else {
    _autoSyncJob = cron.schedule(AUTO_SYNC_CRON, runAutoSync);
    logger.info(`Auto-sync scheduled: "${AUTO_SYNC_CRON}" (options: ${AUTO_SYNC_OPTIONS.join(", ")})`);
  }
} else {
  logger.info("Auto-sync disabled (set AUTO_SYNC_ENABLED=true in .env to enable)");
}

// ── Auto-sync status & control endpoints ──────────────────────────────────
app.get("/api/auto-sync/status", (_req, res) => {
  res.json({
    enabled:    AUTO_SYNC_ENABLED,
    cron:       AUTO_SYNC_CRON,
    options:    AUTO_SYNC_OPTIONS,
    lastSync:   _lastAutoSync,
    nextRun:    _autoSyncJob ? "scheduled" : "not scheduled",
  });
});

app.post("/api/auto-sync/run-now", async (_req, res) => {
  res.json({ ok: true, message: "Manual auto-sync triggered in background" });
  runAutoSync(); // fire and forget
});

// Reset incremental state for a company — forces full re-sync on next run
app.post("/api/auto-sync/reset-state", (req, res) => {
  const { company } = req.body || {};
  const companyName = company || config.tally.companyName;
  if (!companyName) return res.status(400).json({ ok: false, error: "company required" });
  resetCompanyState(companyName, config.erpnext.url || "default");
  res.json({ ok: true, message: `Incremental state cleared for "${companyName}" — next sync will be full` });
});

// Get current incremental sync state for a company
app.get("/api/auto-sync/state", (req, res) => {
  const company = req.query.company || config.tally.companyName;
  if (!company) return res.status(400).json({ ok: false, error: "company required" });
  const state = getCompanyState(company, config.erpnext.url || "default");
  res.json({ ok: true, company, state });
});

// ──────────────────────────────────────────────────────────────────────────

app.listen(config.port, () => {
  logger.info(`Tally Middleware server running on http://localhost:${config.port}`);
  logger.info(`Tally endpoint: ${config.tally.url}`);
  logger.info(`Run POST /api/middleware/check to validate all Tally data`);
});