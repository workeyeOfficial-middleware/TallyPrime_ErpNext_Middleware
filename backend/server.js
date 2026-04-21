import express from "express";
import cors from "cors";
import cron from "node-cron";
import { config } from "./config/config.js";
import router from "./routes/index.js";
import { logger } from "./logs/logger.js";
import { fetchTallyLedgers, fetchTallyStockItems, fetchTallyVouchers, fetchTallyGroups, fetchTallyGodowns, fetchTallyCostCentres } from "./tally/tallyClient.js";
import { runFullSync } from "./tally/Erpnextclient.js";

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

  const now   = new Date();
  const from  = new Date(now);
  from.setDate(from.getDate() - AUTO_SYNC_FROM_DAYS);
  const fromDate = from.toISOString().slice(0, 10);
  const toDate   = now.toISOString().slice(0, 10);

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

  logger.info(`Auto-sync started for "${companyName}" (${fromDate} → ${toDate})`, opts);
  _lastAutoSync = { startedAt: now.toISOString(), status: "running", company: companyName };

  try {
    const groups     = opts.syncChartOfAccounts || opts.syncOpeningBalances ? await fetchTallyGroups(companyName)                    : [];
    const costCentres = opts.syncCostCentres                                 ? await fetchTallyCostCentres(companyName)               : [];
    const godowns    = opts.syncGodowns                                      ? await fetchTallyGodowns(companyName)                   : [];
    const ledgers    = opts.syncLedgers || opts.syncOpeningBalances           ? await fetchTallyLedgers(companyName)                   : [];
    const stockItems = opts.syncStock || opts.syncTaxes                       ? await fetchTallyStockItems(companyName)                : [];
    const vouchers   = opts.syncVouchers || opts.syncInvoices                 ? await fetchTallyVouchers(companyName, fromDate, toDate) : [];

    const result = await runFullSync(
      companyName,
      { groups, ledgers, stockItems, vouchers, godowns, costCentres },
      opts
    );

    _lastAutoSync = { ...result, triggeredBy: "cron", fromDate, toDate };
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

// ──────────────────────────────────────────────────────────────────────────

app.listen(config.port, () => {
  logger.info(`Tally Middleware server running on http://localhost:${config.port}`);
  logger.info(`Tally endpoint: ${config.tally.url}`);
  logger.info(`Run POST /api/middleware/check to validate all Tally data`);
});