import { Router } from "express";
import {
  pingTally,
  fetchTallyCompanies,
  // Accounting Masters
  fetchTallyGroups,
  fetchTallyLedgers,
  fetchTallyVoucherTypes,
  fetchTallyCostCategories,
  fetchTallyCostCentres,
  fetchTallyCurrencies,
  fetchTallyBudgets,
  // Inventory Masters
  fetchTallyStockGroups,
  fetchTallyStockItems,
  fetchTallyStockCategories,
  fetchTallyUnits,
  fetchTallyGodowns,
  // Transactions
  fetchTallyVouchers,
  // Full check
  runMiddlewareCheck,
} from "../tally/tallyClient.js";
import {
  pingErpNext,
  syncLedgersToErpNext,
  syncStockToErpNext,
  syncVouchersToErpNext,
  syncGodownsToErpNext,
  syncOpeningBalancesToErpNext,
  syncCostCentresToErpNext,
  syncInvoicesToErpNext,
  syncTaxesToErpNext,
  syncChartOfAccountsToErpNext,
  smartSyncLedgersToErpNext,
  runFullSync,
  resolveErpNextCompanyPublic,
} from "../tally/Erpnextclient.js";
import {
  getCompanyState,
  saveCompanyState,
  getIncrementalVoucherDates,
  filterChangedMasters,
  buildAlterIdMap,
} from "../syncState.js";
import { logger } from "../logs/logger.js";
import { config } from "../config/config.js";

const router = Router();

// ── Helper: resolve company from query or env ─────────────────────────────────
function resolveCompany(req) {
  return req.query.company || config.tally.companyName || null;
}

// ── Helper: extract per-request ERPNext credentials override ─────────────────
function extractCreds(req) {
  const { erpnextUrl, erpnextApiKey, erpnextApiSecret, erpnextCompany } = req.body || {};
  const creds = {};
  if (erpnextUrl && erpnextApiKey && erpnextApiSecret) {
    creds.url       = erpnextUrl;
    creds.apiKey    = erpnextApiKey;
    creds.apiSecret = erpnextApiSecret;
  }
  // erpnextCompany is always passed through — it's the user's explicit ERPNext company name
  if (erpnextCompany) creds.erpnextCompany = erpnextCompany;
  return creds;
}

// ══════════════════════════════════════════════════════════════════════════════
// ASYNC JOB REGISTRY
// Syncing 15k+ records takes 30–60 min. Returning a jobId immediately prevents
// the browser from timing out and showing "Failed to fetch" while the backend
// is still happily running. The UI polls GET /sync/status/:jobId instead.
// ══════════════════════════════════════════════════════════════════════════════
const jobs = new Map(); // jobId -> { status, result, error, startedAt, type }

function createJob(type) {
  const id = `${type}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
  jobs.set(id, { id, type, status: "running", result: null, error: null, startedAt: new Date().toISOString() });
  // Auto-cleanup after 2 hours
  setTimeout(() => jobs.delete(id), 2 * 60 * 60 * 1000);
  return id;
}

function finishJob(id, result) {
  const job = jobs.get(id);
  if (job) Object.assign(job, { status: "done", result, finishedAt: new Date().toISOString() });
}

function failJob(id, error) {
  const job = jobs.get(id);
  if (job) Object.assign(job, { status: "failed", error: error.message || String(error), finishedAt: new Date().toISOString() });
}

// Track cancelled jobs so background workers can check and bail out early
const cancelledJobs = new Set();

function cancelJob(id) {
  const job = jobs.get(id);
  if (job && job.status === "running") {
    Object.assign(job, { status: "cancelled", error: "Stopped by user", finishedAt: new Date().toISOString() });
    cancelledJobs.add(id);
    // Remove from cancelled set after 10 min (cleanup)
    setTimeout(() => cancelledJobs.delete(id), 10 * 60 * 1000);
    return true;
  }
  return false;
}

export { cancelledJobs }; // so sync workers can import and check this

// ── GET /sync/status/:jobId ───────────────────────────────────────────────────
router.get("/sync/status/:jobId", (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ ok: false, error: "Job not found (may have expired)" });
  res.json({ ok: true, job });
});

// ── GET /sync/jobs ────────────────────────────────────────────────────────────
// Lists all running jobs so the UI can pick up on page refresh
router.get("/sync/jobs", (_req, res) => {
  const list = Array.from(jobs.values()).filter((j) => j.status === "running");
  res.json({ ok: true, jobs: list });
});

// ── POST /sync/cancel/:jobId ──────────────────────────────────────────────────
// Marks the job as cancelled immediately so the UI gets instant feedback.
// The background worker checks cancelledJobs on each iteration and bails out.
router.post("/sync/cancel/:jobId", (req, res) => {
  const { jobId } = req.params;
  const job = jobs.get(jobId);
  if (!job) return res.status(404).json({ ok: false, error: "Job not found" });
  if (job.status !== "running") {
    return res.json({ ok: true, message: `Job already in state: ${job.status}` });
  }
  cancelJob(jobId);
  logger.info(`Job ${jobId} cancelled by user`);
  res.json({ ok: true, message: "Job cancelled", jobId });
});

// ── GET /health ───────────────────────────────────────────────────────────────
router.get("/health", (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString(), tallyUrl: config.tally.url });
});

// ── GET /tally/ping ───────────────────────────────────────────────────────────
router.get("/tally/ping", async (_req, res) => {
  try {
    res.json(await pingTally());
  } catch (err) {
    res.status(500).json({ connected: false, error: err.message });
  }
});

// ── GET /tally/companies ──────────────────────────────────────────────────────
router.get("/tally/companies", async (_req, res) => {
  try {
    const data = await fetchTallyCompanies();
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch companies", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ACCOUNTING MASTERS
// ═══════════════════════════════════════════════════════════════════════════════

router.get("/tally/groups", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyGroups(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch groups", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/tally/ledgers", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyLedgers(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch ledgers", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/tally/voucher-types", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyVoucherTypes(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch voucher types", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/tally/cost-categories", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyCostCategories(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch cost categories", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/tally/cost-centres", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyCostCentres(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch cost centres", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/tally/currencies", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyCurrencies(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch currencies", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/tally/budgets", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyBudgets(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch budgets", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// INVENTORY MASTERS
// ═══════════════════════════════════════════════════════════════════════════════

router.get("/tally/stock-groups", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyStockGroups(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch stock groups", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/tally/stock", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyStockItems(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch stock items", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/tally/stock-categories", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyStockCategories(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch stock categories", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/tally/units", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyUnits(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch units", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

router.get("/tally/godowns", async (req, res) => {
  const company = resolveCompany(req);
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyGodowns(company);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch godowns", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// TRANSACTIONS
// ═══════════════════════════════════════════════════════════════════════════════

router.get("/tally/vouchers", async (req, res) => {
  const company = resolveCompany(req);
  const { from, to } = req.query;
  if (!company) return res.status(400).json({ ok: false, error: "company query param required" });
  try {
    const data = await fetchTallyVouchers(company, from, to);
    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    logger.error("Failed to fetch vouchers", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── POST /middleware/check ────────────────────────────────────────────────────
router.post("/middleware/check", async (req, res) => {
  const { company, fromDate, toDate } = req.body || {};
  const companyName = company || config.tally.companyName;

  if (!companyName) {
    return res.status(400).json({
      ok: false,
      error: "No company specified. Pass { company } in the request body or set TALLY_COMPANY_NAME in .env",
    });
  }

  logger.info(`Middleware check started for: ${companyName}`);

  try {
    const report = await runMiddlewareCheck(companyName, { fromDate, toDate });
    res.json({ ok: true, report });
  } catch (err) {
    logger.error("Middleware check failed", { error: err.message });
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── GET /logs ─────────────────────────────────────────────────────────────────
// ── GET /logs ─────────────────────────────────────────────────────────────────
// Supports: ?company=X  ?fromDate=YYYY-MM-DD  ?toDate=YYYY-MM-DD  ?level=error  ?limit=200
router.get("/logs", (req, res) => {
  const { company, fromDate, toDate, level } = req.query;
  const limit = Math.min(parseInt(req.query.limit) || 200, 1000);
  const logs = logger.getLogs({ company: company || null, fromDate, toDate, level, limit });
  res.json({ logs });
});

// ── GET /logs/companies ───────────────────────────────────────────────────────
// Returns list of all companies that have log files (for tenant selector in UI)
router.get("/logs/companies", (_req, res) => {
  res.json({ companies: logger.listCompanies() });
});

// ── POST /erpnext/resolve-company ─────────────────────────────────────────────
router.post("/erpnext/resolve-company", async (req, res) => {
  const { company } = req.body || {};
  if (!company) return res.status(400).json({ ok: false, error: "company required" });
  try {
    const erpCompany = await resolveErpNextCompanyPublic(company, extractCreds(req));
    res.json({ ok: true, erpCompany });
  } catch (err) {
    res.json({ ok: true, erpCompany: null });
  }
});

// ── GET/POST /erpnext/ping ────────────────────────────────────────────────────
router.get("/erpnext/ping", async (_req, res) => {
  try { res.json(await pingErpNext()); }
  catch (err) { res.status(500).json({ connected: false, error: err.message }); }
});
router.post("/erpnext/ping", async (req, res) => {
  try { res.json(await pingErpNext(extractCreds(req))); }
  catch (err) { res.status(500).json({ connected: false, error: err.message }); }
});

// ══════════════════════════════════════════════════════════════════════════════
// SYNC ROUTES  — all return { ok, jobId } immediately and run in background.
// The UI polls GET /sync/status/:jobId to track progress.
// ══════════════════════════════════════════════════════════════════════════════

// ── POST /sync/ledgers ────────────────────────────────────────────────────────
router.post("/sync/ledgers", async (req, res) => {
  const company = req.body.company || config.tally.companyName;
  if (!company) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("ledgers");
  logger.info(`Ledger sync job ${jobId} started for: ${company}`);
  res.json({ ok: true, jobId, message: "Ledger sync started — poll /api/sync/status/" + jobId });

  // Run in background (intentionally not awaited)
  (async () => {
    try {
      const state      = getCompanyState(company);
      const allLedgers = await fetchTallyLedgers(company);
      const { toSync, unchanged } = filterChangedMasters(allLedgers, state.ledgerAlterIds);
      logger.info(`Ledgers: ${toSync.length} to sync, ${unchanged} unchanged (skipped)`);
      if (toSync.length === 0) {
        logger.info(`Ledger sync job ${jobId}: already up to date`);
        saveCompanyState(company, { ledgerAlterIds: buildAlterIdMap(allLedgers), lastMasterSyncAt: new Date().toISOString() });
        finishJob(jobId, { nothingToSync: true, message: "All ledgers are already up to date — nothing pushed to ERPNext." });
        return;
      }
      const result = await syncLedgersToErpNext(toSync, creds);
      saveCompanyState(company, { ledgerAlterIds: buildAlterIdMap(allLedgers), lastMasterSyncAt: new Date().toISOString() });
      logger.info(`Ledger sync job ${jobId} done`, result);
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Ledger sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});


// ── POST /sync/smart-ledgers ──────────────────────────────────────────────────
// Syncs ONLY the ledgers actually used in the given date range vouchers.
// Much faster than full ledger sync for testing — typically 100-300 instead of 16,000+
router.post("/sync/smart-ledgers", async (req, res) => {
  const { company, fromDate, toDate } = req.body;
  const companyName = company || config.tally.companyName;
  if (!companyName) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("smart-ledgers");
  logger.info(`Smart Ledger sync job ${jobId} started for: ${companyName} (${fromDate} to ${toDate})`);
  res.json({ ok: true, jobId, message: "Smart Ledger sync started — poll /api/sync/status/" + jobId });

  (async () => {
    try {
      logger.info("Smart Ledger Sync: fetching vouchers to extract used ledger names...");
      const [vouchers, allLedgers] = await Promise.all([
        fetchTallyVouchers(companyName, fromDate, toDate),
        fetchTallyLedgers(companyName),
      ]);
      logger.info(`Smart Ledger Sync: got ${vouchers.length} vouchers and ${allLedgers.length} total ledgers`);
      const result = await smartSyncLedgersToErpNext(vouchers, allLedgers, creds);
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Smart Ledger sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});

// ── POST /sync/stock ──────────────────────────────────────────────────────────
router.post("/sync/stock", async (req, res) => {
  const company = req.body.company || config.tally.companyName;
  if (!company) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("stock");
  logger.info(`Stock sync job ${jobId} started for: ${company}`);
  res.json({ ok: true, jobId, message: "Stock sync started — poll /api/sync/status/" + jobId });

  (async () => {
    try {
      const state    = getCompanyState(company);
      const allStock = await fetchTallyStockItems(company);
      const { toSync, unchanged } = filterChangedMasters(allStock, state.stockAlterIds);
      logger.info(`Stock: ${toSync.length} to sync, ${unchanged} unchanged (skipped)`);
      if (toSync.length === 0) {
        logger.info(`Stock sync job ${jobId}: already up to date`);
        saveCompanyState(company, { stockAlterIds: buildAlterIdMap(allStock), lastMasterSyncAt: new Date().toISOString() });
        finishJob(jobId, { nothingToSync: true, message: "All stock items are already up to date — nothing pushed to ERPNext." });
        return;
      }
      const result = await syncStockToErpNext(toSync, creds);
      saveCompanyState(company, { stockAlterIds: buildAlterIdMap(allStock), lastMasterSyncAt: new Date().toISOString() });
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Stock sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});

// ── POST /sync/vouchers ───────────────────────────────────────────────────────
router.post("/sync/vouchers", async (req, res) => {
  const { company, fromDate, toDate } = req.body;
  const companyName = company || config.tally.companyName;
  if (!companyName) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("vouchers");
  logger.info(`Voucher sync job ${jobId} started for: ${companyName}`);
  res.json({ ok: true, jobId, message: "Voucher sync started — poll /api/sync/status/" + jobId });

  (async () => {
    try {
      const { fromDate: effFrom, toDate: effTo, isIncremental } =
        getIncrementalVoucherDates(companyName, req.body.forceFromDate || null, toDate);
      const state       = getCompanyState(companyName);
      const lastSynced  = state.lastVoucherSyncDate;
      if (lastSynced && effTo <= lastSynced) {
        logger.info(`Voucher sync job ${jobId}: already up to date (window ${effFrom}→${effTo} covered by last sync ${lastSynced})`);
        finishJob(jobId, { nothingToSync: true, message: "All vouchers in this date window are already up to date — nothing pushed to ERPNext." });
        return;
      }
      logger.info(`Vouchers: ${isIncremental ? "incremental" : "full"} window ${effFrom} → ${effTo}`);
      const vouchers = await fetchTallyVouchers(companyName, effFrom, effTo);
      const result   = await syncVouchersToErpNext(vouchers, companyName, creds);
      saveCompanyState(companyName, { lastVoucherSyncDate: effTo, lastMasterSyncAt: new Date().toISOString() });
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Voucher sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});

// ── POST /sync/godowns ────────────────────────────────────────────────────────
router.post("/sync/godowns", async (req, res) => {
  const company = req.body.company || config.tally.companyName;
  if (!company) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("godowns");
  logger.info(`Godown sync job ${jobId} started for: ${company}`);
  res.json({ ok: true, jobId, message: "Godown sync started — poll /api/sync/status/" + jobId });

  (async () => {
    try {
      const state      = getCompanyState(company);
      const allGodowns = await fetchTallyGodowns(company);
      const { toSync, unchanged } = filterChangedMasters(allGodowns, state.godownAlterIds);
      logger.info(`Godowns: ${toSync.length} to sync, ${unchanged} unchanged (skipped)`);
      if (toSync.length === 0) {
        logger.info(`Godown sync job ${jobId}: already up to date`);
        saveCompanyState(company, { godownAlterIds: buildAlterIdMap(allGodowns), lastMasterSyncAt: new Date().toISOString() });
        finishJob(jobId, { nothingToSync: true, message: "All godowns are already up to date — nothing pushed to ERPNext." });
        return;
      }
      const result = await syncGodownsToErpNext(toSync, company, creds);
      saveCompanyState(company, { godownAlterIds: buildAlterIdMap(allGodowns), lastMasterSyncAt: new Date().toISOString() });
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Godown sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});

// ── POST /sync/opening-balances ───────────────────────────────────────────────
router.post("/sync/opening-balances", async (req, res) => {
  const company = req.body.company || config.tally.companyName;
  if (!company) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("opening-balances");
  logger.info(`Opening balance sync job ${jobId} started for: ${company}`);
  res.json({ ok: true, jobId, message: "Opening balance sync started — poll /api/sync/status/" + jobId });

  (async () => {
    try {
      // Fetch groups AND ledgers — COA must be synced before opening balances
      // so that ERPNext has real GL accounts to post JE rows against.
      // Without groups, bank/cash/capital ledgers fall through to the skip path.
      const [groups, ledgers] = await Promise.all([
        fetchTallyGroups(company),
        fetchTallyLedgers(company),
      ]);

      // Step 1: ensure the account tree exists in ERPNext
      logger.info(`[OB job ${jobId}] Syncing ${groups.length} groups (COA) before opening balances`);
      await syncChartOfAccountsToErpNext(groups, company, creds);

      // Step 2: post opening balance Journal Entries
      const result = await syncOpeningBalancesToErpNext(ledgers, company, creds);
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Opening balance sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});

// ── POST /sync/cost-centres ───────────────────────────────────────────────────
router.post("/sync/cost-centres", async (req, res) => {
  const company = req.body.company || config.tally.companyName;
  if (!company) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("cost-centres");
  logger.info(`Cost centre sync job ${jobId} started for: ${company}`);
  res.json({ ok: true, jobId, message: "Cost centre sync started — poll /api/sync/status/" + jobId });

  (async () => {
    try {
      const state          = getCompanyState(company);
      const allCostCentres = await fetchTallyCostCentres(company);
      const { toSync, unchanged } = filterChangedMasters(allCostCentres, state.costCentreAlterIds);
      logger.info(`Cost Centres: ${toSync.length} to sync, ${unchanged} unchanged (skipped)`);
      if (toSync.length === 0) {
        logger.info(`Cost centre sync job ${jobId}: already up to date`);
        saveCompanyState(company, { costCentreAlterIds: buildAlterIdMap(allCostCentres), lastMasterSyncAt: new Date().toISOString() });
        finishJob(jobId, { nothingToSync: true, message: "All cost centres are already up to date — nothing pushed to ERPNext." });
        return;
      }
      const result = await syncCostCentresToErpNext(toSync, company, creds);
      saveCompanyState(company, { costCentreAlterIds: buildAlterIdMap(allCostCentres), lastMasterSyncAt: new Date().toISOString() });
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Cost centre sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});

// ── POST /sync/invoices ───────────────────────────────────────────────────────
router.post("/sync/invoices", async (req, res) => {
  const { company, fromDate, toDate } = req.body;
  const companyName = company || config.tally.companyName;
  if (!companyName) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("invoices");
  logger.info(`Invoice sync job ${jobId} started for: ${companyName}`);
  res.json({ ok: true, jobId, message: "Invoice sync started — poll /api/sync/status/" + jobId });

  (async () => {
    try {
      const vouchers = await fetchTallyVouchers(companyName, fromDate, toDate);
      const result   = await syncInvoicesToErpNext(vouchers, companyName, creds);
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Invoice sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});

// ── POST /sync/full ───────────────────────────────────────────────────────────
// FIX: All flags are now destructured in ONE place with defaults of false.
// Previously syncLedgers/syncStock/syncVouchers had hardcoded defaults of `true`
// at the outer level while the other flags (syncGodowns etc.) had `false` inside
// the async block — so clicking "Godowns only" always triggered a ledger sync too.
router.post("/sync/full", async (req, res) => {
  const {
    company,
    fromDate,
    toDate,
    syncChartOfAccounts = false,
    syncLedgers         = false,
    syncOpeningBalances = false,
    syncGodowns         = false,
    syncCostCentres     = false,
    syncStock           = false,
    syncVouchers        = false,
    syncInvoices        = false,
    syncTaxes           = false,
  } = req.body;

  const companyName = company || config.tally.companyName;
  if (!companyName) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("full");
  logger.info(`Full sync job ${jobId} started for: ${companyName}`, {
    syncLedgers, syncStock, syncVouchers, syncGodowns,
    syncCostCentres, syncOpeningBalances, syncInvoices,
    syncTaxes, syncChartOfAccounts,
  });
  res.json({ ok: true, jobId, message: "Full sync started — poll /api/sync/status/" + jobId });

  (async () => {
    try {
      logger.info(`Full sync: fetching Tally data with incremental logic...`);

      // ── Tally connectivity check ──────────────────────────────────
      const tallyPing = await pingTally().catch(function() {
        return { connected: false, error: "No response from Tally" };
      });
      if (!tallyPing.connected) {
        const msg = "Tally is not connected — " + (tallyPing.error || "TallyPrime is not running or server mode is not enabled on port 9000") + ". Please open TallyPrime before syncing.";
        logger.error("[Job " + jobId + "] " + msg);
        failJob(jobId, new Error(msg));
        return;
      }
      logger.info("Tally ping OK (" + tallyPing.latencyMs + "ms)");

      // ── Load incremental state ────────────────────────────────────────────
      const state       = getCompanyState(companyName);
      const isFirstSync = !state.lastVoucherSyncDate && !state.lastMasterSyncAt;
      logger.info(`Sync mode: ${isFirstSync ? "FULL (first run)" : "INCREMENTAL"} for "${companyName}"`);

      // ── Masters — fetch all, sync only changed (via ALTERID) ─────────────
      let groups      = [];
      let costCentres = [];
      let godowns     = [];
      let ledgers     = [];
      let stockItems  = [];
      const newAlterIds = {};

      if (syncChartOfAccounts || syncOpeningBalances) {
        const allGroups = await fetchTallyGroups(companyName);
        const { toSync: changedGroups, unchanged: unchangedGroups } =
          filterChangedMasters(allGroups, state.groupAlterIds);
        logger.info(`Groups: ${changedGroups.length} to sync, ${unchangedGroups} unchanged (skipped)`);
        groups = changedGroups;
        newAlterIds.groupAlterIds = buildAlterIdMap(allGroups);
      }

      if (syncCostCentres) {
        const allCostCentres = await fetchTallyCostCentres(companyName);
        const { toSync: changedCostCentres, unchanged: unchangedCostCentres } =
          filterChangedMasters(allCostCentres, state.costCentreAlterIds);
        logger.info(`Cost Centres: ${changedCostCentres.length} to sync, ${unchangedCostCentres} unchanged (skipped)`);
        costCentres = changedCostCentres;
        newAlterIds.costCentreAlterIds = buildAlterIdMap(allCostCentres);
      }

      if (syncGodowns) {
        const allGodowns = await fetchTallyGodowns(companyName);
        const { toSync: changedGodowns, unchanged: unchangedGodowns } =
          filterChangedMasters(allGodowns, state.godownAlterIds);
        logger.info(`Godowns: ${changedGodowns.length} to sync, ${unchangedGodowns} unchanged (skipped)`);
        godowns = changedGodowns;
        newAlterIds.godownAlterIds = buildAlterIdMap(allGodowns);
      }

      if (syncLedgers || syncOpeningBalances) {
        const allLedgers = await fetchTallyLedgers(companyName);
        const { toSync: changedLedgers, unchanged: unchangedLedgers } =
          filterChangedMasters(allLedgers, state.ledgerAlterIds);
        logger.info(`Ledgers: ${changedLedgers.length} to sync, ${unchangedLedgers} unchanged (skipped)`);
        ledgers = changedLedgers;
        newAlterIds.ledgerAlterIds = buildAlterIdMap(allLedgers);
      }

      if (syncStock || syncTaxes) {
        const allStock = await fetchTallyStockItems(companyName);
        const { toSync: changedStock, unchanged: unchangedStock } =
          filterChangedMasters(allStock, state.stockAlterIds);
        logger.info(`Stock: ${changedStock.length} to sync, ${unchangedStock} unchanged (skipped)`);
        stockItems = changedStock;
        newAlterIds.stockAlterIds = buildAlterIdMap(allStock);
      }

      // ── Vouchers — only fetch new/amended date window ─────────────────
      let vouchers    = [];
      let effectiveFromDate = fromDate;
      let effectiveToDate   = toDate;

      if (syncVouchers || syncInvoices) {
        const dateWindow = getIncrementalVoucherDates(companyName, req.body.forceFromDate || null, toDate);
        effectiveFromDate = dateWindow.fromDate;
        effectiveToDate   = dateWindow.toDate;

        // Same-day skip: if the incremental window starts at or after the
        // date we already synced up to, AND no masters changed, there is
        // nothing new to fetch. Skipping avoids re-pushing 85 identical
        // vouchers as "updated" every time the user clicks Sync today.
        const lastSynced = state.lastVoucherSyncDate;
        const windowIsAlreadyCovered =
          lastSynced &&
          effectiveToDate <= lastSynced &&
          groups.length       === 0 &&
          ledgers.length      === 0 &&
          stockItems.length   === 0 &&
          costCentres.length  === 0 &&
          godowns.length      === 0;

        if (windowIsAlreadyCovered) {
          logger.info(
            "Vouchers: skipping fetch — window " + effectiveFromDate + " → " + effectiveToDate +
            " already covered by last sync (" + lastSynced + ") and no masters changed"
          );
          // vouchers stays [] — nothingToSync fires below
        } else {
          logger.info(
            "Vouchers: " + (dateWindow.isIncremental ? "incremental" : "full") +
            " window " + effectiveFromDate + " → " + effectiveToDate
          );
          vouchers = await fetchTallyVouchers(companyName, effectiveFromDate, effectiveToDate);
        }
      }

      // ── Short-circuit: nothing to push ─────────────────────────────────
      const nothingToSync =
        groups.length     === 0 &&
        ledgers.length    === 0 &&
        stockItems.length === 0 &&
        costCentres.length === 0 &&
        godowns.length    === 0 &&
        vouchers.length   === 0;

      if (nothingToSync) {
        logger.info("[Job " + jobId + "] Nothing to sync — all masters unchanged, no new vouchers in window");
        finishJob(jobId, {
          status: "ok",
          nothingToSync: true,
          message: "Everything is already up to date. No new or changed data found since the last sync.",
          finishedAt: new Date().toISOString(),
        });
        saveCompanyState(companyName, {
          lastVoucherSyncDate: effectiveToDate || new Date().toISOString().slice(0, 10),
          lastMasterSyncAt:    new Date().toISOString(),
          ...newAlterIds,
        });
        return;
      }

            // ── Run ERPNext sync ──────────────────────────────────────────────────
      const result = await runFullSync(
        companyName,
        { groups, ledgers, stockItems, vouchers, godowns, costCentres },
        {
          syncChartOfAccounts, syncCostCentres, syncGodowns,
          syncLedgers, syncStock, syncTaxes,
          syncOpeningBalances, syncVouchers, syncInvoices,
        },
        creds
      );

      // ── Save checkpoint only on success ───────────────────────────────────
      logger.info(`Full sync job ${jobId} done`);
      if (result.status !== "failed") {
        const today = new Date().toISOString().slice(0, 10);
        saveCompanyState(companyName, {
          lastVoucherSyncDate: effectiveToDate || today,
          lastMasterSyncAt:    new Date().toISOString(),
          ...newAlterIds,
        });
        logger.info(`syncState: checkpoint saved → vouchers up to ${effectiveToDate || today}`);
      }
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Full sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});

// ── POST /sync/chart-of-accounts ─────────────────────────────────────────────
router.post("/sync/chart-of-accounts", async (req, res) => {
  const company = req.body.company || config.tally.companyName;
  if (!company) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("chart-of-accounts");
  logger.info(`Chart of Accounts sync job ${jobId} started for: ${company}`);
  res.json({ ok: true, jobId, message: "Chart of Accounts sync started — poll /api/sync/status/" + jobId });

  (async () => {
    try {
      const groups = await fetchTallyGroups(company);
      const result = await syncChartOfAccountsToErpNext(groups, company, creds);
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Chart of Accounts sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});

// ── POST /sync/taxes ──────────────────────────────────────────────────────────
// Creates GST Tax Templates (IGST/CGST/SGST slabs) and links them to Items.
// Must be called AFTER /sync/stock so the Items already exist in ERPNext.
router.post("/sync/taxes", async (req, res) => {
  const company = req.body.company || config.tally.companyName;
  if (!company) return res.status(400).json({ ok: false, error: "company required" });

  const creds = extractCreds(req);
  const jobId = createJob("taxes");
  logger.info(`Tax sync job ${jobId} started for: ${company}`);
  res.json({ ok: true, jobId, message: "Tax sync started — poll /api/sync/status/" + jobId });

  (async () => {
    try {
      const stockItems = await fetchTallyStockItems(company);
      const result     = await syncTaxesToErpNext(stockItems, company, creds);
      finishJob(jobId, result);
    } catch (err) {
      logger.error(`Tax sync job ${jobId} failed: ${err.message}`);
      failJob(jobId, err);
    }
  })();
});

export default router;