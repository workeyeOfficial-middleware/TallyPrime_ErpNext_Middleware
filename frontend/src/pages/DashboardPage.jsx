import { useState, useEffect } from "react";

const BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:4000/api";

// ── Storage helpers ────────────────────────────────────────────────────────
function getErpUrlForCompany(c) {
  try {
    const m = JSON.parse(localStorage.getItem("erp_creds") || "{}");
    if (m[c]?.url) return m[c].url.replace(/\/$/, "");
    const g = JSON.parse(localStorage.getItem("erp_global_creds") || "{}");
    return (g.url || "").replace(/\/$/, "");
  } catch { return ""; }
}
function getErpCompanyName(c) {
  try { return JSON.parse(localStorage.getItem("erp_company_map") || "{}")[c] || c || ""; }
  catch { return c || ""; }
}
function hKey(u) { return `sync-history::${u || "__default__"}`; }
function loadHist(u) {
  try { return (JSON.parse(localStorage.getItem(hKey(u)) || "[]")).map(e => ({ ...e, at: e.at ? new Date(e.at) : null })); }
  catch { return []; }
}
function saveHist(u, arr) {
  try { localStorage.setItem(hKey(u), JSON.stringify(arr.slice(0, 30))); } catch {}
}
function shortUrl(u) { try { return new URL(u).hostname; } catch { return u; } }
function fmt(n) { return (n ?? 0).toLocaleString(); }
function fmtTime(ms) {
  if (!ms) return "—";
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}
function timeAgo(d) {
  if (!d) return "—";
  const s = Math.floor((Date.now() - new Date(d)) / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

const CSS = `
  @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  @keyframes fadeUp   { from { opacity:0; transform:translateY(12px) } to { opacity:1; transform:none } }
  @keyframes spin     { to   { transform: rotate(360deg) } }
  @keyframes pulse    { 0%,100%{opacity:1} 50%{opacity:.35} }
  @keyframes shimmer  { from{background-position:200% 0} to{background-position:-200% 0} }
  @keyframes countUp  { from{opacity:0;transform:scale(.85)} to{opacity:1;transform:scale(1)} }
  @keyframes glow     { 0%,100%{box-shadow:0 0 0 0 rgba(37,99,235,.18)} 50%{box-shadow:0 0 0 6px rgba(37,99,235,0)} }

  .db-stat-card {
    position: relative;
    background: #fff;
    border-radius: 16px;
    padding: 22px 24px 20px;
    overflow: hidden;
    cursor: default;
    transition: transform .18s ease, box-shadow .18s ease;
    border: 1px solid #eef0f6;
    box-shadow: 0 2px 8px rgba(15,23,42,.06), 0 0 0 0 transparent;
  }
  .db-stat-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 8px 28px rgba(15,23,42,.1);
  }
  .db-hist-row { transition: background .12s; }
  .db-hist-row:hover { filter: brightness(.97); }

  ::-webkit-scrollbar { width: 4px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: #e2e8f0; border-radius: 4px; }
`;

// ── Donut ──────────────────────────────────────────────────────────────────
function Donut({ created, updated, failed, size = 104 }) {
  const total = created + updated + failed;
  if (!total) return (
    <svg width={size} height={size} viewBox="0 0 100 100" style={{ flexShrink: 0 }}>
      <circle cx={50} cy={50} r={38} fill="none" stroke="#f1f5f9" strokeWidth={10} strokeDasharray="4 4" />
      <text x={50} y={54} textAnchor="middle" style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, fill: "#94a3b8" }}>no data</text>
    </svg>
  );
  const r = 38, circ = 2 * Math.PI * r;
  const segs = [
    { v: created, c: "#22c55e" },
    { v: updated, c: "#3b82f6" },
    { v: failed,  c: "#ef4444" },
  ];
  let offset = 0;
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" style={{ flexShrink: 0, filter: "drop-shadow(0 2px 8px rgba(0,0,0,.08))" }}>
      <circle cx={50} cy={50} r={r} fill="none" stroke="#f1f5f9" strokeWidth={10} />
      {segs.map(({ v, c }, i) => {
        if (!v) return null;
        const d = circ * (v / total);
        const el = (
          <circle key={i} cx={50} cy={50} r={r} fill="none" stroke={c} strokeWidth={10}
            strokeDasharray={`${d} ${circ - d}`}
            strokeDashoffset={-offset}
            transform="rotate(-90 50 50)"
            strokeLinecap="round" />
        );
        offset += d;
        return el;
      })}
      <text x={50} y={46} textAnchor="middle" style={{ fontFamily: "'Manrope',sans-serif", fontSize: 16, fill: "#0f172a", fontWeight: 800 }}>{fmt(total)}</text>
      <text x={50} y={58} textAnchor="middle" style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 7, fill: "#94a3b8" }}>records</text>
    </svg>
  );
}

// ── No Creds ──────────────────────────────────────────────────────────────
function NoCreds({ company }) {
  return (
    <div style={{ display:"flex", flexDirection:"column", alignItems:"center", justifyContent:"center", minHeight:420, gap:14, textAlign:"center", padding:32 }}>
      <div style={{ width:64, height:64, borderRadius:18, background:"linear-gradient(135deg,#eff6ff,#dbeafe)", display:"flex", alignItems:"center", justifyContent:"center", fontSize:30 }}>🔌</div>
      <p style={{ fontFamily:"'Manrope',sans-serif", fontSize:20, fontWeight:800, color:"#0f172a" }}>No ERPNext credentials</p>
      <p style={{ fontFamily:"'Manrope',sans-serif", fontSize:14, color:"#64748b", maxWidth:340, lineHeight:1.65 }}>
        Go to <strong style={{ color:"#2563eb" }}>Settings</strong> and enter the ERPNext URL, API key and company name for <strong>{company || "this company"}</strong>.
      </p>
    </div>
  );
}

// ── Main ──────────────────────────────────────────────────────────────────
export function DashboardPage({ currentCompany }) {
  const [autoStatus, setAutoStatus] = useState(null);
  const [loading, setLoading]       = useState(true);
  const [now, setNow]               = useState(new Date());

  const erpUrl     = getErpUrlForCompany(currentCompany);
  const erpCompany = getErpCompanyName(currentCompany);
  const hasCreds   = !!erpUrl;

  const [history, setHistory] = useState(() => loadHist(erpUrl));

  useEffect(() => { const t = setInterval(() => setNow(new Date()), 1000); return () => clearInterval(t); }, []);

  useEffect(() => {
    setAutoStatus(null); setLoading(true);
    setHistory(loadHist(getErpUrlForCompany(currentCompany)));
  }, [currentCompany]);

  useEffect(() => {
    if (!hasCreds) { setLoading(false); return; }
    const load = async () => {
      try {
        const q = currentCompany ? `?company=${encodeURIComponent(currentCompany)}` : "";
        const data = await fetch(`${BASE_URL}/auto-sync/status${q}`).then(r => r.json());
        setAutoStatus(data);
      } catch {}
      finally { setLoading(false); }
    };
    load();
    const t = setInterval(load, 5000);
    return () => clearInterval(t);
  }, [currentCompany, hasCreds]);

  const lastSync  = autoStatus?.lastSync;
  const cfg       = autoStatus?.config || {};
  const isRunning = !!autoStatus?.running;
  const steps     = lastSync?.steps || {};

  function sum(step, f) {
    if (!step) return 0;
    let t = 0;
    ["customers","suppliers","accounts","sales","purchase","journalEntries","paymentEntries"]
      .forEach(k => { if (step[k]) t += step[k][f] || 0; });
    if (typeof step[f] === "number") t += step[f];
    return t;
  }

  const allSteps = [steps.chartOfAccounts, steps.ledgers, steps.smartLedgers, steps.openingBalances, steps.godowns, steps.costCentres, steps.stockItems, steps.vouchers, steps.invoices].filter(Boolean);
  const tCreated = allSteps.reduce((a,s) => a + sum(s,"created"), 0);
  const tUpdated = allSteps.reduce((a,s) => a + sum(s,"updated"), 0);
  const tFailed  = allSteps.reduce((a,s) => a + sum(s,"failed"),  0);
  const tSkipped = allSteps.reduce((a,s) => a + sum(s,"skipped"), 0);

  const lastAt   = lastSync?.finishedAt || lastSync?.startedAt;
  const isUD     = lastSync?.status === "uptodate";
  const nextRun  = autoStatus?.nextRunAt ? new Date(autoStatus.nextRunAt) : null;
  const remMs    = nextRun ? Math.max(0, nextRun - now) : 0;

  // Use finishedAt as the unique key — captures BOTH auto-sync and manual sync runs
  const dedupKey = lastSync?.finishedAt || lastSync?.startedAt;
  useEffect(() => {
    if (!dedupKey || !erpUrl) return;
    if (autoStatus?.running) return; // skip in-progress runs
    const u = getErpUrlForCompany(currentCompany);
    setHistory(prev => {
      const entryAt = new Date(dedupKey);
      // Dedup: skip if we already stored this exact run
      if (prev[0]?.finishedAt && prev[0].finishedAt === dedupKey) return prev;
      if (!prev[0]?.finishedAt && prev[0]?.at?.getTime() === entryAt.getTime()) return prev;
      const e = {
        at: entryAt,
        finishedAt: dedupKey,
        status: isUD ? "uptodate" : lastSync.status || "ok",
        from: lastSync.fromDate,
        to: lastSync.toDate,
        upToDate: isUD,
        error: lastSync.status === "failed" ? lastSync.error : null,
        erpUrl: u,
        created: tCreated,
        updated: tUpdated,
        failed: tFailed,
        skipped: tSkipped,
        trigger: lastSync?.triggeredBy || "auto",
      };
      const up = [e, ...prev].slice(0, 50);
      saveHist(u, up);
      return up;
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dedupKey, autoStatus?.running]);

  const lifetimeC = history.reduce((a,h) => a+(h.created||0), 0);
  const lifetimeU = history.reduce((a,h) => a+(h.updated||0), 0);
  const successR  = history.filter(h => h.status !== "failed").length;
  const autoRuns  = history.filter(h => h.trigger === "auto" || !h.trigger).length;
  const manualRuns = history.filter(h => h.trigger === "manual").length;
  const totalR    = history.length;

  const syncColor = isUD ? "#0d9488" : lastSync?.status === "failed" ? "#dc2626" : "#16a34a";
  const syncBg    = isUD ? "#f0fdfa" : lastSync?.status === "failed" ? "#fef2f2" : "#f0fdf4";
  const syncLabel = isUD ? "Up to date" : lastSync?.status === "failed" ? "Failed" : "Success";

  if (loading) return (
    <>
      <style>{CSS}</style>
      <div style={{ display:"flex", alignItems:"center", justifyContent:"center", height:320, gap:10 }}>
        <span style={{ width:20, height:20, borderRadius:"50%", border:"3px solid #bfdbfe", borderTopColor:"#2563eb", animation:"spin .7s linear infinite", display:"inline-block" }} />
        <span style={{ fontFamily:"'Manrope',sans-serif", fontSize:13, color:"#64748b" }}>Loading dashboard…</span>
      </div>
    </>
  );

  if (!hasCreds) return <><style>{CSS}</style><NoCreds company={currentCompany} /></>;

  const letter = (currentCompany || "T").charAt(0).toUpperCase();

  return (
    <>
      <style>{CSS}</style>
      <div style={{ display:"flex", flexDirection:"column", gap:16, fontFamily:"'Manrope',sans-serif", paddingBottom:40 }}>

        {/* ── HERO HEADER ─────────────────────────────────────────────── */}
        <div style={{
          position:"relative", overflow:"hidden", borderRadius:18,
          background:"linear-gradient(135deg, #0f172a 0%, #1e3a5f 55%, #1e40af 100%)",
          padding:"28px 32px", animation:"fadeUp .4s ease both",
          boxShadow:"0 8px 40px rgba(15,23,42,.2)",
        }}>
          <div style={{ position:"absolute", top:-60, right:-60, width:260, height:260, borderRadius:"50%", background:"rgba(255,255,255,.04)", pointerEvents:"none" }} />
          <div style={{ position:"absolute", bottom:-80, right:120, width:200, height:200, borderRadius:"50%", background:"rgba(59,130,246,.12)", pointerEvents:"none" }} />
          <div style={{ position:"absolute", top:-30, left:280, width:140, height:140, borderRadius:"50%", background:"rgba(99,102,241,.1)", pointerEvents:"none" }} />

          {/* Single strict row: [Avatar+Name] · [Stats strip] · [Clock] — no wrapping */}
          <div style={{ position:"relative", display:"grid", gridTemplateColumns:"auto 1fr auto", alignItems:"center", gap:20 }}>

            {/* LEFT — Avatar + company names + status */}
            <div style={{ display:"flex", alignItems:"center", gap:16, minWidth:0 }}>
              <div style={{
                width:52, height:52, borderRadius:14, flexShrink:0,
                background:"linear-gradient(135deg, #3b82f6, #6366f1)",
                display:"flex", alignItems:"center", justifyContent:"center",
                fontFamily:"'Manrope',sans-serif", fontSize:20, fontWeight:800, color:"#fff",
                boxShadow:"0 0 0 3px rgba(255,255,255,.15)",
              }}>{letter}</div>

              <div style={{ minWidth:0 }}>
                {/* Status pill */}
                <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:5 }}>
                  <span style={{
                    width:7, height:7, borderRadius:"50%", flexShrink:0,
                    background: isRunning ? "#22c55e" : cfg.enabled ? "#60a5fa" : "#64748b",
                    boxShadow: isRunning ? "0 0 0 3px rgba(34,197,94,.25)" : cfg.enabled ? "0 0 0 3px rgba(96,165,250,.25)" : "none",
                    animation: (isRunning || cfg.enabled) ? "pulse 1.8s ease infinite" : "none",
                  }} />
                  <span style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"rgba(255,255,255,.5)", textTransform:"uppercase", letterSpacing:"0.12em", whiteSpace:"nowrap" }}>
                    {isRunning ? "Syncing now" : cfg.enabled ? "Auto sync active" : "Sync Dashboard"}
                  </span>
                </div>

                {/* Company pills — single line */}
                <div style={{ display:"flex", alignItems:"center", gap:8, flexWrap:"nowrap" }}>
                  <span style={{
                    background:"rgba(99,102,241,.28)", border:"1px solid rgba(99,102,241,.45)",
                    padding:"3px 12px", borderRadius:8, color:"#c7d2fe", fontSize:19, fontWeight:800,
                    whiteSpace:"nowrap",
                  }}>{currentCompany || "Tally"}</span>

                  {erpCompany && erpCompany !== currentCompany && (
                    <>
                      <span style={{ color:"rgba(255,255,255,.35)", fontSize:16, lineHeight:1 }}>→</span>
                      <span style={{
                        background:"rgba(59,130,246,.28)", border:"1px solid rgba(59,130,246,.5)",
                        padding:"3px 12px", borderRadius:8, color:"#93c5fd", fontSize:19, fontWeight:800,
                        whiteSpace:"nowrap",
                      }}>{erpCompany}</span>
                    </>
                  )}
                </div>

                {/* URL */}
                {erpUrl && (
                  <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"rgba(255,255,255,.3)", marginTop:4 }}>
                    {shortUrl(erpUrl)}
                  </p>
                )}
              </div>
            </div>

            {/* CENTER — Stats strip (shrinks if needed) */}
            {totalR > 0 && (
              <div style={{ display:"flex", justifyContent:"center" }}>
                <div style={{ display:"flex", gap:0, background:"rgba(255,255,255,.06)", borderRadius:12, border:"1px solid rgba(255,255,255,.09)", overflow:"hidden" }}>
                  {[
                    { label:"Runs",      value: totalR },
                    { label:"Auto",      value: autoRuns },
                    { label:"Manual",    value: manualRuns },
                    { label:"Success",   value: `${Math.round((successR/totalR)*100)}%` },
                    { label:"Created",   value: fmt(lifetimeC) },
                    { label:"Updated",   value: fmt(lifetimeU) },
                  ].map(({ label, value }, i, arr) => (
                    <div key={label} style={{
                      padding:"12px 16px", textAlign:"center",
                      borderRight: i < arr.length-1 ? "1px solid rgba(255,255,255,.09)" : "none",
                    }}>
                      <p style={{ fontFamily:"'Manrope',sans-serif", fontSize:16, fontWeight:800, color:"#fff", margin:0, lineHeight:1 }}>{value}</p>
                      <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:8, color:"rgba(255,255,255,.4)", margin:"4px 0 0", textTransform:"uppercase", letterSpacing:"0.1em" }}>{label}</p>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* RIGHT — Clock, always on same row */}
            <div style={{ textAlign:"right", flexShrink:0 }}>
              <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:26, fontWeight:600, color:"#fff", letterSpacing:"0.04em", lineHeight:1 }}>
                {now.toLocaleTimeString("en-IN", { hour12:false })}
              </p>
              <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"rgba(255,255,255,.4)", marginTop:4 }}>
                {now.toLocaleDateString("en-IN", { day:"2-digit", month:"short", year:"numeric" })}
              </p>
              {lastAt && (
                <div style={{ display:"inline-flex", alignItems:"center", gap:4, marginTop:5, padding:"3px 10px", borderRadius:20, background:"rgba(255,255,255,.08)", border:"1px solid rgba(255,255,255,.1)" }}>
                  <span style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:9, color:"rgba(255,255,255,.4)" }}>Last sync {timeAgo(lastAt)}</span>
                </div>
              )}
            </div>

          </div>
        </div>

        {/* ── 4 STAT CARDS ────────────────────────────────────────────── */}
        <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:12 }}>
          {[
            {
              label:"Records Created", value: fmt(tCreated), icon:"✓",
              sub: lastAt ? `Last sync ${timeAgo(lastAt)}` : "No sync yet",
              accent:"#16a34a", accentL:"#dcfce7", gradient:"linear-gradient(135deg,#f0fdf4,#dcfce7)",
              delay:".05s",
            },
            {
              label:"Records Updated", value: fmt(tUpdated), icon:"↻",
              sub:"From last sync run",
              accent:"#2563eb", accentL:"#dbeafe", gradient:"linear-gradient(135deg,#eff6ff,#dbeafe)",
              delay:".1s",
            },
            {
              label:"Failed Records", value: fmt(tFailed), icon: tFailed > 0 ? "✗" : "✓",
              sub: tFailed > 0 ? "Check live logs" : "All clear",
              accent: tFailed > 0 ? "#dc2626" : "#16a34a",
              accentL: tFailed > 0 ? "#fee2e2" : "#dcfce7",
              gradient: tFailed > 0 ? "linear-gradient(135deg,#fef2f2,#fee2e2)" : "linear-gradient(135deg,#f0fdf4,#dcfce7)",
              delay:".15s",
            },
            {
              label:"Auto Sync", value: cfg.enabled ? "ON" : "OFF", icon:"⏱",
              sub: cfg.enabled ? `Every ${cfg.interval}` : "Not scheduled",
              accent: cfg.enabled ? "#0d9488" : "#94a3b8",
              accentL: cfg.enabled ? "#ccfbf1" : "#f1f5f9",
              gradient: cfg.enabled ? "linear-gradient(135deg,#f0fdfa,#ccfbf1)" : "linear-gradient(135deg,#f8fafc,#f1f5f9)",
              delay:".2s",
            },
          ].map(({ label, value, icon, sub, accent, accentL, gradient, delay }) => (
            <div key={label} className="db-stat-card" style={{ animation:`fadeUp .45s ease ${delay} both` }}>
              <div style={{ position:"absolute", top:0, right:0, width:90, height:90, borderRadius:"0 16px 0 90px", background:gradient, pointerEvents:"none" }} />
              <div style={{ position:"relative" }}>
                <div style={{
                  width:40, height:40, borderRadius:12,
                  background:accentL, border:`1.5px solid ${accent}22`,
                  display:"flex", alignItems:"center", justifyContent:"center",
                  fontSize:17, fontWeight:700, color:accent,
                  marginBottom:20,
                }}>{icon}</div>
                <p style={{ fontFamily:"'Manrope',sans-serif", fontSize:34, fontWeight:800, color:"#0f172a", letterSpacing:"-1.5px", lineHeight:1, animation:`countUp .5s ease ${delay} both` }}>
                  {value}
                </p>
                <p style={{ fontFamily:"'Manrope',sans-serif", fontSize:11, fontWeight:600, color:"#64748b", margin:"8px 0 4px", textTransform:"uppercase", letterSpacing:"0.07em" }}>
                  {label}
                </p>
                <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"#94a3b8" }}>{sub}</p>
                <div style={{ position:"absolute", bottom:-20, left:-24, right:-24, height:3, background:`linear-gradient(90deg,${accent},${accent}00)` }} />
              </div>
            </div>
          ))}
        </div>

        {/* ── LAST SYNC — FULL WIDTH ───────────────────────────────────── */}
        <div style={{
          background:"#fff", borderRadius:18, padding:"28px 32px",
          border:"1px solid #eef0f6",
          boxShadow:"0 4px 24px rgba(15,23,42,.08), 0 1px 3px rgba(15,23,42,.06)",
          animation:"fadeUp .45s ease .22s both",
        }}>
          {/* Header */}
          <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:28 }}>
            <div>
              <p style={{ fontFamily:"'Manrope',sans-serif", fontSize:13, fontWeight:800, color:"#0f172a", textTransform:"uppercase", letterSpacing:"0.08em" }}>Last Sync</p>
              {lastAt && (
                <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:11, color:"#94a3b8", marginTop:4 }}>
                  {new Date(lastAt).toLocaleString("en-IN")}
                </p>
              )}
            </div>
            {lastSync?.status && (
              <span style={{
                display:"inline-flex", alignItems:"center", gap:6,
                padding:"6px 16px", borderRadius:20,
                background:syncBg, border:`1.5px solid ${syncColor}30`,
                fontFamily:"'JetBrains Mono',monospace", fontSize:10, fontWeight:700, color:syncColor,
                textTransform:"uppercase", letterSpacing:"0.1em",
              }}>
                <span style={{ width:6, height:6, borderRadius:"50%", background:syncColor, boxShadow:`0 0 0 3px ${syncColor}25` }} />
                {syncLabel}
              </span>
            )}
          </div>

          {/* Body: Donut + Legend + Bars */}
          <div style={{ display:"grid", gridTemplateColumns:"180px 1fr", gap:48, alignItems:"center" }}>

            {/* Donut — larger */}
            <div style={{ display:"flex", flexDirection:"column", alignItems:"center", gap:12 }}>
              <Donut created={tCreated} updated={tUpdated} failed={tFailed} size={160} />
              <span style={{
                fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"#94a3b8",
                background:"#f8fafc", border:"1px solid #e2e8f0",
                padding:"3px 12px", borderRadius:20,
              }}>
                {fmt(tCreated + tUpdated + tFailed + tSkipped)} total records
              </span>
            </div>

            {/* Stats + bars */}
            <div style={{ display:"flex", flexDirection:"column", gap:18 }}>
              {[
                { l:"Created", v:tCreated, c:"#22c55e", bg:"#f0fdf4", border:"#bbf7d0" },
                { l:"Updated", v:tUpdated, c:"#3b82f6", bg:"#eff6ff", border:"#bfdbfe" },
                { l:"Failed",  v:tFailed,  c:"#ef4444", bg:"#fef2f2", border:"#fecaca" },
                { l:"Skipped", v:tSkipped, c:"#f59e0b", bg:"#fffbeb", border:"#fde68a" },
              ].map(({ l, v, c, bg, border }) => {
                const tot = tCreated + tUpdated + tFailed + tSkipped;
                const pct = tot > 0 ? Math.min(100, (v / tot) * 100) : 0;
                return (
                  <div key={l} style={{ display:"grid", gridTemplateColumns:"100px 1fr 72px", alignItems:"center", gap:16 }}>
                    {/* Label */}
                    <div style={{ display:"flex", alignItems:"center", gap:8 }}>
                      <span style={{ width:10, height:10, borderRadius:3, background:c, flexShrink:0, boxShadow:`0 0 0 2px ${c}25` }} />
                      <span style={{ fontFamily:"'Manrope',sans-serif", fontSize:13, fontWeight:600, color:"#475569" }}>{l}</span>
                    </div>
                    {/* Bar */}
                    <div style={{ height:10, borderRadius:5, background:"#f1f5f9", overflow:"hidden" }}>
                      <div style={{
                        height:"100%", width:`${pct}%`, borderRadius:5,
                        background:`linear-gradient(90deg,${c},${c}bb)`,
                        transition:"width 1.2s cubic-bezier(.4,0,.2,1)",
                        boxShadow:`0 1px 4px ${c}40`,
                      }} />
                    </div>
                    {/* Value chip */}
                    <span style={{
                      fontFamily:"'JetBrains Mono',monospace", fontSize:12, fontWeight:700,
                      color:c, background:bg, border:`1.5px solid ${border}`,
                      padding:"4px 12px", borderRadius:8, textAlign:"center",
                    }}>{fmt(v)}</span>
                  </div>
                );
              })}

              {/* Footer timestamp */}
              {lastAt && (
                <div style={{
                  display:"flex", alignItems:"center", gap:12, marginTop:8,
                  padding:"12px 16px", borderRadius:12,
                  background:"linear-gradient(135deg,#f8fafc,#f1f5f9)",
                  border:"1px solid #e2e8f0",
                }}>
                  <span style={{ fontSize:16 }}>🕐</span>
                  <div>
                    <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:12, fontWeight:600, color:"#334155" }}>
                      {new Date(lastAt).toLocaleString("en-IN")}
                    </p>
                    {lastSync?.fromDate && (
                      <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"#94a3b8", marginTop:3 }}>
                        Range: {lastSync.fromDate} → {lastSync.toDate}
                      </p>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* ── BOTTOM ROW ──────────────────────────────────────────────── */}
        <div style={{ display:"grid", gridTemplateColumns:"290px 1fr", gap:12 }}>

          {/* Auto-sync */}
          <div style={{ background:"#fff", borderRadius:16, padding:22, border:"1px solid #eef0f6", boxShadow:"0 2px 8px rgba(15,23,42,.06)", animation:"fadeUp .45s ease .34s both" }}>
            <p style={{ fontFamily:"'Manrope',sans-serif", fontSize:12, fontWeight:700, color:"#0f172a", textTransform:"uppercase", letterSpacing:"0.07em", marginBottom:14 }}>Auto Sync</p>

            <div style={{
              padding:"14px 16px", borderRadius:12, marginBottom:14,
              background: cfg.enabled ? "linear-gradient(135deg,#eff6ff,#dbeafe)" : "#f8fafc",
              border: `1px solid ${cfg.enabled ? "#bfdbfe" : "#e2e8f0"}`,
            }}>
              <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom: cfg.enabled && nextRun ? 12 : 0 }}>
                <span style={{ width:9, height:9, borderRadius:"50%", background: cfg.enabled ? "#2563eb" : "#94a3b8", boxShadow: cfg.enabled ? "0 0 0 3px rgba(37,99,235,.2)" : "none", flexShrink:0, animation: cfg.enabled ? "pulse 1.6s ease infinite" : "none" }} />
                <span style={{ fontFamily:"'Manrope',sans-serif", fontSize:13, fontWeight:700, color: cfg.enabled ? "#1d4ed8" : "#64748b" }}>
                  {cfg.enabled ? `Active — every ${cfg.interval}` : "Not scheduled"}
                </span>
              </div>
              {cfg.enabled && nextRun && (
                <div style={{ display:"flex", flexDirection:"column", gap:6 }}>
                  <div style={{ display:"flex", justifyContent:"space-between" }}>
                    <span style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"#64748b" }}>Next run in</span>
                    <span style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:11, fontWeight:600, color:"#2563eb" }}>{fmtTime(remMs)}</span>
                  </div>
                  <div style={{ height:5, borderRadius:3, background:"#bfdbfe", overflow:"hidden" }}>
                    <div style={{ height:"100%", borderRadius:3, background:"#2563eb", width:`${Math.max(2,100-(remMs/(cfg.intervalMs||3600000))*100)}%`, transition:"width 1s linear" }} />
                  </div>
                  <span style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:9, color:"#93c5fd" }}>
                    At {nextRun.toLocaleTimeString("en-IN",{hour12:false})}
                  </span>
                </div>
              )}
            </div>

            {[
              { label:"Tally Company",   value: currentCompany || "—"   },
              { label:"ERPNext URL",     value: shortUrl(erpUrl) || "—" },
              { label:"ERPNext Company", value: erpCompany || "—"       },
            ].map(({ label, value }) => (
              <div key={label} style={{ display:"flex", justifyContent:"space-between", alignItems:"center", padding:"8px 10px", borderRadius:8, background:"#f8fafc", marginBottom:5 }}>
                <span style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:9, color:"#94a3b8", textTransform:"uppercase", letterSpacing:"0.08em" }}>{label}</span>
                <span style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:11, fontWeight:600, color:"#334155", maxWidth:160, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{value}</span>
              </div>
            ))}
          </div>

          {/* History */}
          <div style={{ background:"#fff", borderRadius:16, padding:22, border:"1px solid #eef0f6", boxShadow:"0 2px 8px rgba(15,23,42,.06)", animation:"fadeUp .45s ease .4s both" }}>
            <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:14 }}>
              <p style={{ fontFamily:"'Manrope',sans-serif", fontSize:12, fontWeight:700, color:"#0f172a", textTransform:"uppercase", letterSpacing:"0.07em" }}>Sync History</p>
              <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                {erpUrl && <span style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:9, color:"#94a3b8" }}>{shortUrl(erpUrl)}</span>}
                {history.length > 0 && <span style={{ fontFamily:"'Manrope',sans-serif", fontSize:11, fontWeight:600, color:"#64748b", background:"#f1f5f9", padding:"2px 8px", borderRadius:20 }}>{history.length} runs</span>}
              </div>
            </div>

            {lastSync && (
              <div style={{
                display:"flex", alignItems:"center", gap:14,
                padding:"13px 16px", borderRadius:12, marginBottom:12,
                background: syncBg, border:`1.5px solid ${syncColor}25`,
              }}>
                <div style={{
                  width:38, height:38, borderRadius:10, flexShrink:0,
                  background: syncColor + "18",
                  display:"flex", alignItems:"center", justifyContent:"center",
                  fontSize:18, color:syncColor,
                }}>{isUD ? "✓" : lastSync.status==="failed" ? "✗" : "✓"}</div>
                <div style={{ flex:1 }}>
                  <p style={{ fontFamily:"'Manrope',sans-serif", fontSize:13, fontWeight:700, color:"#0f172a" }}>
                    {isUD ? "Already up to date" : lastSync.status==="failed" ? "Last sync failed" : "Last sync successful"}
                  </p>
                  {lastAt && <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"#64748b", marginTop:2 }}>{timeAgo(lastAt)} · {new Date(lastAt).toLocaleString("en-IN")}</p>}
                </div>
                <span style={{
                  fontFamily:"'JetBrains Mono',monospace", fontSize:9, fontWeight:700, color:syncColor,
                  padding:"3px 10px", borderRadius:20,
                  background: syncColor + "15", border:`1px solid ${syncColor}25`,
                  textTransform:"uppercase", letterSpacing:"0.08em",
                }}>{syncLabel}</span>
              </div>
            )}

                  {history.length === 0 && !lastSync ? (
              <div style={{ textAlign:"center", padding:"32px 0" }}>
                <div style={{ fontSize:28, opacity:.15, marginBottom:8 }}>⟳</div>
                <p style={{ fontFamily:"'Manrope',sans-serif", fontSize:13, color:"#94a3b8" }}>No sync runs yet</p>
              </div>
            ) : (
              <div style={{ display:"flex", flexDirection:"column", gap:4, maxHeight:260, overflowY:"auto", paddingRight:2 }}>
                {history.map((h, i) => {
                  const ud  = h.upToDate;
                  const fl  = h.status === "failed";
                  const c   = ud ? "#0d9488" : fl ? "#dc2626" : "#16a34a";
                  const bg  = ud ? "#f0fdfa" : fl ? "#fef2f2" : "#f0fdf4";
                  const at  = h.at ? new Date(h.at) : null;
                  const isAuto = h.trigger === "auto" || (!h.trigger);
                  return (
                    <div key={i} className="db-hist-row" style={{
                      display:"flex", alignItems:"center", gap:10,
                      padding:"9px 12px", borderRadius:9,
                      background:bg, border:`1px solid ${c}20`,
                    }}>
                      <span style={{ width:7, height:7, borderRadius:"50%", background:c, flexShrink:0, boxShadow:`0 0 0 2px ${c}25` }} />
                      <div style={{ flex:1 }}>
                        <div style={{ display:"flex", alignItems:"center", gap:6, flexWrap:"wrap" }}>
                          <span style={{ fontFamily:"'Manrope',sans-serif", fontSize:12, fontWeight:600, color:"#0f172a" }}>
                            {ud ? "Up to date" : fl ? "Failed" : "Success"}
                          </span>
                          {/* Auto / Manual badge */}
                          <span style={{
                            fontFamily:"'JetBrains Mono',monospace", fontSize:9, fontWeight:700,
                            padding:"1px 7px", borderRadius:5,
                            background: isAuto ? "rgba(99,102,241,.1)" : "rgba(16,185,129,.1)",
                            color: isAuto ? "#6366f1" : "#059669",
                            border: `1px solid ${isAuto ? "rgba(99,102,241,.2)" : "rgba(16,185,129,.2)"}`,
                          }}>{isAuto ? "AUTO" : "MANUAL"}</span>
                        </div>
                        {h.from && <span style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"#64748b", marginRight:6 }}>{h.from} → {h.to}</span>}
                        {(h.created || h.updated || h.failed || h.skipped) ? (
                          <span style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"#94a3b8" }}>
                            {h.created ? `+${fmt(h.created)} ` : ""}{h.updated ? `~${fmt(h.updated)} ` : ""}{h.failed ? `✗${fmt(h.failed)} ` : ""}{h.skipped ? `=${fmt(h.skipped)}` : ""}
                          </span>
                        ) : null}
                        {h.error && <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"#dc2626", marginTop:2 }}>{h.error}</p>}
                      </div>
                      {at && (
                        <div style={{ textAlign:"right", flexShrink:0 }}>
                          <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:10, color:"#475569" }}>{at.toLocaleTimeString("en-IN",{hour12:false})}</p>
                          <p style={{ fontFamily:"'JetBrains Mono',monospace", fontSize:9, color:"#94a3b8", marginTop:1 }}>{at.toLocaleDateString("en-IN")}</p>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>

      </div>
    </>
  );
}