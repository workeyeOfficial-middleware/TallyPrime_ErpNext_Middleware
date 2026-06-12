// src/pages/SettingsPage.jsx
import { useState, useEffect } from "react";

const BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:4000/api";

const C = {
  card:"#ffffff", surface:"#f0f3fa", bg:"#e8edf6", border:"#d8dff0",
  ink:"#0a0e1a", muted:"#5a6482", dim:"#8a94b0",
  accent:"#2563eb", accentD:"#1d4ed8", accentL:"#eef4ff", accentB:"#b8d0fe",
  green:"#16a34a", greenD:"#15803d", greenL:"#f0fdf4", greenB:"#bbf7d0",
  amber:"#d97706", amberL:"#fffbeb", amberB:"#fde68a",
  red:"#dc2626", redL:"#fef2f2", redB:"#fecaca",
  mono:"'JetBrains Mono','Fira Code',monospace",
  sans:"'DM Sans','Plus Jakarta Sans',sans-serif",
  title:"'Syne','Plus Jakarta Sans',sans-serif",
};

// ── Storage keys (must match SyncToErpNext exactly) ──────────────────────────
const CREDS_KEY        = "erp_creds";         // per-company map  { companyName: {url,apiKey,apiSecret} }
const GLOBAL_CREDS_KEY = "erp_global_creds";  // flat global creds (legacy / fallback)
const COMPANY_MAP_KEY  = "erp_company_map";   // { tallyCompany: erpnextCompanyName }

function loadAllPerCompanyCreds() {
  try { return JSON.parse(localStorage.getItem(CREDS_KEY) || "{}"); } catch { return {}; }
}
function loadGlobalCreds() {
  try { return JSON.parse(localStorage.getItem(GLOBAL_CREDS_KEY) || "{}"); } catch { return {}; }
}
function loadCompanyMap() {
  try { return JSON.parse(localStorage.getItem(COMPANY_MAP_KEY) || "{}"); } catch { return {}; }
}

/**
 * Save credentials for a specific Tally company.
 * Also keeps erp_global_creds in sync so the fallback in SyncToErpNext still works.
 */
function saveCredsForCompany(companyName, creds) {
  // 1. per-company map
  const all = loadAllPerCompanyCreds();
  all[companyName] = creds;
  localStorage.setItem(CREDS_KEY, JSON.stringify(all));

  // 2. global fallback (url/key/secret only — no company name here)
  const global = { url: creds.url, apiKey: creds.apiKey, apiSecret: creds.apiSecret };
  localStorage.setItem(GLOBAL_CREDS_KEY, JSON.stringify(global));
}

/**
 * Save ERPNext company name mapping for a Tally company.
 */
function saveErpCompanyMapping(tallyCompany, erpnextCompany) {
  const map = loadCompanyMap();
  map[tallyCompany] = erpnextCompany;
  localStorage.setItem(COMPANY_MAP_KEY, JSON.stringify(map));
}

const inp = (extra = {}) => ({
  width: "100%", padding: "10px 14px",
  border: `1.5px solid ${C.border}`, borderRadius: 9,
  fontFamily: C.mono, fontSize: 13, color: C.ink,
  background: C.surface, outline: "none",
  transition: "border-color .15s, background .15s, box-shadow .15s",
  boxSizing: "border-box", ...extra,
});
const onFocus = (e) => {
  e.target.style.borderColor = C.accent;
  e.target.style.background  = C.card;
  e.target.style.boxShadow   = `0 0 0 3px ${C.accentB}55`;
};
const onBlur = (e) => {
  e.target.style.borderColor = C.border;
  e.target.style.background  = C.surface;
  e.target.style.boxShadow   = "none";
};

function Spinner({ size = 14, color = C.accent }) {
  return (
    <span style={{
      display:"inline-block", width:size, height:size, borderRadius:"50%",
      border:`2px solid ${color}20`, borderTopColor:color,
      animation:"sp-spin .7s linear infinite", flexShrink:0,
    }} />
  );
}

// ── Per-company credential row ────────────────────────────────────────────────
function CompanyCredRow({ companyName, erpMap, onSaved }) {
  const allCreds  = loadAllPerCompanyCreds();
  const saved     = allCreds[companyName] || {};
  const hasCreds  = !!(saved.url && saved.apiKey && saved.apiSecret);
  const erpSaved  = erpMap[companyName] || "";

  const [open,    setOpen]    = useState(!hasCreds);
  const [url,     setUrl]     = useState(saved.url       || "");
  const [key,     setKey]     = useState(saved.apiKey    || "");
  const [secret,  setSecret]  = useState(saved.apiSecret || "");
  const [erpCo,   setErpCo]   = useState(erpSaved);
  const [savedOk, setSavedOk] = useState(false);
  const [ping,    setPing]    = useState(null);
  const [pinging, setPinging] = useState(false);

  // re-init if companyName changes
  useEffect(() => {
    const c = loadAllPerCompanyCreds()[companyName] || {};
    setUrl(c.url || ""); setKey(c.apiKey || ""); setSecret(c.apiSecret || "");
    setErpCo(loadCompanyMap()[companyName] || "");
    setSavedOk(false); setPing(null);
  }, [companyName]);

  function handleSave() {
    const creds = { url: url.trim(), apiKey: key.trim(), apiSecret: secret.trim() };
    saveCredsForCompany(companyName, creds);
    saveErpCompanyMapping(companyName, erpCo.trim());
    setSavedOk(true);
    onSaved && onSaved(creds);
    setTimeout(() => setSavedOk(false), 2500);
  }

  async function handlePing() {
    setPinging(true); setPing(null);
    try {
      const res = await fetch(`${BASE_URL}/erpnext/ping`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ erpnextUrl: url.trim(), erpnextApiKey: key.trim(), erpnextApiSecret: secret.trim() }),
      });
      setPing(await res.json());
    } catch (e) {
      setPing({ connected: false, error: e.message });
    } finally {
      setPinging(false);
    }
  }

  const canSave = !!(url && key && secret && erpCo);

  return (
    <div style={{
      border: `1.5px solid ${hasCreds ? C.greenB : C.amberB}`,
      borderRadius: 11, overflow: "hidden", marginBottom: 10,
      background: C.card,
    }}>
      {/* ── Collapsed header ── */}
      <button
        onClick={() => setOpen(s => !s)}
        style={{
          width: "100%", display: "flex", alignItems: "center", gap: 11,
          padding: "12px 16px", background: "none", border: "none",
          cursor: "pointer", textAlign: "left",
        }}
      >
        <div style={{
          width: 32, height: 32, borderRadius: 8, flexShrink: 0,
          background: hasCreds ? C.greenL : C.amberL,
          border: `1.5px solid ${hasCreds ? C.greenB : C.amberB}`,
          display: "flex", alignItems: "center", justifyContent: "center", fontSize: 15,
        }}>
          {hasCreds ? "🔐" : "⚙️"}
        </div>
        <div style={{ flex: 1 }}>
          <p style={{ fontFamily: C.title, fontSize: 12, fontWeight: 700, color: C.ink, margin: 0 }}>
            {companyName}
          </p>
          <p style={{ fontFamily: C.mono, fontSize: 10, color: hasCreds ? C.green : C.amber, margin: "2px 0 0" }}>
            {hasCreds ? `✓ ${saved.url}${erpSaved ? ` → ${erpSaved}` : ""}` : "⚠ No credentials saved"}
          </p>
        </div>
        <span style={{
          fontFamily: C.mono, fontSize: 10, color: C.dim,
          transform: open ? "rotate(90deg)" : "none",
          transition: "transform .2s", flexShrink: 0,
        }}>▶</span>
      </button>

      {/* ── Expanded form ── */}
      {open && (
        <div style={{ padding: "0 16px 16px", display: "flex", flexDirection: "column", gap: 12, borderTop: `1px solid ${C.border}`, background: C.surface }}>
          <p style={{ fontFamily: C.mono, fontSize: 10, color: C.muted, paddingTop: 12, margin: 0, lineHeight: 1.6 }}>
            Credentials are saved locally per Tally company and used during sync.
          </p>

          {/* URL */}
          <div>
            <label style={{ display:"block", fontFamily:C.mono, fontSize:9, color:C.muted, textTransform:"uppercase", letterSpacing:"0.12em", marginBottom:5, fontWeight:700 }}>ERPNext URL</label>
            <input value={url} onChange={e => setUrl(e.target.value)} placeholder="https://yoursite.frappe.cloud"
              style={inp({ fontFamily: C.mono, fontSize: 12 })} onFocus={onFocus} onBlur={onBlur} />
          </div>

          {/* Key + Secret */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
            {[
              ["API Key",    key,    setKey,    "api_key",    "text"],
              ["API Secret", secret, setSecret, "api_secret", "password"],
            ].map(([lbl, val, setter, ph, type]) => (
              <div key={lbl}>
                <label style={{ display:"block", fontFamily:C.mono, fontSize:9, color:C.muted, textTransform:"uppercase", letterSpacing:"0.12em", marginBottom:5, fontWeight:700 }}>{lbl}</label>
                <input value={val} onChange={e => setter(e.target.value)} type={type} placeholder={ph}
                  style={inp({ fontFamily: C.mono, fontSize: 12 })} onFocus={onFocus} onBlur={onBlur} />
              </div>
            ))}
          </div>

          {/* ERPNext Company Name */}
          <div>
            <label style={{ display:"block", fontFamily:C.mono, fontSize:9, color:C.muted, textTransform:"uppercase", letterSpacing:"0.12em", marginBottom:5, fontWeight:700 }}>
              ERPNext Company Name <span style={{ color: C.amber }}>— must match exactly</span>
            </label>
            <input value={erpCo} onChange={e => setErpCo(e.target.value)} placeholder="e.g. Test Company"
              style={inp({ borderColor: erpCo ? C.greenB : C.border, background: erpCo ? C.greenL : C.surface })}
              onFocus={onFocus} onBlur={onBlur} />
            {erpCo && (
              <p style={{ fontFamily: C.mono, fontSize: 10, color: C.green, margin: "5px 0 0" }}>
                ✓ Will sync to: <strong>{erpCo}</strong>
              </p>
            )}
          </div>

          {/* Buttons */}
          <div style={{ display: "flex", gap: 9, marginTop: 2 }}>
            <button onClick={handleSave} disabled={!canSave}
              style={{
                flex: 1, padding: "10px 16px", borderRadius: 9, border: "none",
                background: savedOk ? C.green : !canSave ? C.surface : C.accentD,
                color: !canSave ? C.dim : "#fff",
                fontFamily: C.title, fontSize: 12, fontWeight: 700,
                cursor: !canSave ? "not-allowed" : "pointer", transition: "background .2s",
              }}>
              {savedOk ? "✓ Saved!" : "Save Credentials"}
            </button>
            <button onClick={handlePing} disabled={pinging || !url || !key || !secret}
              style={{
                display: "flex", alignItems: "center", gap: 7, padding: "10px 16px",
                borderRadius: 9, background: C.accentL, border: `1.5px solid ${C.accentB}`,
                color: C.accentD, fontFamily: C.mono, fontSize: 11, fontWeight: 600,
                cursor: pinging || !url || !key || !secret ? "not-allowed" : "pointer",
                opacity: !url || !key || !secret ? 0.5 : 1, transition: "all .15s", flexShrink: 0,
              }}>
              {pinging ? <><Spinner size={11} /> Testing…</> : "🔌 Test"}
            </button>
          </div>

          {/* Ping result */}
          {ping && (
            <div style={{
              padding: "10px 14px", borderRadius: 9,
              background: ping.connected ? C.greenL : C.redL,
              border: `1.5px solid ${ping.connected ? C.greenB : C.redB}`,
            }}>
              <p style={{ fontFamily: C.mono, fontSize: 11, color: ping.connected ? C.green : C.red, margin: 0 }}>
                {ping.connected
                  ? `✓ Connected as ${ping.user} · ${ping.latencyMs}ms`
                  : `✗ ${ping.error}`}
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Main Settings page ────────────────────────────────────────────────────────
export function SettingsPage({ companies = [], selectedCompany = "" }) {
  const [erpMap, setErpMap] = useState(loadCompanyMap);
  const [licenseActive, setLicenseActive] = useState(false);

  // Re-check license status whenever component renders or localStorage changes
  useEffect(() => {
    function checkLicense() {
      const hasExistingCreds = (() => {
        try {
          const creds = JSON.parse(localStorage.getItem("erp_creds") || "{}");
          return Object.values(creds).some(c => c?.url && c?.apiKey && c?.apiSecret);
        } catch { return false; }
      })();
      const active = (localStorage.getItem("license_status") === "active"
        && !!localStorage.getItem("license_email"))
        || hasExistingCreds;
      setLicenseActive(active);
    }
    checkLicense();
    // Poll every second so it updates immediately after LicenseSection saves
    const t = setInterval(checkLicense, 1000);
    return () => clearInterval(t);
  }, []);

  const isLicenseActive = licenseActive;

  // Active company = selectedCompany prop (set from Sync page) or last used or first available
  const resolvedCompany = selectedCompany
    || localStorage.getItem("last_tally_company")
    || (companies?.[0]?.name || companies?.[0] || "");

  function handleSaved() {
    setErpMap(loadCompanyMap());
  }

  const card = {
    background: C.card, border: `1.5px solid ${C.border}`,
    borderRadius: 14, padding: 24,
    boxShadow: "0 4px 24px rgba(13,21,50,.08)",
    marginBottom: 16,
  };

  return (
    <div style={{ fontFamily: C.sans }}>
      <style>{`@keyframes sp-spin{to{transform:rotate(360deg)}}`}</style>

      {/* ── ERPNext Credentials ── */}
      <div style={{ ...card, position: "relative", overflow: "hidden" }}>
        <div style={{
          display: "flex", alignItems: "center", gap: 12,
          marginBottom: 20, paddingBottom: 16, borderBottom: `1px solid ${C.border}`,
        }}>
          <div style={{
            width: 36, height: 36, borderRadius: 10,
            background: isLicenseActive ? C.accentL : C.surface,
            border: `1.5px solid ${isLicenseActive ? C.accentB : C.border}`,
            display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18,
          }}>{isLicenseActive ? "🔐" : "🔒"}</div>
          <div>
            <h2 style={{ fontFamily: C.title, fontSize: 15, fontWeight: 800, color: C.ink, margin: 0, letterSpacing: "-0.3px" }}>
              ERPNext Credentials
            </h2>
            <p style={{ fontFamily: C.mono, fontSize: 10, color: isLicenseActive ? C.muted : C.amber, margin: "3px 0 0" }}>
              {isLicenseActive
                ? (resolvedCompany
                    ? <>For Tally company: <strong>{resolvedCompany}</strong></>
                    : "Saved locally per Tally company — never sent to any server")
                : "⚠ Activate your license below to connect ERPNext"}
            </p>
          </div>
        </div>

        {/* Lock overlay when license not active */}
        {!isLicenseActive ? (
          <div style={{
            padding: "24px 20px", borderRadius: 10,
            background: C.surface, border: `1.5px dashed ${C.border}`,
            display: "flex", flexDirection: "column", alignItems: "center",
            gap: 10, textAlign: "center",
          }}>
            <span style={{ fontSize: 32 }}>🔒</span>
            <p style={{ fontFamily: C.title, fontSize: 14, fontWeight: 700, color: C.ink, margin: 0 }}>
              License Required
            </p>
            <p style={{ fontFamily: C.mono, fontSize: 10, color: C.muted, margin: 0, lineHeight: 1.7, maxWidth: 360 }}>
              Enter your registered email in the <strong>License</strong> section below and verify it to unlock ERPNext connection.
            </p>
          </div>
        ) : !resolvedCompany ? (
          <div style={{ padding: "14px 16px", borderRadius: 10, background: C.amberL, border: `1px solid ${C.amberB}` }}>
            <p style={{ fontFamily: C.mono, fontSize: 11, color: C.amber, margin: 0, lineHeight: 1.65 }}>
              No Tally company selected. Go to <strong>Sync</strong> and select a company first.
            </p>
          </div>
        ) : (
          <CompanyCredRow
            key={resolvedCompany}
            companyName={resolvedCompany}
            erpMap={erpMap}
            onSaved={handleSaved}
          />
        )}
      </div>

      {/* ── License ── */}
      <LicenseSection />
    </div>
  );
}

const FEATURES = [
  { icon: "🔄", text: "Tally to ERPNext Data Sync" },
  { icon: "🗂", text: "Chart of Accounts Sync" },
  { icon: "👥", text: "Ledger Sync" },
  { icon: "💰", text: "Opening Balance Sync" },
  { icon: "📦", text: "Inventory & Warehouse Sync" },
  { icon: "🧾", text: "Invoice & Voucher Sync" },
  { icon: "⏱", text: "Manual & Auto Sync" },
  { icon: "🏢", text: "Multi-Company Support" },
  { icon: "📊", text: "Live Logs & Monitoring" },
  { icon: "📅", text: "Date Range Sync" },
  { icon: "🔌", text: "Connection Testing & Validation" },
];

function LicenseSection() {
  const [email, setEmail]         = useState("");
  const [emailSubmitted, setEmailSubmitted] = useState(
    () => !!localStorage.getItem("license_email") && localStorage.getItem("license_status") === "active"
  );
  const [verifying, setVerifying] = useState(false);
  const [emailError, setEmailError] = useState("");
  const [licenseKey, setLicenseKey] = useState(
    () => localStorage.getItem("license_key") || ""
  );
  const [activating, setActivating] = useState(false);
  const [activated, setActivated]   = useState(
    () => localStorage.getItem("license_status") === "active" && !!localStorage.getItem("license_email")
  );
  const licPlan   = localStorage.getItem("license_plan")  || "Pro";
  const licEnd    = localStorage.getItem("license_end")   || "";
  const [activateMsg, setActivateMsg] = useState("");
  const savedEmail = localStorage.getItem("license_email") || "";

  async function handleVerifyEmail() {
    if (!email.trim() || !email.includes("@")) {
      setEmailError("Please enter a valid email address.");
      return;
    }
    setVerifying(true);
    setEmailError("");
    try {
      const res  = await fetch(`${BASE_URL}/license/verify-email`, {
        method : "POST",
        headers: { "Content-Type": "application/json" },
        body   : JSON.stringify({ email: email.trim().toLowerCase() }),
      });
      const data = await res.json();
      if (data.valid) {
        localStorage.setItem("license_email",  email.trim().toLowerCase());
        localStorage.setItem("license_plan",   data.plan   || "Pro");
        localStorage.setItem("license_end",    data.endDate|| "");
        localStorage.setItem("license_status", "active");
        setEmailSubmitted(true);
        setActivated(true);
      } else {
        setEmailError(data.reason || "No active license found for this email. Please purchase at our website.");
      }
    } catch (e) {
      setEmailError("Could not reach license server. Please check your internet connection.");
    } finally {
      setVerifying(false);
    }
  }

  function handleActivate() {
    // License is validated via email — no separate key needed for now
    // This is kept for future key-based flow
  }

  function handleReset() {
    localStorage.removeItem("license_email");
    localStorage.removeItem("license_key");
    localStorage.removeItem("license_status");
    setEmailSubmitted(false);
    setActivated(false);
    setEmail("");
    setLicenseKey("");
    setActivateMsg("");
  }

  const card = {
    background: C.card, border: `1.5px solid ${C.border}`,
    borderRadius: 14, padding: 24,
    boxShadow: "0 4px 24px rgba(13,21,50,.08)",
    marginBottom: 16,
  };

  return (
    <div style={card}>
      {/* ── Header ── */}
      <div style={{
        display: "flex", alignItems: "center", gap: 12,
        marginBottom: 20, paddingBottom: 16, borderBottom: `1px solid ${C.border}`,
      }}>
        <div style={{
          width: 36, height: 36, borderRadius: 10,
          background: activated ? C.greenL : C.accentL,
          border: `1.5px solid ${activated ? C.greenB : C.accentB}`,
          display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18,
        }}>{activated ? "✓" : "🔑"}</div>
        <div style={{ flex: 1 }}>
          <h2 style={{ fontFamily: C.title, fontSize: 15, fontWeight: 800, color: C.ink, margin: 0, letterSpacing: "-0.3px" }}>
            License
          </h2>
          <p style={{ fontFamily: C.mono, fontSize: 10, color: activated ? C.green : C.muted, margin: "3px 0 0" }}>
            {activated ? `✓ Active — ${savedEmail}` : "Enter your registered email to activate"}
          </p>
        </div>
        {activated && (
          <button onClick={handleReset} style={{
            padding: "4px 10px", borderRadius: 6, border: `1px solid ${C.border}`,
            background: "none", color: C.muted, fontFamily: C.mono, fontSize: 9,
            fontWeight: 700, cursor: "pointer",
          }}>Reset</button>
        )}
      </div>

      {/* ── Plan Card ── */}
      <div style={{
        borderRadius: 12, overflow: "hidden",
        border: `1.5px solid ${activated ? C.greenB : C.accentB}`,
        marginBottom: 20,
        boxShadow: activated ? `0 4px 20px ${C.green}18` : `0 4px 20px ${C.accent}18`,
      }}>
        {/* Plan header */}
        <div style={{
          background: activated
            ? `linear-gradient(135deg, ${C.green}, ${C.greenD})`
            : `linear-gradient(135deg, ${C.accent}, ${C.accentD})`,
          padding: "18px 22px",
          display: "flex", alignItems: "center", justifyContent: "space-between",
        }}>
          <div>
            <p style={{ fontFamily: C.title, fontSize: 18, fontWeight: 800, color: "#fff", margin: 0, letterSpacing: "-0.4px" }}>
              Tally → ERPNext Middleware
            </p>
            <p style={{ fontFamily: C.mono, fontSize: 10, color: "rgba(255,255,255,0.6)", margin: "4px 0 0" }}>
              One-time license · Lifetime updates
            </p>
          </div>
          <div style={{ textAlign: "right" }}>
            <p style={{ fontFamily: C.title, fontSize: 26, fontWeight: 800, color: "#fff", margin: 0, letterSpacing: "-1px" }}>
              ₹15,000
            </p>
            {activated && (
              <span style={{
                fontFamily: C.mono, fontSize: 9, fontWeight: 700,
                background: "rgba(255,255,255,0.2)", color: "#fff",
                padding: "2px 8px", borderRadius: 20, letterSpacing: "0.1em",
              }}>ACTIVE</span>
            )}
          </div>
        </div>

        {/* Features grid */}
        <div style={{
          padding: "18px 22px",
          background: C.surface,
          display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px 20px",
        }}>
          {FEATURES.map((f, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 9 }}>
              <span style={{
                width: 22, height: 22, borderRadius: 6, flexShrink: 0,
                background: activated ? C.greenL : C.accentL,
                border: `1px solid ${activated ? C.greenB : C.accentB}`,
                display: "flex", alignItems: "center", justifyContent: "center", fontSize: 11,
              }}>{f.icon}</span>
              <span style={{ fontFamily: C.sans, fontSize: 12, color: C.ink, fontWeight: 500 }}>{f.text}</span>
            </div>
          ))}
        </div>
      </div>

      {/* ── Step 1: Email verification ── */}
      {!emailSubmitted && (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <label style={{ fontFamily: C.mono, fontSize: 9, color: C.muted, textTransform: "uppercase", letterSpacing: "0.12em", fontWeight: 700 }}>
            Step 1 — Enter Registered Email
          </label>
          <p style={{ fontFamily: C.mono, fontSize: 10, color: C.muted, margin: 0, lineHeight: 1.6 }}>
            Use the same email you used when purchasing on our website.
          </p>
          <div style={{ display: "flex", gap: 9 }}>
            <input
              value={email} onChange={e => setEmail(e.target.value)}
              onKeyDown={e => e.key === "Enter" && handleVerifyEmail()}
              placeholder="you@example.com"
              style={inp({ flex: 1, fontFamily: C.mono, fontSize: 12 })}
              onFocus={onFocus} onBlur={onBlur}
            />
            <button onClick={handleVerifyEmail} disabled={verifying || !email.trim()}
              style={{
                display: "flex", alignItems: "center", gap: 7,
                padding: "10px 18px", borderRadius: 9, border: "none",
                background: !email.trim() ? C.surface : C.accentD,
                color: !email.trim() ? C.dim : "#fff",
                fontFamily: C.title, fontSize: 12, fontWeight: 700,
                cursor: !email.trim() ? "not-allowed" : "pointer",
                flexShrink: 0, transition: "all .15s",
              }}>
              {verifying ? <><Spinner size={11} color="#fff" /> Verifying…</> : "Verify →"}
            </button>
          </div>
          {emailError && (
            <p style={{ fontFamily: C.mono, fontSize: 10, color: C.red, margin: 0 }}>{emailError}</p>
          )}
        </div>
      )}



      {/* ── Activated state ── */}
      {activated && (
        <div style={{
          padding: "14px 18px", borderRadius: 10,
          background: C.greenL, border: `1.5px solid ${C.greenB}`,
          display: "flex", alignItems: "center", gap: 12,
        }}>
          <div style={{
            width: 36, height: 36, borderRadius: 9, flexShrink: 0,
            background: C.green, display: "flex", alignItems: "center",
            justifyContent: "center", fontSize: 18, color: "#fff",
          }}>✓</div>
          <div style={{ flex: 1 }}>
            <p style={{ fontFamily: C.title, fontSize: 13, fontWeight: 800, color: C.green, margin: 0 }}>
              License Active — {licPlan}
            </p>
            <p style={{ fontFamily: C.mono, fontSize: 10, color: C.green, margin: "3px 0 0", opacity: 0.8 }}>
              {savedEmail}{licEnd ? ` · Expires: ${new Date(licEnd).toLocaleDateString("en-IN")}` : ""}
            </p>
          </div>
        </div>
      )}
    </div>
  );
}