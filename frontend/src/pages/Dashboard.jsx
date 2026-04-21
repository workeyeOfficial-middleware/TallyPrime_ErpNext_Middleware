import { useState, useEffect } from "react";
import { SyncToErpNext } from "./SyncToErpNext";

const BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:4000/api";

const C = {
  card:"#ffffff", surface:"#f8f9fc", bg:"#eef0f6", border:"#e4e7ef", borderH:"#c8cedd",
  ink:"#0c0e14", muted:"#6b7280", dim:"#9ca3af",
  accent:"#2563eb", accentD:"#1d4ed8", accentL:"#eff6ff", accentB:"#bfdbfe",
  green:"#16a34a", greenL:"#f0fdf4", greenB:"#bbf7d0",
  amber:"#d97706", amberL:"#fffbeb", amberB:"#fde68a",
  red:"#dc2626", redL:"#fef2f2", redB:"#fecaca",
  mono:"'JetBrains Mono','Fira Code',monospace",
  sans:"'DM Sans','Plus Jakarta Sans',sans-serif",
  title:"'Syne','Plus Jakarta Sans',sans-serif",
};

const NAV_ITEMS = [
  { id: "data-check",  icon: "○", label: "Data Check",  sub: "Validate connection" },
  { id: "quick-fetch", icon: "↓", label: "Quick Fetch", sub: "Pull tally data" },
  { id: "sync",        icon: "⇄", label: "Sync",        sub: "Push to ERPNext", active: true },
  { id: "live-logs",   icon: "≡", label: "Live Logs",   sub: "Monitor events" },
];

const PAGE_META = {
  "data-check":  { title: "Data Check",    sub: "Validate connection to Tally & ERPNext" },
  "quick-fetch": { title: "Quick Fetch",   sub: "Pull raw Tally data for inspection" },
  "sync":        { title: "Sync to ERPNext", sub: "Push Tally data into ERPNext" },
  "live-logs":   { title: "Live Logs",     sub: "Monitor sync events in real-time" },
};

// Derive display name: first selected company, fallback to first available
function deriveDisplayName(companies, selectedCompany) {
  if (selectedCompany) return selectedCompany;
  if (companies && companies.length > 0) return companies[0].name;
  return null;
}

// Get initials from a company name for the avatar
function getInitials(name) {
  if (!name) return "?";
  const words = name.trim().split(/\s+/);
  if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
  return (words[0][0] + words[words.length - 1][0]).toUpperCase();
}

export default function Dashboard() {
  const [activePage, setActivePage] = useState("sync");
  const [companies, setCompanies] = useState([]);
  const [companiesLoading, setCompaniesLoading] = useState(true);
  const [selectedCompany, setSelectedCompany] = useState(null);

  // Fetch Tally companies on mount — this drives the top-right company name
  useEffect(() => {
    setCompaniesLoading(true);
    fetch(`${BASE_URL}/tally/companies`)
      .then(r => r.json())
      .then(data => {
        const list = data?.data || [];
        setCompanies(list);
        // Restore last-used company from localStorage
        const last = localStorage.getItem("last_tally_company");
        const found = list.find(c => c.name === last);
        setSelectedCompany(found ? found.name : list[0]?.name || null);
      })
      .catch(() => {
        // Tally might be offline on load — that's fine, show nothing
        setCompanies([]);
        setSelectedCompany(null);
      })
      .finally(() => setCompaniesLoading(false));
  }, []);

  // When SyncToErpNext changes company, mirror it here for the header
  function handleCompanyChange(name) {
    if (name) {
      setSelectedCompany(name);
      localStorage.setItem("last_tally_company", name);
    }
  }

  const displayName = deriveDisplayName(companies, selectedCompany);
  const initials = getInitials(displayName);
  const meta = PAGE_META[activePage];

  return (
    <div style={{ display:"flex", height:"100vh", fontFamily:C.sans, background:C.bg, overflow:"hidden" }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;600;700&display=swap');
        * { box-sizing: border-box; margin:0; padding:0; }
        ::-webkit-scrollbar { width:5px; height:5px; }
        ::-webkit-scrollbar-track { background:transparent; }
        ::-webkit-scrollbar-thumb { background:${C.border}; border-radius:10px; }
        .nav-item:hover { background: rgba(255,255,255,0.08) !important; }
        .nav-item.active { background: rgba(255,255,255,0.13) !important; }
      `}</style>

      {/* ── Sidebar ── */}
      <aside style={{
        width:192, flexShrink:0, background:"linear-gradient(180deg,#1e293b 0%,#0f172a 100%)",
        display:"flex", flexDirection:"column", padding:"0 0 16px",
        boxShadow:"2px 0 16px rgba(0,0,0,.18)", zIndex:10,
      }}>
        {/* Logo */}
        <div style={{ padding:"22px 18px 20px", borderBottom:"1px solid rgba(255,255,255,.08)" }}>
          <div style={{ display:"flex", alignItems:"center", gap:10 }}>
            <div style={{
              width:34, height:34, borderRadius:9,
              background:"linear-gradient(135deg,#2563eb,#1d4ed8)",
              display:"flex", alignItems:"center", justifyContent:"center",
              boxShadow:"0 4px 12px rgba(37,99,235,.45)", flexShrink:0,
            }}>
              <span style={{ fontSize:16 }}>⇄</span>
            </div>
            <div>
              <p style={{ fontFamily:C.title, fontSize:13, fontWeight:800, color:"#fff", lineHeight:1.2 }}>Tally → ERP</p>
              <p style={{ fontFamily:C.mono, fontSize:9, color:"rgba(255,255,255,.38)", letterSpacing:"0.1em", textTransform:"uppercase" }}>MIDDLEWARE</p>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav style={{ flex:1, padding:"12px 10px", display:"flex", flexDirection:"column", gap:3 }}>
          {NAV_ITEMS.map(item => {
            const isActive = activePage === item.id;
            return (
              <button
                key={item.id}
                className={`nav-item${isActive ? " active" : ""}`}
                onClick={() => setActivePage(item.id)}
                style={{
                  display:"flex", alignItems:"center", gap:10, padding:"9px 10px",
                  borderRadius:9, border:"none", cursor:"pointer", textAlign:"left",
                  background: isActive ? "rgba(255,255,255,.13)" : "transparent",
                  transition:"background .15s",
                  borderLeft: isActive ? `3px solid ${C.accent}` : "3px solid transparent",
                }}
              >
                <div style={{
                  width:28, height:28, borderRadius:7, flexShrink:0,
                  background: isActive ? C.accent : "rgba(255,255,255,.07)",
                  display:"flex", alignItems:"center", justifyContent:"center",
                  fontSize:12, color:"#fff", transition:"background .15s",
                }}>{item.icon}</div>
                <div>
                  <p style={{ fontFamily:C.sans, fontSize:12, fontWeight:600, color:isActive?"#fff":"rgba(255,255,255,.65)", lineHeight:1.2 }}>{item.label}</p>
                  <p style={{ fontFamily:C.mono, fontSize:9, color:"rgba(255,255,255,.3)" }}>{item.sub}</p>
                </div>
              </button>
            );
          })}
        </nav>

        {/* Version pill */}
        <div style={{ padding:"0 14px" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7, padding:"7px 10px", borderRadius:8, background:"rgba(255,255,255,.05)" }}>
            <span style={{ width:7, height:7, borderRadius:"50%", background:C.green, flexShrink:0 }}/>
            <span style={{ fontFamily:C.mono, fontSize:9, color:"rgba(255,255,255,.35)" }}>v1.0 · online</span>
          </div>
        </div>
      </aside>

      {/* ── Main ── */}
      <div style={{ flex:1, display:"flex", flexDirection:"column", overflow:"hidden" }}>

        {/* Top bar */}
        <header style={{
          height:56, flexShrink:0, background:C.card,
          borderBottom:`1px solid ${C.border}`,
          display:"flex", alignItems:"center", justifyContent:"space-between",
          padding:"0 28px", boxShadow:"0 1px 4px rgba(0,0,0,.05)",
        }}>
          {/* Page title */}
          <div>
            <h1 style={{ fontFamily:C.title, fontSize:17, fontWeight:800, color:C.ink, letterSpacing:"-0.4px", lineHeight:1.2 }}>{meta.title}</h1>
            <p style={{ fontFamily:C.mono, fontSize:10, color:C.muted, marginTop:1 }}>{meta.sub}</p>
          </div>

          {/* Company badge — dynamic, never hardcoded */}
          <div style={{ display:"flex", alignItems:"center", gap:10 }}>
            {companiesLoading ? (
              <div style={{
                padding:"6px 14px", borderRadius:20, background:C.surface,
                border:`1px solid ${C.border}`,
              }}>
                <span style={{ fontFamily:C.mono, fontSize:10, color:C.dim }}>Loading…</span>
              </div>
            ) : displayName ? (
              <>
                <div style={{
                  padding:"5px 13px", borderRadius:20,
                  background:C.surface, border:`1px solid ${C.border}`,
                  maxWidth:220, overflow:"hidden",
                }}>
                  <p style={{
                    fontFamily:C.sans, fontSize:12, fontWeight:600, color:C.ink,
                    overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap",
                  }} title={displayName}>{displayName}</p>
                </div>
                {/* Avatar with initials */}
                <div style={{
                  width:32, height:32, borderRadius:"50%", flexShrink:0,
                  background:`linear-gradient(135deg,${C.accent},${C.accentD})`,
                  display:"flex", alignItems:"center", justifyContent:"center",
                  boxShadow:`0 2px 8px ${C.accent}44`,
                }}>
                  <span style={{ fontFamily:C.title, fontSize:11, fontWeight:800, color:"#fff" }}>{initials}</span>
                </div>
              </>
            ) : (
              <div style={{
                padding:"5px 13px", borderRadius:20,
                background:C.amberL, border:`1px solid ${C.amberB}`,
              }}>
                <span style={{ fontFamily:C.mono, fontSize:10, color:C.amber }}>⚠ Tally offline</span>
              </div>
            )}
          </div>
        </header>

        {/* Page content */}
        <main style={{ flex:1, overflowY:"auto", padding:"24px 28px" }}>
          {activePage === "sync" && (
            <SyncToErpNextWrapper
              companies={companies}
              onCompanyChange={handleCompanyChange}
            />
          )}
          {activePage === "data-check" && <PlaceholderPage icon="○" title="Data Check" desc="Validate your Tally and ERPNext connections before syncing." />}
          {activePage === "quick-fetch" && <PlaceholderPage icon="↓" title="Quick Fetch" desc="Pull raw Tally data for inspection and debugging." />}
          {activePage === "live-logs" && <PlaceholderPage icon="≡" title="Live Logs" desc="Monitor sync events and errors in real-time." />}
        </main>
      </div>
    </div>
  );
}

// Wrapper that intercepts company changes from SyncToErpNext
function SyncToErpNextWrapper({ companies, onCompanyChange }) {
  // We monkeypatch by wrapping companies with a Proxy-like trick:
  // SyncToErpNext calls setCompany internally. We detect the change
  // via a custom companies list that fires a callback on selection.
  // Since SyncToErpNext already saves to localStorage, we just
  // listen to storage changes + pass a sentinel prop.
  useEffect(() => {
    function onStorage(e) {
      if (e.key === "last_tally_company" && e.newValue) {
        onCompanyChange(e.newValue);
      }
    }
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, [onCompanyChange]);

  return <SyncToErpNext companies={companies} />;
}

function PlaceholderPage({ icon, title, desc }) {
  return (
    <div style={{
      display:"flex", flexDirection:"column", alignItems:"center", justifyContent:"center",
      height:"60vh", gap:14, opacity:0.5,
    }}>
      <div style={{
        width:56, height:56, borderRadius:14, background:C.surface,
        border:`1.5px solid ${C.border}`, display:"flex", alignItems:"center",
        justifyContent:"center", fontSize:24,
      }}>{icon}</div>
      <div style={{ textAlign:"center" }}>
        <p style={{ fontFamily:C.title, fontSize:15, fontWeight:800, color:C.ink }}>{title}</p>
        <p style={{ fontFamily:C.mono, fontSize:11, color:C.muted, marginTop:5 }}>{desc}</p>
      </div>
    </div>
  );
}