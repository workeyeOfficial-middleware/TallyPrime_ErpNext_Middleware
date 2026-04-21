import { useState } from "react";

import { SyncToErpNext } from "./pages/SyncToErpNext";
import { MiddlewareCheck } from "./pages/MiddlewareCheck";
import { QuickFetch } from "./pages/QuickFetch";
import { LiveLogs } from "./pages/LiveLogs";

const NAV = [
  { id: "check", label: "Data Check",  icon: "⬡", desc: "Validate connection" },
  { id: "fetch", label: "Quick Fetch", icon: "⇅", desc: "Pull tally data"    },
  { id: "sync",  label: "Sync",        icon: "⟳", desc: "Push to ERPNext"   },
  { id: "logs",  label: "Live Logs",   icon: "≡", desc: "Monitor events"    },
];

const PAGE_META = {
  check: { title: "Data Check",      sub: "Validates full Tally data connection" },
  fetch: { title: "Quick Fetch",     sub: "Pull records directly from Tally"     },
  sync:  { title: "Sync to ERPNext", sub: "Push Tally data into ERPNext"         },
  logs:  { title: "Live Logs",       sub: "Real-time middleware event stream"     },
};

export default function DashboardWrapper({ companies }) {
  const [activeTab, setActiveTab] = useState("check");
  const meta = PAGE_META[activeTab];

  return (
    <div style={{
      display: "flex",
      height: "100vh",
      background: "linear-gradient(145deg, #dde6f5 0%, #eef2f7 50%, #e8edf6 100%)",
      fontFamily: "'Plus Jakarta Sans', sans-serif",
      overflow: "hidden",
    }}>

      {/* ── SIDEBAR ── */}
      <aside style={{
        width: 248,
        background: "linear-gradient(175deg, #1e3a8a 0%, #1d4ed8 55%, #2563eb 100%)",
        display: "flex",
        flexDirection: "column",
        flexShrink: 0,
        boxShadow: "6px 0 32px rgba(30,58,138,0.22), 2px 0 0 rgba(255,255,255,0.1) inset",
        position: "relative",
        zIndex: 10,
      }}>
        {/* Shine overlay */}
        <div style={{
          position: "absolute", top: 0, left: 0, right: 0, height: 140,
          background: "linear-gradient(180deg, rgba(255,255,255,0.14) 0%, transparent 100%)",
          pointerEvents: "none",
        }} />

        {/* Logo area */}
        <div style={{
          padding: "22px 20px 18px",
          borderBottom: "1px solid rgba(255,255,255,0.1)",
          position: "relative",
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div style={{
              width: 44, height: 44, borderRadius: 13,
              background: "rgba(255,255,255,0.14)",
              border: "1px solid rgba(255,255,255,0.28)",
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 20,
              boxShadow: "0 6px 16px rgba(0,0,0,0.22), 0 1px 0 rgba(255,255,255,0.25) inset",
            }}>⟳</div>
            <div>
              <div style={{
                fontSize: 17, fontWeight: 800, color: "#fff",
                letterSpacing: "-0.4px",
                textShadow: "0 2px 4px rgba(0,0,0,0.25)",
              }}>Tally → ERP</div>
              <div style={{
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: 9, color: "rgba(255,255,255,0.45)",
                letterSpacing: "0.18em", marginTop: 2,
              }}>MIDDLEWARE</div>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav style={{ padding: "14px 12px", flex: 1, display: "flex", flexDirection: "column", gap: 5 }}>
          {NAV.map(({ id, label, icon, desc }) => {
            const active = activeTab === id;
            return (
              <button
                key={id}
                onClick={() => setActiveTab(id)}
                style={{
                  width: "100%", textAlign: "left",
                  padding: "13px 14px", borderRadius: 13,
                  border: active ? "1px solid rgba(255,255,255,0.28)" : "1px solid transparent",
                  cursor: "pointer",
                  display: "flex", alignItems: "center", gap: 12,
                  background: active ? "rgba(255,255,255,0.18)" : "transparent",
                  boxShadow: active
                    ? "0 6px 18px rgba(0,0,0,0.18), 0 1px 0 rgba(255,255,255,0.22) inset"
                    : "none",
                  transition: "all 0.2s ease",
                  position: "relative",
                }}
                onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = "rgba(255,255,255,0.09)"; }}
                onMouseLeave={(e) => { if (!active) e.currentTarget.style.background = "transparent"; }}
              >
                <span style={{
                  width: 36, height: 36, borderRadius: 10,
                  background: active ? "rgba(255,255,255,0.22)" : "rgba(255,255,255,0.08)",
                  border: "1px solid rgba(255,255,255,0.16)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 16, flexShrink: 0,
                  boxShadow: active ? "0 3px 8px rgba(0,0,0,0.2)" : "none",
                  transition: "all 0.2s",
                }}>
                  {icon}
                </span>
                <div>
                  <div style={{
                    fontSize: 15, fontWeight: active ? 700 : 500,
                    color: active ? "#fff" : "rgba(255,255,255,0.72)",
                    letterSpacing: "-0.2px",
                    textShadow: active ? "0 1px 4px rgba(0,0,0,0.2)" : "none",
                  }}>
                    {label}
                  </div>
                  <div style={{
                    fontSize: 11, color: "rgba(255,255,255,0.38)",
                    marginTop: 1, fontWeight: 400,
                  }}>
                    {desc}
                  </div>
                </div>
              </button>
            );
          })}
        </nav>

        {/* Footer */}
        <div style={{
          padding: "14px 20px",
          borderTop: "1px solid rgba(255,255,255,0.1)",
          display: "flex", alignItems: "center", gap: 8,
        }}>
          <div style={{
            width: 8, height: 8, borderRadius: "50%",
            background: "#4ade80",
            boxShadow: "0 0 8px rgba(74,222,128,0.8)",
          }} />
          <span style={{
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 10, color: "rgba(255,255,255,0.4)",
          }}>
            v1.0 · online
          </span>
        </div>
      </aside>

      {/* ── MAIN ── */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>

        {/* Topbar */}
        <header style={{
          background: "rgba(255,255,255,0.8)",
          backdropFilter: "blur(16px)",
          borderBottom: "1px solid rgba(255,255,255,0.95)",
          boxShadow: "0 2px 20px rgba(0,0,0,0.07)",
          display: "flex", alignItems: "center", justifyContent: "space-between",
          padding: "0 32px", height: 70, flexShrink: 0,
        }}>
          <div>
            <h2 style={{
              fontSize: 24, fontWeight: 800, color: "#0f172a",
              letterSpacing: "-0.6px", lineHeight: 1.1,
            }}>
              {meta.title}
            </h2>
            <p style={{ fontSize: 13, color: "#64748b", marginTop: 3, fontWeight: 500 }}>
              {meta.sub}
            </p>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div style={{
              fontSize: 13, fontWeight: 600, color: "#374151",
              background: "#fff",
              border: "1px solid #e2e8ef",
              borderRadius: 10, padding: "9px 16px",
              boxShadow: "0 2px 8px rgba(0,0,0,0.05), 0 1px 0 #fff inset",
            }}>
              Rajlaxmi Solutions Pvt. Ltd.
            </div>
            <div style={{
              width: 42, height: 42,
              background: "linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%)",
              color: "#fff", borderRadius: 13,
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 17, fontWeight: 800,
              boxShadow: "0 4px 16px rgba(37,99,235,0.42), 0 1px 0 rgba(255,255,255,0.25) inset",
            }}>
              R
            </div>
          </div>
        </header>

        {/* Content */}
        <main style={{ flex: 1, overflowY: "auto", padding: "28px 32px" }}>
          <div style={{ maxWidth: 1100, margin: "0 auto" }}>
            {activeTab === "check" && <MiddlewareCheck companies={companies} />}
            {activeTab === "fetch" && <QuickFetch companies={companies} />}
            {activeTab === "sync"  && <SyncToErpNext companies={companies} />}
            {activeTab === "logs"  && <LiveLogs />}
          </div>
        </main>
      </div>
    </div>
  );
}