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
      background: "linear-gradient(145deg, #d8e2f5 0%, #e8edf6 50%, #dde5f4 100%)",
      fontFamily: "'Plus Jakarta Sans', sans-serif",
      overflow: "hidden",
    }}>

      {/* ── SIDEBAR ── */}
      <aside style={{
        width: 248,
        background: "linear-gradient(160deg, #1a2540 0%, #0d1528 50%, #080e1e 100%)",
        display: "flex",
        flexDirection: "column",
        flexShrink: 0,
        boxShadow: "4px 0 40px rgba(0,0,0,0.5), 2px 0 0 rgba(255,255,255,0.04)",
        position: "relative",
        zIndex: 10,
      }}>
        {/* Dot-grid texture */}
        <div style={{
          position: "absolute", top: 0, left: 0, width: "100%", height: "100%",
          backgroundImage: "radial-gradient(circle, rgba(255,255,255,0.028) 1px, transparent 1px)",
          backgroundSize: "22px 22px",
          pointerEvents: "none",
        }} />
        {/* Left-edge blue glow */}
        <div style={{
          position: "absolute", top: 0, left: 0, width: "100%", height: "100%",
          background: "linear-gradient(90deg, rgba(37,99,235,0.08) 0%, transparent 55%)",
          pointerEvents: "none",
        }} />
        {/* Top shine */}
        <div style={{
          position: "absolute", top: 0, left: 0, right: 0, height: 160,
          background: "linear-gradient(180deg, rgba(255,255,255,0.06) 0%, transparent 100%)",
          pointerEvents: "none",
        }} />

        {/* Logo area */}
        <div style={{
          padding: "22px 18px 18px",
          borderBottom: "1px solid rgba(255,255,255,0.07)",
          position: "relative",
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div style={{
              width: 44, height: 44, borderRadius: 13,
              background: "linear-gradient(145deg, #3b82f6, #1d4ed8)",
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 20,
              boxShadow: "0 8px 24px rgba(37,99,235,0.55), 0 2px 0 rgba(255,255,255,0.18) inset, 0 -2px 0 rgba(0,0,0,0.3) inset",
              transform: "perspective(80px) rotateX(4deg)",
              flexShrink: 0,
            }}>⟳</div>
            <div>
              <div style={{
                fontSize: 17, fontWeight: 800, color: "#fff",
                letterSpacing: "-0.4px",
                textShadow: "0 2px 8px rgba(37,99,235,0.4)",
              }}>Tally → ERP</div>
              <div style={{
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: 9, color: "rgba(255,255,255,0.32)",
                letterSpacing: "0.18em", marginTop: 2, textTransform: "uppercase",
              }}>MIDDLEWARE</div>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav style={{ padding: "14px 10px", flex: 1, display: "flex", flexDirection: "column", gap: 5, position: "relative" }}>
          {NAV.map(({ id, label, icon, desc }) => {
            const active = activeTab === id;
            return (
              <button
                key={id}
                onClick={() => setActiveTab(id)}
                style={{
                  width: "100%", textAlign: "left",
                  padding: "11px 12px", borderRadius: 12,
                  border: active ? "1px solid rgba(255,255,255,0.13)" : "1px solid transparent",
                  cursor: "pointer",
                  display: "flex", alignItems: "center", gap: 12,
                  background: active
                    ? "linear-gradient(135deg, rgba(37,99,235,0.38), rgba(29,78,216,0.22))"
                    : "transparent",
                  boxShadow: active
                    ? "0 6px 20px rgba(37,99,235,0.28), 0 1px 0 rgba(255,255,255,0.09) inset, 0 -1px 0 rgba(0,0,0,0.2) inset"
                    : "none",
                  transition: "all 0.2s ease",
                  position: "relative",
                }}
                onMouseEnter={(e) => {
                  if (!active) {
                    e.currentTarget.style.background = "linear-gradient(135deg, rgba(255,255,255,0.08), rgba(255,255,255,0.04))";
                    e.currentTarget.style.borderColor = "rgba(255,255,255,0.07)";
                    e.currentTarget.style.boxShadow = "0 2px 10px rgba(0,0,0,0.2)";
                  }
                }}
                onMouseLeave={(e) => {
                  if (!active) {
                    e.currentTarget.style.background = "transparent";
                    e.currentTarget.style.borderColor = "transparent";
                    e.currentTarget.style.boxShadow = "none";
                  }
                }}
              >
                {/* Active left glow bar */}
                {active && (
                  <div style={{
                    position: "absolute", left: 0, top: "18%", height: "64%", width: 3,
                    borderRadius: "0 3px 3px 0",
                    background: "linear-gradient(180deg, #60a5fa, #2563eb, #1d4ed8)",
                    boxShadow: "0 0 12px rgba(96,165,250,0.8)",
                  }} />
                )}
                <span style={{
                  width: 36, height: 36, borderRadius: 10,
                  background: active
                    ? "linear-gradient(145deg, #3b82f6, #1e40af)"
                    : "rgba(255,255,255,0.06)",
                  border: active ? "1px solid rgba(255,255,255,0.2)" : "1px solid rgba(255,255,255,0.05)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 16, flexShrink: 0,
                  boxShadow: active
                    ? "0 4px 12px rgba(37,99,235,0.45), 0 1px 0 rgba(255,255,255,0.18) inset"
                    : "0 2px 6px rgba(0,0,0,0.25)",
                  transition: "all 0.2s",
                }}>
                  {icon}
                </span>
                <div>
                  <div style={{
                    fontSize: 13.5, fontWeight: active ? 700 : 500,
                    color: active ? "#fff" : "rgba(255,255,255,0.6)",
                    letterSpacing: "-0.2px",
                    textShadow: active ? "0 1px 6px rgba(0,0,0,0.3)" : "none",
                  }}>
                    {label}
                  </div>
                  <div style={{
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: 9.5, color: active ? "rgba(255,255,255,0.38)" : "rgba(255,255,255,0.25)",
                    marginTop: 2, fontWeight: 400,
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
          padding: "12px 16px",
          borderTop: "1px solid rgba(255,255,255,0.07)",
          position: "relative",
        }}>
          <div style={{
            display: "flex", alignItems: "center", gap: 8,
            background: "rgba(255,255,255,0.04)",
            border: "1px solid rgba(255,255,255,0.07)",
            borderRadius: 10, padding: "8px 12px",
            boxShadow: "0 2px 8px rgba(0,0,0,0.2), 0 1px 0 rgba(255,255,255,0.05) inset",
          }}>
            <div style={{
              width: 8, height: 8, borderRadius: "50%",
              background: "#4ade80",
              boxShadow: "0 0 10px rgba(74,222,128,0.9)",
              flexShrink: 0,
            }} />
            <span style={{
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: 9.5, color: "rgba(255,255,255,0.32)",
              letterSpacing: "0.06em",
            }}>
              v1.0 · online
            </span>
          </div>
        </div>
      </aside>

      {/* ── MAIN ── */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>

        {/* Topbar */}
        <header style={{
          background: "rgba(255,255,255,0.92)",
          backdropFilter: "blur(20px)",
          borderBottom: "1px solid rgba(200,215,240,0.8)",
          boxShadow: "0 2px 24px rgba(13,21,50,.09), 0 1px 0 rgba(255,255,255,.95) inset",
          display: "flex", alignItems: "center",
          padding: "0 32px", height: 70, flexShrink: 0,
        }}>
          <div>
            <h2 style={{
              fontSize: 24, fontWeight: 800, color: "#0a0e1a",
              letterSpacing: "-0.6px", lineHeight: 1.1,
            }}>
              {meta.title}
            </h2>
            <p style={{ fontSize: 13, color: "#5a6482", marginTop: 3, fontWeight: 500 }}>
              {meta.sub}
            </p>
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