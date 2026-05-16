import { StatusDot } from "./StatusDot";

const T = {
  ink:    "#0a0e1a",
  muted:  "#5a6482",
  dim:    "#8a94b0",
  border: "#d8dff0",
  panel:  "#f0f3fa",
  card:   "#ffffff",
  accent: "#2563eb", accentL: "#eff6ff", accentB: "#bfdbfe",
  green:  "#059669", greenL:  "#ecfdf5", greenB:  "#a7f3d0",
  amber:  "#d97706", amberL:  "#fffbeb", amberB:  "#fde68a",
  red:    "#dc2626", redL:    "#fef2f2", redB:    "#fecaca",
  mono:   "'JetBrains Mono', 'Fira Code', monospace",
  title:  "'Plus Jakarta Sans', sans-serif",
};

const STATUS_LABEL = { ok: "PASS", warn: "WARN", fail: "FAIL", pending: "—", running: "…" };
const STATUS_STYLE = {
  ok:      { color: T.green,  bg: T.greenL,  border: T.greenB,  glow: "rgba(5,150,105,0.15)"  },
  warn:    { color: T.amber,  bg: T.amberL,  border: T.amberB,  glow: "rgba(217,119,6,0.15)"  },
  fail:    { color: T.red,    bg: T.redL,    border: T.redB,    glow: "rgba(220,38,38,0.15)"  },
  pending: { color: T.muted,  bg: T.panel,   border: T.border,  glow: "transparent"            },
  running: { color: T.accent, bg: T.accentL, border: T.accentB, glow: "rgba(37,99,235,0.15)"  },
};

export function CheckRow({ icon, label, check, children }) {
  const status = check?.status ?? "pending";
  const s = STATUS_STYLE[status] || STATUS_STYLE.pending;

  return (
    <div
      style={{
        display: "flex", flexDirection: "column",
        padding: "16px 0",
        borderBottom: `1px solid ${T.border}`,
      }}
      className="check-row-last-no-border"
    >
      {/* ── Row header ── */}
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>

        {/* Icon bubble */}
        <span style={{
          fontSize: 16, width: 36, height: 36,
          display: "flex", alignItems: "center", justifyContent: "center",
          background: T.card,
          border: `1.5px solid ${T.border}`,
          borderRadius: 10, flexShrink: 0,
          boxShadow: "0 2px 8px rgba(0,0,0,0.07), 0 1px 0 #fff inset",
        }}>
          {icon}
        </span>

        {/* Label */}
        <span style={{
          fontFamily: T.title, fontSize: 14, fontWeight: 700,
          color: T.ink, flex: 1, letterSpacing: "-0.2px",
        }}>
          {label}
        </span>

        {/* Status pill */}
        <span style={{
          fontFamily: T.mono, fontSize: 9, padding: "4px 13px",
          borderRadius: 20,
          border: `1.5px solid ${s.border}`,
          background: s.bg,
          color: s.color,
          fontWeight: 700, letterSpacing: "0.16em", textTransform: "uppercase",
          boxShadow: `0 0 0 3px ${s.glow}, 0 1px 3px rgba(0,0,0,0.05)`,
        }}>
          {STATUS_LABEL[status] || status.toUpperCase()}
        </span>

        <StatusDot status={status} />
      </div>

      {/* ── byType chips (e.g. voucher breakdown) ── */}
      {check?.byType && Object.keys(check.byType).length > 0 && (
        <div style={{ marginLeft: 48, marginTop: 10, display: "flex", flexWrap: "wrap", gap: 6 }}>
          {Object.entries(check.byType).map(([type, count]) => (
            <span key={type} style={{
              fontFamily: T.mono, fontSize: 10.5,
              background: T.card, border: `1px solid ${T.border}`,
              borderRadius: 7, padding: "3px 10px", color: T.muted,
              boxShadow: "0 1px 3px rgba(0,0,0,0.04)",
            }}>
              {type}:&nbsp;<span style={{ color: T.ink, fontWeight: 700 }}>{count}</span>
            </span>
          ))}
        </div>
      )}

      {/* ── Named items (e.g. Companies list) ── */}
      {check?.data?.length > 0 && check.data[0]?.name && (
        <div style={{ marginLeft: 48, marginTop: 10, display: "flex", flexWrap: "wrap", gap: 8 }}>
          {check.data.map((c) => (
            <div key={c.guid || c.name} style={{
              fontFamily: T.title, fontSize: 13, fontWeight: 700,
              background: T.accentL,
              border: `1.5px solid ${T.accentB}`,
              color: "#1d4ed8", borderRadius: 12,
              padding: "10px 18px",
              letterSpacing: "-0.2px",
              boxShadow: `0 3px 10px rgba(37,99,235,0.12), 0 1px 0 rgba(255,255,255,0.9) inset`,
              display: "flex", alignItems: "center", gap: 8,
            }}>
              <span style={{ fontSize: 15 }}>🏢</span>
              {c.name}
            </div>
          ))}
        </div>
      )}

      {/* ── Error message ── */}
      {check?.error && (
        <p style={{
          fontFamily: T.mono, fontSize: 11.5,
          color: T.red, background: T.redL,
          border: `1px solid ${T.redB}`, borderRadius: 10,
          padding: "10px 14px", lineHeight: 1.65,
          margin: "10px 0 0 48px",
          boxShadow: "0 2px 6px rgba(220,38,38,0.08)",
        }}>
          ✗&nbsp;{check.error}
        </p>
      )}

      {/* ── Big StatBox rows injected by parent ── */}
      {children}
    </div>
  );
}