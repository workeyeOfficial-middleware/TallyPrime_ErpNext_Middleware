import { StatusDot } from "./StatusDot";

const T = {
  ink:    "#0f172a",
  muted:  "#64748b",
  border: "#e2e8ef",
  panel:  "#f8fafc",
  accent: "#2563eb", accentL: "#eff6ff", accentB: "#bfdbfe",
  green:  "#059669", greenL:  "#ecfdf5", greenB:  "#a7f3d0",
  amber:  "#d97706", amberL:  "#fffbeb", amberB:  "#fde68a",
  red:    "#dc2626", redL:    "#fef2f2", redB:    "#fecaca",
  mono:   "'JetBrains Mono', 'Fira Code', monospace",
  title:  "'Plus Jakarta Sans', sans-serif",
};

const STATUS_LABEL = { ok: "PASS", warn: "WARN", fail: "FAIL", pending: "—", running: "…" };
const STATUS_STYLE = {
  ok:      { color: T.green,  bg: T.greenL,  border: T.greenB  },
  warn:    { color: T.amber,  bg: T.amberL,  border: T.amberB  },
  fail:    { color: T.red,    bg: T.redL,    border: T.redB    },
  pending: { color: T.muted,  bg: T.panel,   border: T.border  },
  running: { color: T.accent, bg: T.accentL, border: T.accentB },
};

export function CheckRow({ icon, label, check, children }) {
  const status = check?.status ?? "pending";
  const s = STATUS_STYLE[status] || STATUS_STYLE.pending;

  return (
    <div
      style={{
        display: "flex", flexDirection: "column", gap: 10,
        padding: "18px 0",
        borderBottom: `1px solid ${T.border}`,
      }}
      className="check-row-last-no-border"
    >
      {/* ── Row header ── */}
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        {/* Icon */}
        <span style={{
          fontSize: 16, width: 36, height: 36,
          display: "flex", alignItems: "center", justifyContent: "center",
          background: "#fff",
          border: `1px solid ${T.border}`,
          borderRadius: 10, flexShrink: 0,
          boxShadow: "0 2px 6px rgba(0,0,0,0.06), 0 1px 0 #fff inset",
        }}>
          {icon}
        </span>

        {/* Label */}
        <span style={{
          fontFamily: T.title, fontSize: 15, fontWeight: 700,
          color: T.ink, flex: 1, letterSpacing: "-0.2px",
        }}>
          {label}
        </span>

        {/* Status pill */}
        <span style={{
          fontFamily: T.mono, fontSize: 9.5, padding: "4px 12px",
          borderRadius: 20, border: `1px solid ${s.border}`,
          background: s.bg, color: s.color,
          fontWeight: 700, letterSpacing: "0.14em", textTransform: "uppercase",
          boxShadow: "0 1px 3px rgba(0,0,0,0.05)",
        }}>
          {STATUS_LABEL[status] || status.toUpperCase()}
        </span>

        <StatusDot status={status} />
      </div>

      {/* ── Count pills ── */}
      {check?.count !== undefined && (
        <div style={{ marginLeft: 48, display: "flex", flexWrap: "wrap", gap: 6 }}>
          <Pill label="Total"  value={check.count}      color="blue" />
          {check.partyCount  !== undefined && <Pill label="Party"  value={check.partyCount}  color="green" />}
          {check.withGstin   !== undefined && <Pill label="GST"    value={check.withGstin}   color="amber" />}
          {check.withEmail   !== undefined && <Pill label="Email"  value={check.withEmail}   color="blue"  />}
          {check.totalAmount !== undefined && (
            <Pill
              label="Amount"
              value={"₹" + check.totalAmount.toLocaleString("en-IN", { maximumFractionDigits: 0 })}
              color="green"
            />
          )}
          {check.latencyMs !== undefined && <Pill label="Ping" value={`${check.latencyMs}ms`} color="blue" />}
        </div>
      )}

      {/* ── byType chips ── */}
      {check?.byType && Object.keys(check.byType).length > 0 && (
        <div style={{ marginLeft: 48, display: "flex", flexWrap: "wrap", gap: 6 }}>
          {Object.entries(check.byType).map(([type, count]) => (
            <span key={type} style={{
              fontFamily: T.mono, fontSize: 10.5,
              background: "#fff", border: `1px solid ${T.border}`,
              borderRadius: 7, padding: "3px 10px", color: T.muted,
              boxShadow: "0 1px 3px rgba(0,0,0,0.04)",
            }}>
              {type}:&nbsp;<span style={{ color: T.ink, fontWeight: 700 }}>{count}</span>
            </span>
          ))}
        </div>
      )}

      {/* ── Named items ── */}
      {check?.data?.length > 0 && check.data[0]?.name && (
        <div style={{ marginLeft: 48, display: "flex", flexWrap: "wrap", gap: 6 }}>
          {check.data.map((c) => (
            <span key={c.guid || c.name} style={{
              fontFamily: T.title, fontSize: 12, fontWeight: 700,
              background: T.accentL, border: `1px solid ${T.accentB}`,
              color: "#1d4ed8", borderRadius: 20, padding: "4px 12px",
              letterSpacing: "-0.1px",
              boxShadow: "0 1px 4px rgba(37,99,235,0.1)",
            }}>
              {c.name}
            </span>
          ))}
        </div>
      )}

      {/* ── Error ── */}
      {check?.error && (
        <p style={{
          marginLeft: 48, fontFamily: T.mono, fontSize: 11.5,
          color: T.red, background: T.redL,
          border: `1px solid ${T.redB}`, borderRadius: 10,
          padding: "10px 14px", lineHeight: 1.65,
          boxShadow: "0 2px 6px rgba(220,38,38,0.08)",
        }}>
          ✗&nbsp;{check.error}
        </p>
      )}

      {children}
    </div>
  );
}

function Pill({ label, value, color }) {
  const map = {
    blue:  { color: "#1d4ed8", bg: "#eff6ff", border: "#bfdbfe" },
    green: { color: "#047857", bg: "#ecfdf5", border: "#a7f3d0" },
    amber: { color: "#b45309", bg: "#fffbeb", border: "#fde68a" },
  };
  const s = map[color] || map.blue;
  return (
    <span style={{
      fontFamily: "'JetBrains Mono', monospace", fontSize: 10.5,
      border: `1px solid ${s.border}`, borderRadius: 7,
      padding: "3px 10px", background: s.bg, color: s.color,
      boxShadow: "0 1px 3px rgba(0,0,0,0.04)",
    }}>
      {label}:&nbsp;<span style={{ fontWeight: 700 }}>{value}</span>
    </span>
  );
}