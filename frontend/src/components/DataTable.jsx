import { useState } from "react";

const T = {
  ink:    "#0a0e1a",
  muted:  "#5a6482",
  dim:    "#8a94b0",
  border: "#d8dff0",
  panel:  "#f0f3fa",
  accent: "#2563eb",
  mono:   "'JetBrains Mono', 'Fira Code', monospace",
  title:  "'Plus Jakarta Sans', sans-serif",
};

export function DataTable({ title, rows, columns }) {
  const [open, setOpen] = useState(false);
  if (!rows || rows.length === 0) return null;

  const preview = rows.slice(0, 5);

  return (
    <div style={{ marginLeft: 48, marginTop: 8 }}>
      <button
        onClick={() => setOpen((o) => !o)}
        style={{
          background: "none", border: "none", cursor: "pointer", padding: 0,
          display: "inline-flex", alignItems: "center", gap: 7,
          fontFamily: T.mono, fontSize: 10.5, color: T.dim,
          transition: "color 0.12s",
        }}
        onMouseEnter={(e) => { e.currentTarget.style.color = T.accent; }}
        onMouseLeave={(e) => { e.currentTarget.style.color = T.dim; }}
      >
        <span style={{
          display: "inline-block", fontSize: 8, lineHeight: 1,
          transition: "transform 0.18s ease",
          transform: open ? "rotate(90deg)" : "rotate(0deg)",
        }}>▶</span>
        <span style={{ textDecoration: "underline", textUnderlineOffset: 2 }}>
          {open ? "Hide" : "Preview"} sample&nbsp;({Math.min(rows.length, 5)} of {rows.length} rows)
        </span>
      </button>

      {open && (
        <div style={{
          marginTop: 10, overflowX: "auto",
          borderRadius: 12, border: `1px solid ${T.border}`,
          background: "#fff",
          boxShadow: "0 4px 20px rgba(0,0,0,0.06), 0 1px 0 #fff inset",
          animation: "dt-fade 0.15s ease-out",
        }}>
          <table style={{
            width: "100%", fontFamily: T.mono, fontSize: 11.5,
            borderCollapse: "collapse",
          }}>
            <thead>
              <tr style={{ background: T.panel, borderBottom: `1px solid ${T.border}` }}>
                {columns.map((c) => (
                  <th key={c.key} style={{
                    textAlign: "left", padding: "9px 14px",
                    color: T.muted, fontWeight: 700,
                    fontSize: 9.5, letterSpacing: "0.12em",
                    textTransform: "uppercase", whiteSpace: "nowrap",
                  }}>
                    {c.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {preview.map((row, i) => (
                <tr
                  key={i}
                  style={{
                    borderBottom: i < preview.length - 1 ? `1px solid ${T.border}` : "none",
                    transition: "background 0.1s",
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.background = T.panel; }}
                  onMouseLeave={(e) => { e.currentTarget.style.background = "transparent"; }}
                >
                  {columns.map((c) => (
                    <td key={c.key} style={{
                      padding: "8px 14px", color: T.ink,
                      maxWidth: 160, overflow: "hidden",
                      textOverflow: "ellipsis", whiteSpace: "nowrap",
                    }}>
                      {c.render ? c.render(row[c.key], row) : (row[c.key] ?? "—")}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>

          {rows.length > 5 && (
            <div style={{
              padding: "7px 14px",
              borderTop: `1px solid ${T.border}`,
              background: T.panel,
            }}>
              <span style={{ fontFamily: T.mono, fontSize: 9.5, color: T.dim }}>
                +{rows.length - 5} more rows not shown
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}