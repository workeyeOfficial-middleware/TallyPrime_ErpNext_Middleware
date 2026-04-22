import { useState } from "react";
import { tallyAPI } from "../api/tallyAPI";
import { DataTable } from "../components/DataTable";

// ── Logic constants (unchanged) ───────────────────────────────────────────────
const TODAY      = new Date().toISOString().slice(0, 10);
const YEAR_START = `${new Date().getFullYear()}-04-01`;

// ── Design tokens ──────────────────────────────────────────────────────────────
const C = {
  bg:      "#e8edf6",
  card:    "#ffffff",
  surface: "#f0f3fa",
  border:  "#d8dff0",
  borderH: "#b0bcd8",
  ink:     "#0a0e1a",
  ink2:    "#1e2a4a",
  muted:   "#5a6482",
  dim:     "#8a94b0",
  accent:  "#2563eb",
  accentD: "#1d4ed8",
  accentL: "#eef4ff",
  accentB: "#b8d0fe",
  green:   "#16a34a",
  greenD:  "#15803d",
  greenL:  "#f0fdf4",
  greenB:  "#bbf7d0",
  amber:   "#d97706",
  amberL:  "#fffbeb",
  red:     "#dc2626",
  redL:    "#fef2f2",
  mono:    "'JetBrains Mono','Fira Code',monospace",
  sans:    "'DM Sans','Plus Jakarta Sans',sans-serif",
  title:   "'Syne','Plus Jakarta Sans',sans-serif",
};

// ── Spinner ────────────────────────────────────────────────────────────────────
function Spin({ size = 14, color = C.accent }) {
  return (
    <span style={{
      display: "inline-block", width: size, height: size,
      borderRadius: "50%",
      border: `2px solid ${color}25`,
      borderTopColor: color,
      animation: "qf-spin .7s linear infinite",
      flexShrink: 0,
    }} />
  );
}

// ── Section divider ────────────────────────────────────────────────────────────
function SectionDivider({ label }) {
  return (
    <div style={{
      gridColumn: "1 / -1",
      display: "flex", alignItems: "center", gap: 12,
      padding: "16px 0 4px",
    }}>
      <div style={{ flex: 1, height: 1, background: `linear-gradient(90deg,${C.accent}40,${C.border})` }} />
      <span style={{
        fontFamily: C.mono, fontSize: 9, color: C.accent,
        textTransform: "uppercase", letterSpacing: "0.18em", fontWeight: 700,
        background: C.accentL, border: `1px solid ${C.accentB}`,
        padding: "3px 10px", borderRadius: 20,
      }}>
        {label}
      </span>
      <div style={{ flex: 1, height: 1, background: `linear-gradient(90deg,${C.border},${C.accent}40)` }} />
    </div>
  );
}

// ── Input style helper ─────────────────────────────────────────────────────────
const inp = (extra = {}) => ({
  width: "100%", padding: "9px 13px",
  border: `1.5px solid ${C.border}`, borderRadius: 9,
  fontFamily: C.sans, fontSize: 13, color: C.ink,
  background: C.surface, outline: "none",
  transition: "border-color .15s, background .15s, box-shadow .15s",
  boxSizing: "border-box", appearance: "none",
  ...extra,
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

// ── FetchCard ──────────────────────────────────────────────────────────────────
function FetchCard({ action, results, loading }) {
  const [hov, setHov] = useState(false);
  const res      = results[action.key];
  const busy     = loading[action.key];
  const disabled = busy || !action.enabled;
  const hasData  = res && !res.error;
  const hasError = res?.error;

  const borderColor = hasData ? C.greenB : hasError ? "#fca5a5" : hov && !disabled ? C.borderH : C.border;

  return (
    <div style={{
      background: C.card,
      border: `1.5px solid ${borderColor}`,
      borderRadius: 12,
      overflow: "hidden",
      transition: "border-color .15s, box-shadow .15s, transform .12s",
      boxShadow: hov && !disabled ? "0 4px 16px rgba(0,0,0,.07)" : "0 1px 3px rgba(0,0,0,.04)",
      transform: hov && !disabled ? "translateY(-1px)" : "none",
    }}>
      <button
        onClick={action.fn}
        disabled={disabled}
        onMouseEnter={() => setHov(true)}
        onMouseLeave={() => setHov(false)}
        style={{
          width: "100%", textAlign: "left",
          display: "flex", alignItems: "center", gap: 11,
          padding: "12px 14px",
          background: "transparent", border: "none",
          cursor: disabled ? "not-allowed" : "pointer",
          opacity: disabled ? 0.45 : 1,
          transition: "background .12s",
        }}
      >
        {/* Icon bubble */}
        <div style={{
          width: 36, height: 36, borderRadius: 9, flexShrink: 0,
          background: hasData ? C.greenL : C.surface,
          border: `1.5px solid ${hasData ? C.greenB : C.border}`,
          display: "flex", alignItems: "center", justifyContent: "center",
          fontSize: 17, lineHeight: 1,
          transition: "all .15s",
        }}>
          {action.icon}
        </div>

        {/* Text */}
        <div style={{ flex: 1, minWidth: 0 }}>
          <p style={{
            fontFamily: C.title, fontSize: 12, fontWeight: 700,
            color: hasData ? C.greenD : C.ink,
            letterSpacing: "-0.2px", margin: 0,
          }}>
            {action.label}
          </p>
          {hasData && (
            <p style={{ fontFamily: C.mono, fontSize: 10, color: C.green, margin: "2px 0 0", fontWeight: 600 }}>
              ✓ {res.count?.toLocaleString()} records
            </p>
          )}
          {hasError && (
            <p style={{
              fontFamily: C.mono, fontSize: 10, color: C.red, margin: "2px 0 0",
              overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
            }}>
              {res.error}
            </p>
          )}
          {!res && !busy && (
            <p style={{ fontFamily: C.mono, fontSize: 10, color: C.dim, margin: "2px 0 0" }}>
              Click to fetch
            </p>
          )}
        </div>

        {/* Status indicator */}
        {busy
          ? <Spin size={15} />
          : hasData
          ? <span style={{
              width: 22, height: 22, borderRadius: "50%",
              background: C.greenL, border: `1.5px solid ${C.greenB}`,
              display: "flex", alignItems: "center", justifyContent: "center",
              flexShrink: 0, fontSize: 10, color: C.green, fontWeight: 700,
            }}>✓</span>
          : hasError
          ? <span style={{ fontFamily: C.mono, fontSize: 16, color: "#fca5a5" }}>✗</span>
          : <span style={{ fontFamily: C.mono, fontSize: 13, color: C.accent, fontWeight: 700, flexShrink: 0 }}>→</span>
        }
      </button>

      {/* Data table */}
      {hasData && res.data?.length > 0 && (
        <div style={{
          borderTop: `1.5px solid ${C.greenB}`,
          padding: "10px 14px 14px",
          background: C.greenL + "55",
          animation: "qf-fade-in .2s ease",
        }}>
          <DataTable rows={res.data} columns={action.columns} title={action.label} />
        </div>
      )}
      {hasData && res.data?.length === 0 && (
        <div style={{ borderTop: `1px solid ${C.border}`, padding: "8px 14px" }}>
          <p style={{ fontFamily: C.mono, fontSize: 10, color: C.muted, margin: 0 }}>No records found</p>
        </div>
      )}
    </div>
  );
}

// ── Main component ─────────────────────────────────────────────────────────────
export function QuickFetch({ companies }) {
  // ── State (unchanged) ──────────────────────────────────────────────────────
  const [company,  setCompany]  = useState(companies?.[0]?.name || "");
  const [fromDate, setFromDate] = useState(YEAR_START);
  const [toDate,   setToDate]   = useState(TODAY);
  const [results,  setResults]  = useState({});
  const [loading,  setLoading]  = useState({});

  // ── doFetch (unchanged) ────────────────────────────────────────────────────
  async function doFetch(key, apiFn) {
    setLoading((l) => ({ ...l, [key]: true }));
    setResults((r) => ({ ...r, [key]: null }));
    try {
      const res = await apiFn();
      setResults((r) => ({ ...r, [key]: res }));
    } catch (e) {
      setResults((r) => ({ ...r, [key]: { error: e.message } }));
    } finally {
      setLoading((l) => ({ ...l, [key]: false }));
    }
  }

  const co = company.trim();

  // ── Sections (unchanged logic) ─────────────────────────────────────────────
  const sections = [
    {
      label: null,
      items: [
        {
          key: "companies", icon: "🏢", label: "Companies", enabled: true,
          fn: () => doFetch("companies", () => tallyAPI.companies()),
          columns: [{ key: "name", label: "Company" }, { key: "state", label: "State" }, { key: "gstin", label: "GSTIN" }],
        },
      ],
    },
    {
      label: "Accounting Masters",
      items: [
        { key: "groups",         icon: "🗂",  label: "Groups",          enabled: !!co, fn: () => doFetch("groups",         () => tallyAPI.groups(co)),         columns: [{ key: "name", label: "Group" }, { key: "parent", label: "Parent" }] },
        { key: "ledgers",        icon: "📒",  label: "Ledgers",         enabled: !!co, fn: () => doFetch("ledgers",        () => tallyAPI.ledgers(co)),        columns: [{ key: "name", label: "Ledger" }, { key: "parentGroup", label: "Group" }, { key: "closingBalance", label: "Balance", render: (v) => v ? `₹${Number(v).toLocaleString("en-IN")}` : "—" }] },
        { key: "voucherTypes",   icon: "🏷",  label: "Voucher Types",   enabled: !!co, fn: () => doFetch("voucherTypes",   () => tallyAPI.voucherTypes(co)),   columns: [{ key: "name", label: "Type" }, { key: "parent", label: "Parent" }] },
        { key: "costCategories", icon: "📂",  label: "Cost Categories", enabled: !!co, fn: () => doFetch("costCategories", () => tallyAPI.costCategories(co)), columns: [{ key: "name", label: "Category" }] },
        { key: "costCentres",    icon: "🏬",  label: "Cost Centres",    enabled: !!co, fn: () => doFetch("costCentres",    () => tallyAPI.costCentres(co)),    columns: [{ key: "name", label: "Centre" }, { key: "parent", label: "Parent" }] },
        { key: "currencies",     icon: "💱",  label: "Currencies",      enabled: !!co, fn: () => doFetch("currencies",     () => tallyAPI.currencies(co)),     columns: [{ key: "name", label: "Currency" }, { key: "symbol", label: "Symbol" }] },
        { key: "budgets",        icon: "📊",  label: "Budgets",         enabled: !!co, fn: () => doFetch("budgets",        () => tallyAPI.budgets(co)),        columns: [{ key: "name", label: "Budget" }, { key: "startDate", label: "From" }] },
      ],
    },
    {
      label: "Inventory Masters",
      items: [
        { key: "stockGroups",     icon: "🗃",  label: "Stock Groups",     enabled: !!co, fn: () => doFetch("stockGroups",     () => tallyAPI.stockGroups(co)),     columns: [{ key: "name", label: "Group" }, { key: "parent", label: "Parent" }] },
        { key: "stock",           icon: "📦",  label: "Stock Items",      enabled: !!co, fn: () => doFetch("stock",           () => tallyAPI.stock(co)),           columns: [{ key: "name", label: "Item" }, { key: "group", label: "Group" }, { key: "closingValue", label: "Value", render: (v) => v ? `₹${Number(v).toLocaleString("en-IN")}` : "—" }] },
        { key: "stockCategories", icon: "🏷",  label: "Stock Categories", enabled: !!co, fn: () => doFetch("stockCategories", () => tallyAPI.stockCategories(co)), columns: [{ key: "name", label: "Category" }] },
        { key: "units",           icon: "📐",  label: "Units",            enabled: !!co, fn: () => doFetch("units",           () => tallyAPI.units(co)),           columns: [{ key: "name", label: "Unit" }, { key: "isSimple", label: "Type", render: (v) => v ? "Simple" : "Compound" }] },
        { key: "godowns",         icon: "🏭",  label: "Godowns",          enabled: !!co, fn: () => doFetch("godowns",         () => tallyAPI.godowns(co)),         columns: [{ key: "name", label: "Godown" }, { key: "parent", label: "Parent" }] },
      ],
    },
    {
      label: "Transactions",
      items: [
        {
          key: "vouchers", icon: "🧾", label: "Vouchers", enabled: !!co,
          fn: () => doFetch("vouchers", () => tallyAPI.vouchers(co, fromDate, toDate)),
          columns: [{ key: "voucherDate", label: "Date" }, { key: "voucherType", label: "Type" }, { key: "partyName", label: "Party" }, { key: "netAmount", label: "Amount", render: (v) => v ? `₹${Number(v).toLocaleString("en-IN")}` : "—" }],
        },
      ],
    },
  ];

  const fetchedCount = Object.values(results).filter(r => r && !r.error).length;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6, fontFamily: C.sans }}>
      <style>{`
        @keyframes qf-spin    { to { transform:rotate(360deg); } }
        @keyframes qf-fade-in { from { opacity:0; transform:translateY(4px); } to { opacity:1; transform:none; } }
      `}</style>

      {/* ── Config card ── */}
      <div style={{
        background: C.card, border: `1.5px solid ${C.border}`,
        borderRadius: 14, padding: 20, marginBottom: 6,
        boxShadow: "0 4px 24px rgba(13,21,50,.08), 0 1px 0 rgba(255,255,255,.9) inset",
      }}>
        {/* Header */}
        <div style={{
          display: "flex", alignItems: "center", justifyContent: "space-between",
          marginBottom: 18, paddingBottom: 14, borderBottom: `1px solid ${C.border}`,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{ width: 3, height: 20, background: `linear-gradient(180deg,${C.accent},${C.accentD})`, borderRadius: 2 }} />
            <div>
              <h2 style={{ fontFamily: C.title, fontWeight: 800, fontSize: 14, color: C.ink, letterSpacing: "-0.4px", margin: 0 }}>
                Quick Fetch
              </h2>
              <p style={{ fontFamily: C.mono, fontSize: 9, color: C.muted, margin: 0, letterSpacing: "0.06em" }}>
                Tally data inspector
              </p>
            </div>
          </div>
          {fetchedCount > 0 && (
            <span style={{
              fontFamily: C.mono, fontSize: 10, fontWeight: 700,
              color: C.green, background: C.greenL,
              border: `1.5px solid ${C.greenB}`,
              padding: "3px 12px", borderRadius: 20,
              animation: "qf-fade-in .2s ease",
            }}>
              ✓ {fetchedCount} fetched
            </span>
          )}
        </div>

        {/* Company picker */}
        <div style={{ marginBottom: 13 }}>
          <label style={{
            display: "block", fontFamily: C.mono, fontSize: 9, color: C.muted,
            letterSpacing: "0.14em", textTransform: "uppercase", marginBottom: 6, fontWeight: 700,
          }}>
            Tally Company
          </label>
          {companies?.length > 0 ? (
            <select
              value={company}
              onChange={(e) => setCompany(e.target.value)}
              style={{ ...inp(), cursor: "pointer" }}
              onFocus={onFocus} onBlur={onBlur}
            >
              <option value="">— select company —</option>
              {companies.map((c) => (
                <option key={c.guid || c.name} value={c.name}>{c.name}</option>
              ))}
            </select>
          ) : (
            <input
              value={company}
              onChange={(e) => setCompany(e.target.value)}
              placeholder="Company name"
              style={inp()}
              onFocus={onFocus} onBlur={onBlur}
            />
          )}
        </div>

        {/* Date range */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          {[["From Date (Vouchers)", fromDate, setFromDate], ["To Date (Vouchers)", toDate, setToDate]].map(([lbl, val, setter]) => (
            <div key={lbl}>
              <label style={{
                display: "block", fontFamily: C.mono, fontSize: 9, color: C.muted,
                letterSpacing: "0.14em", textTransform: "uppercase", marginBottom: 6, fontWeight: 700,
              }}>
                {lbl}
              </label>
              <input
                type="date"
                value={val}
                onChange={(e) => setter(e.target.value)}
                style={{ ...inp(), fontFamily: C.mono, fontSize: 12 }}
                onFocus={onFocus} onBlur={onBlur}
              />
            </div>
          ))}
        </div>

        {!co && (
          <div style={{
            marginTop: 13, padding: "9px 13px", borderRadius: 9,
            background: C.amberL, border: `1.5px solid #fde68a`,
            display: "flex", alignItems: "center", gap: 8,
          }}>
            <span style={{ fontSize: 12 }}>⚠️</span>
            <p style={{ fontFamily: C.mono, fontSize: 10, color: C.amber, margin: 0 }}>
              Select a Tally company to enable all fetch options
            </p>
          </div>
        )}
      </div>

      {/* ── Fetch cards grid ── */}
      {sections.map((section) => (
        <div key={section.label || "top"} style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
          {section.label && <SectionDivider label={section.label} />}
          {section.items.map((action) => (
            <FetchCard
              key={action.key}
              action={action}
              results={results}
              loading={loading}
            />
          ))}
        </div>
      ))}
    </div>
  );
}