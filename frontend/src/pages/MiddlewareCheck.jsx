import { useState, useRef, useEffect } from "react";
import { tallyAPI } from "../api/tallyAPI";
import { CheckRow } from "../components/CheckRow";

// ── Logic constants (unchanged) ──────────────────────────────────────────────
const TODAY      = new Date().toISOString().slice(0, 10);
const YEAR_START = `${new Date().getFullYear()}-04-01`;

// ── Design tokens ────────────────────────────────────────────────────────────
const C = {
  card:    "#ffffff",
  surface: "#f0f3fa",
  bg:      "#e8edf6",
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
  amberB:  "#fde68a",
  red:     "#dc2626",
  redL:    "#fef2f2",
  redB:    "#fecaca",
  mono:    "'JetBrains Mono','Fira Code',monospace",
  sans:    "'DM Sans','Plus Jakarta Sans',sans-serif",
  title:   "'Syne','Plus Jakarta Sans',sans-serif",
};

// ── Spinner ──────────────────────────────────────────────────────────────────
function Spin({ size = 16, color = C.accent }) {
  return (
    <span style={{
      display:"inline-block", width:size, height:size,
      borderRadius:"50%",
      border:`2.5px solid ${color}25`,
      borderTopColor:color,
      animation:"mc-spin .75s linear infinite",
      flexShrink:0,
    }}/>
  );
}

// ── Input helpers ────────────────────────────────────────────────────────────
const inp = (extra = {}) => ({
  width:"100%", padding:"10px 13px",
  border:`1.5px solid ${C.border}`, borderRadius:10,
  fontFamily:C.sans, fontSize:13, color:C.ink,
  background:C.surface, outline:"none",
  transition:"border-color .15s, background .15s, box-shadow .15s",
  boxSizing:"border-box",
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

// ── Section divider ──────────────────────────────────────────────────────────
function SectionHeader({ label }) {
  return (
    <div style={{ display:"flex", alignItems:"center", gap:12, padding:"18px 20px 6px" }}>
      <div style={{ flex:1, height:1, background:`linear-gradient(90deg,${C.border},transparent)` }}/>
      <span style={{
        fontFamily:C.mono, fontSize:9, fontWeight:600,
        color:C.accent, textTransform:"uppercase", letterSpacing:"0.18em",
        background:C.accentL, border:`1px solid ${C.accentB}`,
        padding:"3px 10px", borderRadius:20,
      }}>
        {label}
      </span>
      <div style={{ flex:1, height:1, background:`linear-gradient(90deg,transparent,${C.border})` }}/>
    </div>
  );
}

// ── Status result banner ─────────────────────────────────────────────────────
const STATUS_CFG = {
  ok:   { bg:C.greenL, border:C.greenB,  text:C.green,  icon:"✓", label:"ALL CHECKS PASSED"        },
  warn: { bg:C.amberL, border:C.amberB,  text:C.amber,  icon:"⚠", label:"COMPLETED WITH WARNINGS"  },
  fail: { bg:C.redL,   border:C.redB,    text:C.red,    icon:"✗", label:"CHECKS FAILED"             },
};

// ── Per-check stat boxes row ─────────────────────────────────────────────────
function StatBox({ label, value, color = C.accent, bg = C.accentL, bd = C.accentB }) {
  if (value === undefined || value === null) return null;
  return (
    <div style={{
      display:"flex", flexDirection:"column", alignItems:"center", justifyContent:"center",
      padding:"10px 18px", borderRadius:11,
      background:bg, border:`1.5px solid ${bd}`,
      boxShadow:`0 3px 10px ${bd}60, 0 1px 0 rgba(255,255,255,.9) inset`,
      minWidth:68, flex:"0 0 auto",
    }}>
      <span style={{ fontFamily:C.title, fontSize:20, fontWeight:800, color, lineHeight:1 }}>
        {typeof value === "number" ? value.toLocaleString() : value}
      </span>
      <span style={{ fontFamily:C.mono, fontSize:9, color:C.muted, marginTop:4, textTransform:"uppercase", letterSpacing:"0.12em", whiteSpace:"nowrap" }}>
        {label}
      </span>
    </div>
  );
}
function StatRow({ children }) {
  const boxes = Array.isArray(children) ? children.filter(Boolean) : [children].filter(Boolean);
  if (!boxes.length) return null;
  return (
    <div style={{ display:"flex", gap:8, flexWrap:"wrap", marginLeft:48, marginTop:10, marginBottom:2 }}>
      {boxes}
    </div>
  );
}

// ── Summary stat pill ────────────────────────────────────────────────────────
function StatBadge({ label, value, color = C.accent, bg = C.accentL, bd = C.accentB }) {
  return (
    <div style={{
      display:"flex", flexDirection:"column", alignItems:"center",
      padding:"10px 16px", borderRadius:10,
      background:bg, border:`1.5px solid ${bd}`,
      minWidth:70,
    }}>
      <span style={{ fontFamily:C.title, fontSize:18, fontWeight:800, color, lineHeight:1 }}>
        {(value ?? 0).toLocaleString()}
      </span>
      <span style={{ fontFamily:C.mono, fontSize:9, color:C.muted, marginTop:3, textTransform:"uppercase", letterSpacing:"0.1em" }}>
        {label}
      </span>
    </div>
  );
}

// ── Main component ───────────────────────────────────────────────────────────
export function MiddlewareCheck({ companies }) {
  // ── State (unchanged) ──────────────────────────────────────────────────────
  const [company,     setCompany]     = useState(companies?.[0]?.name || "");
  const [fromDate,    setFromDate]    = useState(YEAR_START);
  const [toDate,      setToDate]      = useState(TODAY);
  const [loading,     setLoading]     = useState(false);
  const [report,      setReport]      = useState(null);
  const [rawResponse, setRawResponse] = useState(null);
  const [error,       setError]       = useState(null);
  const [showRaw,     setShowRaw]     = useState(false); // kept for error fallback
  const resultsRef = useRef(null);

  // ── Effects (unchanged) ────────────────────────────────────────────────────
  useEffect(() => {
    if (report && resultsRef.current) {
      setTimeout(() => resultsRef.current.scrollIntoView({ behavior:"smooth", block:"start" }), 100);
    }
  }, [report]);

  useEffect(() => {
    if (companies?.length > 0 && !company) setCompany(companies[0].name);
  }, [companies]); // eslint-disable-line

  // ── runCheck (unchanged) ───────────────────────────────────────────────────
  async function runCheck() {
    if (!company.trim()) { setError("Enter a company name"); return; }
    setLoading(true); setReport(null); setRawResponse(null); setError(null); setShowRaw(false);
    try {
      const res = await tallyAPI.middlewareCheck(company, fromDate, toDate);
      setRawResponse(res);
      if (res.ok && res.report) setReport(res.report);
      else { setError(res.error || "Check returned no report."); setShowRaw(true); }
    } catch (e) {
      setError(`Network error: ${e.message}`);
      setRawResponse({ fetchError: e.message });
      setShowRaw(true);
    } finally {
      setLoading(false);
    }
  }

  // ── effectiveStatus (unchanged) ───────────────────────────────────────────
  const effectiveStatus = report
    ? (report.checks?.ping?.status === "fail" || report.checks?.companies?.status === "fail") ? "fail"
    : report.checks?.ledgers?.count > 0 ? "ok"
    : report.status === "ok" ? "ok" : report.status === "warning" ? "warn" : "fail"
    : null;

  return (
    <div style={{ display:"flex", flexDirection:"column", gap:16, fontFamily:C.sans }}>
      <style>{`
        @keyframes mc-spin    { to { transform:rotate(360deg); } }
        @keyframes mc-fade-in { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:none} }
        @keyframes mc-pop     { 0%{transform:scale(.96);opacity:0} 100%{transform:scale(1);opacity:1} }
      `}</style>

      {/* ── Config card ──────────────────────────────────────────────────── */}
      <div style={{
        background:C.card, border:`1.5px solid ${C.border}`,
        borderRadius:16, padding:22,
        boxShadow:"0 4px 24px rgba(13,21,50,.08), 0 1px 0 rgba(255,255,255,.9) inset",
      }}>
        {/* Header */}
        <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:20, paddingBottom:16, borderBottom:`1px solid ${C.border}` }}>
          <div style={{ width:3, height:22, background:`linear-gradient(180deg,${C.accent},${C.accentD})`, borderRadius:2 }}/>
          <div>
            <h2 style={{ fontFamily:C.title, fontWeight:800, fontSize:15, color:C.ink, letterSpacing:"-0.4px", margin:0 }}>
              Check Configuration
            </h2>
            <p style={{ fontFamily:C.mono, fontSize:9, color:C.muted, margin:0, letterSpacing:"0.06em" }}>
              Validates full Tally data connection
            </p>
          </div>
        </div>

        {/* Company */}
        <div style={{ marginBottom:14 }}>
          <label style={{
            display:"block", fontFamily:C.mono, fontSize:9, color:C.muted,
            letterSpacing:"0.14em", textTransform:"uppercase", marginBottom:6, fontWeight:600,
          }}>
            Tally Company
          </label>
          {companies?.length > 0 ? (
            <select
              value={company}
              onChange={(e) => setCompany(e.target.value)}
              style={{ ...inp(), cursor:"pointer", appearance:"none" }}
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
              placeholder="e.g. Rajlaxmi Solutions Private Limited"
              style={inp()}
              onFocus={onFocus} onBlur={onBlur}
            />
          )}
        </div>

        {/* Date range */}
        <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:12, marginBottom:20 }}>
          {[["FROM DATE", fromDate, setFromDate], ["TO DATE", toDate, setToDate]].map(([lbl, val, setter]) => (
            <div key={lbl}>
              <label style={{
                display:"block", fontFamily:C.mono, fontSize:9, color:C.muted,
                letterSpacing:"0.14em", textTransform:"uppercase", marginBottom:6, fontWeight:600,
              }}>
                {lbl}
              </label>
              <input
                type="date" value={val}
                onChange={(e) => setter(e.target.value)}
                style={{ ...inp(), fontFamily:C.mono, fontSize:12 }}
                onFocus={onFocus} onBlur={onBlur}
              />
            </div>
          ))}
        </div>

        {/* Run button */}
        <button
          onClick={runCheck}
          disabled={loading}
          style={{
            width:"100%", padding:"13px 20px",
            borderRadius:11, border:"none",
            background: loading ? C.accentL : `linear-gradient(135deg,${C.accent},${C.accentD})`,
            color: loading ? C.accent : "#fff",
            fontFamily:C.title, fontSize:14, fontWeight:700,
            letterSpacing:"-0.2px", cursor: loading ? "not-allowed" : "pointer",
            display:"flex", alignItems:"center", justifyContent:"center", gap:10,
            boxShadow: loading ? "none" : `0 4px 16px ${C.accent}44`,
            transition:"all .2s",
          }}
        >
          {loading ? (
            <><Spin size={15} color={C.accent}/> Fetching all masters from Tally…</>
          ) : (
            <><span style={{ fontSize:16 }}>▶</span> Run Full Middleware Check</>
          )}
        </button>

        {/* Error */}
        {error && (
          <div style={{
            marginTop:14, padding:"11px 15px",
            background:C.redL, border:`1.5px solid ${C.redB}`,
            borderRadius:10, animation:"mc-fade-in .2s ease",
          }}>
            <p style={{ fontFamily:C.mono, fontSize:11, color:C.red, margin:0, whiteSpace:"pre-wrap" }}>
              ✗ {error}
            </p>
          </div>
        )}
      </div>

      {/* ── Results ──────────────────────────────────────────────────────── */}
      {(report || rawResponse) && (
        <div ref={resultsRef} style={{ display:"flex", flexDirection:"column", gap:14, animation:"mc-pop .25s ease" }}>

          {/* Status banner */}
          {(() => {
            if (!effectiveStatus) return null;
            const cfg = STATUS_CFG[effectiveStatus] || STATUS_CFG.warn;
            return (
              <div style={{
                background:cfg.bg, border:`1.5px solid ${cfg.border}`,
                borderRadius:14, padding:"18px 20px",
                animation:"mc-pop .2s ease",
              }}>
                <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom: report?.summary ? 16 : 0 }}>
                  <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                    <span style={{
                      width:32, height:32, borderRadius:8,
                      background:cfg.text + "20", border:`1.5px solid ${cfg.border}`,
                      display:"flex", alignItems:"center", justifyContent:"center",
                      fontSize:16, flexShrink:0,
                    }}>
                      {cfg.icon}
                    </span>
                    <div>
                      <p style={{ fontFamily:C.title, fontWeight:800, fontSize:14, color:cfg.text, margin:0, letterSpacing:"-0.3px" }}>
                        {cfg.label}
                      </p>
                      {report?.finishedAt && (
                        <p style={{ fontFamily:C.mono, fontSize:9, color:C.muted, margin:"2px 0 0" }}>
                          Completed at {new Date(report.finishedAt).toLocaleTimeString("en-IN", { hour12:false })}
                        </p>
                      )}
                    </div>
                  </div>
                </div>

                {/* Summary stat pills */}
                {report?.summary && (
                  <div style={{ display:"flex", gap:8, flexWrap:"wrap", marginTop:4 }}>
                    <StatBadge label="Groups"   value={report.summary.groups}   color={C.accent}  bg={C.accentL} bd={C.accentB}/>
                    <StatBadge label="Ledgers"  value={report.summary.ledgers}  color={C.accent}  bg={C.accentL} bd={C.accentB}/>
                    <StatBadge label="Stock"    value={report.summary.stockItems} color={C.amber} bg={C.amberL}  bd={C.amberB}/>
                    <StatBadge label="Vouchers" value={report.summary.vouchers} color={C.green}   bg={C.greenL}  bd={C.greenB}/>
                  </div>
                )}
              </div>
            );
          })()}

          {/* Check results */}
          {report && (
            <>
              {/* Connection */}
              <div style={{
                background:C.card,
                border:`1.5px solid ${C.border}`,
                borderRadius:16,
                boxShadow:"0 8px 32px rgba(13,21,50,.10), 0 2px 0 rgba(255,255,255,.95) inset, 0 1px 0 rgba(13,21,50,.06)",
                overflow:"hidden",
              }}>
                <div style={{
                  padding:"13px 20px",
                  background:`linear-gradient(135deg,${C.accentL},${C.surface})`,
                  borderBottom:`1.5px solid ${C.accentB}`,
                  display:"flex", alignItems:"center", gap:10,
                }}>
                  <div style={{ width:28, height:28, borderRadius:8, background:C.accent, display:"flex", alignItems:"center", justifyContent:"center", fontSize:13, boxShadow:`0 3px 10px ${C.accent}55` }}>🔌</div>
                  <span style={{ fontFamily:C.title, fontWeight:800, fontSize:12, color:C.ink, letterSpacing:"-0.2px" }}>Connection</span>
                </div>
                <div style={{ padding:"8px 16px 14px" }}>
                  <CheckRow icon="🔌" label="Tally Ping" check={report.checks?.ping}/>
                  <CheckRow icon="🏢" label="Companies" check={report.checks?.companies}/>
                </div>
              </div>

              {/* Accounting Masters */}
              <div style={{
                background:C.card,
                border:`1.5px solid ${C.border}`,
                borderRadius:16,
                boxShadow:"0 8px 32px rgba(13,21,50,.10), 0 2px 0 rgba(255,255,255,.95) inset, 0 1px 0 rgba(13,21,50,.06)",
                overflow:"hidden",
              }}>
                <div style={{
                  padding:"13px 20px",
                  background:`linear-gradient(135deg,#f5f3ff,${C.surface})`,
                  borderBottom:`1.5px solid #ddd6fe`,
                  display:"flex", alignItems:"center", gap:10,
                }}>
                  <div style={{ width:28, height:28, borderRadius:8, background:"#7c3aed", display:"flex", alignItems:"center", justifyContent:"center", fontSize:13, boxShadow:"0 3px 10px #7c3aed55" }}>📒</div>
                  <span style={{ fontFamily:C.title, fontWeight:800, fontSize:12, color:C.ink, letterSpacing:"-0.2px" }}>Accounting Masters</span>
                </div>
                <div style={{ padding:"8px 16px 14px" }}>
                  <CheckRow icon="📒" label="Ledgers" check={report.checks?.ledgers}>
                    <StatRow>
                      <StatBox label="Total"     value={report.checks?.ledgers?.count}         color={C.accent} bg={C.accentL} bd={C.accentB}/>
                      <StatBox label="Customers" value={report.checks?.ledgers?.customerCount}  color={C.green}  bg={C.greenL}  bd={C.greenB}/>
                      <StatBox label="Suppliers" value={report.checks?.ledgers?.supplierCount}  color={C.amber}  bg={C.amberL}  bd={C.amberB}/>
                      <StatBox label="Party"     value={report.checks?.ledgers?.partyCount}     color="#0d9488"  bg="#f0fdfa"    bd="#99f6e4"/>
                      <StatBox label="GST"       value={report.checks?.ledgers?.withGstin}      color="#7c3aed"  bg="#f5f3ff"    bd="#ddd6fe"/>
                      <StatBox label="Email"     value={report.checks?.ledgers?.withEmail}      color="#0d9488"  bg="#f0fdfa"    bd="#99f6e4"/>
                    </StatRow>
                  </CheckRow>
                  <CheckRow icon="🗂" label="Groups" check={report.checks?.groups}><StatRow><StatBox label="Total" value={report.checks?.groups?.count} color={C.accent} bg={C.accentL} bd={C.accentB}/></StatRow></CheckRow>
                  <CheckRow icon="🏷" label="Voucher Types" check={report.checks?.voucherTypes}>
                    <StatRow>
                      <StatBox label="Total"  value={report.checks?.voucherTypes?.count}       color={C.accent} bg={C.accentL} bd={C.accentB}/>
                      <StatBox label="Active" value={report.checks?.voucherTypes?.activeCount} color={C.green}  bg={C.greenL}  bd={C.greenB}/>
                    </StatRow>
                  </CheckRow>
                  <CheckRow icon="📂" label="Cost Categories" check={report.checks?.costCategories}><StatRow><StatBox label="Total" value={report.checks?.costCategories?.count} color={C.accent} bg={C.accentL} bd={C.accentB}/></StatRow></CheckRow>
                  <CheckRow icon="🏬" label="Cost Centres" check={report.checks?.costCentres}><StatRow><StatBox label="Total" value={report.checks?.costCentres?.count} color={C.accent} bg={C.accentL} bd={C.accentB}/></StatRow></CheckRow>
                  <CheckRow icon="💱" label="Currencies" check={report.checks?.currencies}><StatRow><StatBox label="Total" value={report.checks?.currencies?.count} color={C.accent} bg={C.accentL} bd={C.accentB}/></StatRow></CheckRow>
                  <CheckRow icon="📊" label="Budgets" check={report.checks?.budgets}><StatRow><StatBox label="Total" value={report.checks?.budgets?.count} color={C.accent} bg={C.accentL} bd={C.accentB}/></StatRow></CheckRow>
                </div>
              </div>

              {/* Inventory Masters */}
              <div style={{
                background:C.card,
                border:`1.5px solid ${C.border}`,
                borderRadius:16,
                boxShadow:"0 8px 32px rgba(13,21,50,.10), 0 2px 0 rgba(255,255,255,.95) inset, 0 1px 0 rgba(13,21,50,.06)",
                overflow:"hidden",
              }}>
                <div style={{
                  padding:"13px 20px",
                  background:`linear-gradient(135deg,${C.amberL},${C.surface})`,
                  borderBottom:`1.5px solid ${C.amberB}`,
                  display:"flex", alignItems:"center", gap:10,
                }}>
                  <div style={{ width:28, height:28, borderRadius:8, background:C.amber, display:"flex", alignItems:"center", justifyContent:"center", fontSize:13, boxShadow:`0 3px 10px ${C.amber}55` }}>📦</div>
                  <span style={{ fontFamily:C.title, fontWeight:800, fontSize:12, color:C.ink, letterSpacing:"-0.2px" }}>Inventory Masters</span>
                </div>
                <div style={{ padding:"8px 16px 14px" }}>
                  <CheckRow icon="🗃" label="Stock Groups" check={report.checks?.stockGroups}><StatRow><StatBox label="Total" value={report.checks?.stockGroups?.count} color={C.accent} bg={C.accentL} bd={C.accentB}/></StatRow></CheckRow>
                  <CheckRow icon="📦" label="Stock Items"      check={report.checks?.stockItems}>
                    <StatRow>
                      <StatBox label="Total"    value={report.checks?.stockItems?.count}    color={C.accent} bg={C.accentL} bd={C.accentB}/>
                      <StatBox label="With HSN" value={report.checks?.stockItems?.hsnCount} color="#0d9488"  bg="#f0fdfa"   bd="#99f6e4"/>
                      {report.checks?.stockItems?.totalClosingValue > 0 && (
                        <StatBox label="Closing Value" value={`₹${Number(report.checks.stockItems.totalClosingValue).toLocaleString("en-IN")}`} color={C.green} bg={C.greenL} bd={C.greenB}/>
                      )}
                    </StatRow>
                  </CheckRow>
                  <CheckRow icon="🏷" label="Stock Categories" check={report.checks?.stockCategories}><StatRow><StatBox label="Total" value={report.checks?.stockCategories?.count} color={C.accent} bg={C.accentL} bd={C.accentB}/></StatRow></CheckRow>
                  <CheckRow icon="📐" label="Units of Measure" check={report.checks?.units}>
                    <StatRow>
                      <StatBox label="Total"    value={report.checks?.units?.count}         color={C.accent} bg={C.accentL} bd={C.accentB}/>
                      <StatBox label="Simple"   value={report.checks?.units?.simpleCount}   color={C.green}  bg={C.greenL}  bd={C.greenB}/>
                      <StatBox label="Compound" value={report.checks?.units?.compoundCount} color={C.muted}  bg={C.surface} bd={C.border}/>
                    </StatRow>
                  </CheckRow>
                  <CheckRow icon="🏭" label="Godowns" check={report.checks?.godowns}><StatRow><StatBox label="Total" value={report.checks?.godowns?.count} color={C.accent} bg={C.accentL} bd={C.accentB}/></StatRow></CheckRow>
                </div>
              </div>

              {/* Transactions */}
              <div style={{
                background:C.card,
                border:`1.5px solid ${C.border}`,
                borderRadius:16,
                boxShadow:"0 8px 32px rgba(13,21,50,.10), 0 2px 0 rgba(255,255,255,.95) inset, 0 1px 0 rgba(13,21,50,.06)",
                overflow:"hidden",
              }}>
                <div style={{
                  padding:"13px 20px",
                  background:`linear-gradient(135deg,${C.greenL},${C.surface})`,
                  borderBottom:`1.5px solid ${C.greenB}`,
                  display:"flex", alignItems:"center", gap:10,
                }}>
                  <div style={{ width:28, height:28, borderRadius:8, background:C.green, display:"flex", alignItems:"center", justifyContent:"center", fontSize:13, boxShadow:`0 3px 10px ${C.green}55` }}>🧾</div>
                  <span style={{ fontFamily:C.title, fontWeight:800, fontSize:12, color:C.ink, letterSpacing:"-0.2px" }}>Transactions</span>
                </div>
                <div style={{ padding:"8px 16px 14px" }}>
                  <CheckRow icon="🧾" label="Vouchers" check={report.checks?.vouchers}>
                    <StatRow>
                      <StatBox label="Total"   value={report.checks?.vouchers?.count}      color={C.green}  bg={C.greenL}  bd={C.greenB}/>
                      <StatBox label="Types"   value={report.checks?.vouchers?.typeCount}  color={C.accent} bg={C.accentL} bd={C.accentB}/>
                      <StatBox label="Parties" value={report.checks?.vouchers?.partyCount} color="#0d9488"  bg="#f0fdfa"   bd="#99f6e4"/>
                    </StatRow>
                    {report.checks?.vouchers?.count === 0 && (
                      <p style={{ fontFamily:C.mono, fontSize:11, color:C.amber, marginLeft:48, marginTop:8 }}>
                        ⚠ No vouchers in this date range — try a wider range.
                      </p>
                    )}
                  </CheckRow>
                </div>
              </div>

              {/* Errors */}
              {report.errors?.length > 0 && (
                <div style={{
                  background:C.redL, border:`1.5px solid ${C.redB}`,
                  borderRadius:14, padding:"16px 20px",
                  display:"flex", flexDirection:"column", gap:8,
                  animation:"mc-pop .2s ease",
                }}>
                  <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:4 }}>
                    <span style={{ fontSize:14 }}>🚨</span>
                    <p style={{ fontFamily:C.mono, fontSize:9, color:C.red, textTransform:"uppercase", letterSpacing:"0.14em", fontWeight:700, margin:0 }}>
                      Issues Found
                    </p>
                  </div>
                  {report.errors.map((e, i) => (
                    <div key={i} style={{
                      display:"flex", alignItems:"flex-start", gap:8,
                      padding:"8px 12px", borderRadius:8,
                      background:C.redB + "60", border:`1px solid ${C.redB}`,
                    }}>
                      <span style={{ color:C.red, fontWeight:700, flexShrink:0 }}>✗</span>
                      <p style={{ fontFamily:C.mono, fontSize:11, color:C.red, margin:0 }}>{e}</p>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}