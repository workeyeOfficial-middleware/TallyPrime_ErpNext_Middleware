import { useState, useEffect, useRef, useCallback } from "react";

const BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:4000/api";
const TODAY    = new Date().toISOString().slice(0, 10);

const C = {
  bg:      "#0d1117",
  surface: "#161b22",
  card:    "#1c2128",
  border:  "#30363d",
  borderB: "#484f58",
  ink:     "#f0f6fc",
  sub:     "#cdd9e5",
  muted:   "#8b949e",
  dimmed:  "#6e7681",
  accent:  "#4493f8",
  accentL: "#1f3358",
  green:   "#3fb950",
  greenL:  "#122d1a",
  greenB:  "#238636",
  amber:   "#e3b341",
  amberL:  "#2d2000",
  amberB:  "#9e6a03",
  red:     "#ff7b72",
  redL:    "#2d1216",
  redB:    "#b62324",
  mono:    "'JetBrains Mono','Fira Code','Cascadia Code',monospace",
  sans:    "'Inter','DM Sans','Plus Jakarta Sans',sans-serif",
};

const LEVEL = {
  info:    { color: C.sub,   bg: "transparent",      pill: "#30363d", pillText: C.muted,  tag: "INFO",    dot: C.dimmed },
  success: { color: C.green, bg: C.greenL + "99",    pill: C.greenB,  pillText: "#aff5b4", tag: "OK",     dot: C.green  },
  warn:    { color: C.amber, bg: C.amberL + "99",    pill: C.amberB,  pillText: "#ffd8a8", tag: "WARN",   dot: C.amber  },
  error:   { color: C.red,   bg: C.redL   + "99",    pill: C.redB,    pillText: "#ffa198", tag: "ERROR",  dot: C.red    },
};

const selStyle = {
  background: C.surface,
  border: `1.5px solid ${C.border}`,
  borderRadius: 8,
  color: C.sub,
  fontFamily: C.sans,
  fontSize: 12,
  fontWeight: 500,
  padding: "7px 10px",
  outline: "none",
  cursor: "pointer",
  appearance: "none",
  WebkitAppearance: "none",
};

function useCounts(logs) {
  return logs.reduce((acc, l) => { acc[l.level] = (acc[l.level] || 0) + 1; return acc; }, {});
}

export function LiveLogs({ currentCompany = null }) {
  const [company,     setCompany]     = useState(currentCompany || "");
  const [companies,   setCompanies]   = useState([]);
  const [fromDate,    setFromDate]    = useState(TODAY);
  const [toDate,      setToDate]      = useState(TODAY);
  const [levelFilter, setLevelFilter] = useState("");
  const [logs,        setLogs]        = useState([]);
  const [loading,     setLoading]     = useState(false);
  const pollRef  = useRef(null);
  const scrollRef = useRef(null);

  useEffect(() => {
    fetch(`${BASE_URL}/logs/companies`)
      .then(r => r.ok ? r.json() : { companies: [] })
      .then(d => setCompanies(d.companies || []))
      .catch(() => {});
  }, []);

  const fetchLogs = useCallback(() => {
    const p = new URLSearchParams({ limit: 500 });
    if (company)     p.set("company",  company);
    if (fromDate)    p.set("fromDate", fromDate);
    if (toDate)      p.set("toDate",   toDate);
    if (levelFilter) p.set("level",    levelFilter);
    setLoading(true);
    fetch(`${BASE_URL}/logs?${p}`)
      .then(r => r.ok ? r.json() : { logs: [] })
      .then(d => setLogs(d.logs || []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [company, fromDate, toDate, levelFilter]);

  useEffect(() => {
    fetchLogs();
    pollRef.current = setInterval(fetchLogs, 3000);
    return () => clearInterval(pollRef.current);
  }, [fetchLogs]);

  const counts = useCounts(logs);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20, fontFamily: C.sans }}>
      <style>{`
        @keyframes ll-pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.4;transform:scale(.75)} }
        @keyframes ll-in    { from{opacity:0;transform:translateY(-3px)} to{opacity:1;transform:none} }
        .ll-row:hover { background: rgba(99,110,123,.08) !important; }
        .ll-ctrl:hover { border-color: ${C.accent} !important; }
        .ll-btn-pri:hover  { opacity:.88; }
        .ll-btn-sec:hover  { border-color:${C.borderB} !important; color:${C.sub} !important; }
        .ll-ctrl option { background:${C.surface}; color:${C.sub}; }
        input[type=date]::-webkit-calendar-picker-indicator { filter: invert(0.6); cursor:pointer; }
      `}</style>

      {/* ── Header ─────────────────────────────────────────────────────── */}
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", flexWrap:"wrap", gap:12 }}>
        <div style={{ display:"flex", alignItems:"center", gap:12 }}>
          <div style={{ width:4, height:36, background:`linear-gradient(180deg,${C.accent},${C.green})`, borderRadius:4 }}/>
          <div>
            <h2 style={{ margin:0, fontFamily:C.sans, fontWeight:700, fontSize:18, color:C.ink, letterSpacing:"-0.5px" }}>
              Live Logs
            </h2>
            <p style={{ margin:0, fontFamily:C.mono, fontSize:10, color:C.muted, marginTop:2 }}>
              {company ? `● ${company.replace(/-/g," ")}` : "● All companies · middleware output stream"}
            </p>
          </div>
        </div>

        <div style={{ display:"flex", alignItems:"center", gap:10 }}>
          {/* Count badges */}
          {[
            { key:"error",   label:"Errors",   color:C.red,   bg:C.redL,   border:C.redB   },
            { key:"warn",    label:"Warnings", color:C.amber, bg:C.amberL, border:C.amberB },
            { key:"success", label:"Success",  color:C.green, bg:C.greenL, border:C.greenB },
          ].map(({ key, label, color, bg, border }) => counts[key] > 0 && (
            <div key={key} style={{
              display:"flex", alignItems:"center", gap:5,
              padding:"5px 12px", borderRadius:20,
              background:bg, border:`1.5px solid ${border}`,
            }}>
              <span style={{ width:7, height:7, borderRadius:"50%", background:color, display:"inline-block" }}/>
              <span style={{ fontFamily:C.sans, fontSize:12, fontWeight:600, color }}>{counts[key]} {label}</span>
            </div>
          ))}

          {/* Live indicator */}
          <div style={{
            display:"flex", alignItems:"center", gap:7,
            padding:"6px 14px", borderRadius:20,
            background: loading ? C.amberL : C.accentL,
            border:`1.5px solid ${loading ? C.amberB : C.accent}55`,
          }}>
            <span style={{
              width:8, height:8, borderRadius:"50%",
              background: loading ? C.amber : C.accent,
              animation:"ll-pulse 1.4s ease-in-out infinite",
              display:"inline-block", boxShadow:`0 0 8px ${loading ? C.amber : C.accent}`,
            }}/>
            <span style={{ fontFamily:C.sans, fontSize:12, fontWeight:600, color: loading ? C.amber : C.accent }}>
              {loading ? "Refreshing…" : `Live · ${logs.length} entries`}
            </span>
          </div>
        </div>
      </div>

      {/* ── Filter bar ─────────────────────────────────────────────────── */}
      <div style={{
        display:"flex", alignItems:"flex-end", gap:12, flexWrap:"wrap",
        background:C.surface, border:`1.5px solid ${C.border}`,
        borderRadius:12, padding:"14px 18px",
      }}>
        {/* Company */}
        <div style={{ display:"flex", flexDirection:"column", gap:5, flex:"2 1 180px" }}>
          <label style={{ fontFamily:C.sans, fontSize:11, fontWeight:600, color:C.muted, letterSpacing:"0.04em" }}>
            🏢 Company
          </label>
          <select className="ll-ctrl" value={company} onChange={e => setCompany(e.target.value)}
            style={{ ...selStyle, width:"100%" }}>
            <option value="">— All Companies —</option>
            {companies.map(c => <option key={c} value={c}>{c.replace(/-/g," ")}</option>)}
          </select>
        </div>

        {/* From */}
        <div style={{ display:"flex", flexDirection:"column", gap:5, flex:"1 1 130px" }}>
          <label style={{ fontFamily:C.sans, fontSize:11, fontWeight:600, color:C.muted, letterSpacing:"0.04em" }}>
            📅 From Date
          </label>
          <input type="date" className="ll-ctrl" value={fromDate} onChange={e => setFromDate(e.target.value)}
            style={{ ...selStyle, width:"100%" }}/>
        </div>

        {/* To */}
        <div style={{ display:"flex", flexDirection:"column", gap:5, flex:"1 1 130px" }}>
          <label style={{ fontFamily:C.sans, fontSize:11, fontWeight:600, color:C.muted, letterSpacing:"0.04em" }}>
            📅 To Date
          </label>
          <input type="date" className="ll-ctrl" value={toDate} onChange={e => setToDate(e.target.value)}
            style={{ ...selStyle, width:"100%" }}/>
        </div>

        {/* Level */}
        <div style={{ display:"flex", flexDirection:"column", gap:5, flex:"1 1 110px" }}>
          <label style={{ fontFamily:C.sans, fontSize:11, fontWeight:600, color:C.muted, letterSpacing:"0.04em" }}>
            🔍 Level
          </label>
          <select className="ll-ctrl" value={levelFilter} onChange={e => setLevelFilter(e.target.value)}
            style={{ ...selStyle, width:"100%" }}>
            <option value="">All Levels</option>
            <option value="error">✗  Error</option>
            <option value="warn">⚠  Warning</option>
            <option value="success">✓  Success</option>
            <option value="info">·  Info</option>
          </select>
        </div>

        {/* Buttons */}
        <div style={{ display:"flex", flexDirection:"column", gap:5 }}>
          <label style={{ fontFamily:C.sans, fontSize:11, color:"transparent" }}>.</label>
          <div style={{ display:"flex", gap:8 }}>
            <button className="ll-btn-pri" onClick={fetchLogs} style={{
              fontFamily:C.sans, fontSize:13, fontWeight:600, color:"#fff",
              background:`linear-gradient(135deg,${C.accent},#1a56db)`,
              border:"none", borderRadius:8, padding:"7px 20px", cursor:"pointer",
              boxShadow:`0 2px 8px ${C.accent}44`,
            }}>
              Apply
            </button>
            <button className="ll-btn-sec" onClick={() => {
              setCompany(currentCompany || "");
              setFromDate(TODAY); setToDate(TODAY); setLevelFilter("");
            }} style={{
              fontFamily:C.sans, fontSize:13, fontWeight:500, color:C.muted,
              background:"transparent", border:`1.5px solid ${C.border}`,
              borderRadius:8, padding:"7px 16px", cursor:"pointer",
            }}>
              Reset
            </button>
          </div>
        </div>

        <div style={{ marginLeft:"auto", alignSelf:"center", display:"flex", alignItems:"center", gap:6 }}>
          <span style={{ fontSize:14 }}>🗑</span>
          <span style={{ fontFamily:C.sans, fontSize:11, color:C.dimmed }}>Auto-deleted after 15 days</span>
        </div>
      </div>

      {/* ── Terminal ────────────────────────────────────────────────────── */}
      <div style={{
        background:"#010409",
        border:`1.5px solid ${C.border}`,
        borderRadius:14,
        overflow:"hidden",
        boxShadow:"0 4px 24px rgba(0,0,0,.5), 0 0 0 1px #ffffff06",
      }}>
        {/* Title bar */}
        <div style={{
          background:C.surface,
          borderBottom:`1.5px solid ${C.border}`,
          padding:"10px 18px",
          display:"flex", alignItems:"center", gap:0,
        }}>
          <div style={{ display:"flex", gap:7, marginRight:16 }}>
            {[C.red, C.amber, C.green].map((c,i) => (
              <span key={i} style={{
                width:12, height:12, borderRadius:"50%",
                background:c, display:"inline-block",
                boxShadow:`0 0 6px ${c}88`,
              }}/>
            ))}
          </div>
          <span style={{ fontFamily:C.mono, fontSize:11, color:C.muted, flex:1, textAlign:"center", letterSpacing:"0.06em" }}>
            {company ? `${company.replace(/-/g," ")}.log` : "middleware.log — all companies"}
          </span>
          <span style={{ fontFamily:C.mono, fontSize:10, color:C.dimmed }}>UTF-8</span>
        </div>

        {/* Column headers */}
        <div style={{
          display:"flex", alignItems:"center",
          padding:"6px 0", borderBottom:`1px solid ${C.border}22`,
          background:"#010409",
        }}>
          <span style={{ fontFamily:C.mono, fontSize:9, color:C.dimmed, width:46, textAlign:"right", paddingRight:12, flexShrink:0, letterSpacing:"0.1em" }}>#</span>
          <span style={{ fontFamily:C.mono, fontSize:9, color:C.dimmed, width:140, paddingRight:10, flexShrink:0, letterSpacing:"0.1em" }}>TIMESTAMP</span>
          <span style={{ fontFamily:C.mono, fontSize:9, color:C.dimmed, width:62, paddingRight:10, flexShrink:0, letterSpacing:"0.1em" }}>LEVEL</span>
          {!company && <span style={{ fontFamily:C.mono, fontSize:9, color:C.dimmed, width:120, paddingRight:10, flexShrink:0, letterSpacing:"0.1em" }}>COMPANY</span>}
          <span style={{ fontFamily:C.mono, fontSize:9, color:C.dimmed, flex:1, letterSpacing:"0.1em" }}>MESSAGE</span>
        </div>

        {/* Log rows */}
        <div ref={scrollRef} style={{
          height:480, overflowY:"auto",
          display:"flex", flexDirection:"column",
          background:"#010409",
          scrollbarWidth:"thin",
          scrollbarColor:`${C.border} transparent`,
        }}>
          {logs.length === 0 ? (
            <div style={{ display:"flex", flexDirection:"column", alignItems:"center", justifyContent:"center", height:"100%", gap:16 }}>
              <div style={{ width:56, height:56, borderRadius:14, background:C.surface, border:`1.5px solid ${C.border}`, display:"flex", alignItems:"center", justifyContent:"center", fontSize:26 }}>📋</div>
              <div style={{ textAlign:"center" }}>
                <p style={{ fontFamily:C.sans, fontSize:14, fontWeight:600, color:C.muted, margin:0 }}>No logs found</p>
                <p style={{ fontFamily:C.sans, fontSize:12, color:C.dimmed, margin:"6px 0 0" }}>
                  {company ? "No logs for this company in the selected range" : "Run a middleware check to see output"}
                </p>
              </div>
            </div>
          ) : logs.map((log, idx) => {
            const lvl = LEVEL[log.level] || LEVEL.info;
            return (
              <div key={log.id} className="ll-row" style={{
                display:"flex", alignItems:"center",
                padding:"5px 0",
                background: lvl.bg,
                borderLeft: log.level !== "info" ? `3px solid ${lvl.dot}` : "3px solid transparent",
                transition:"background 0.1s",
                minHeight:32,
              }}>
                {/* Line no */}
                <span style={{ fontFamily:C.mono, fontSize:11, color:C.dimmed, flexShrink:0, width:46, textAlign:"right", paddingRight:12, userSelect:"none", fontVariantNumeric:"tabular-nums" }}>
                  {String(idx + 1).padStart(3, " ")}
                </span>

                {/* Timestamp */}
                <span style={{ fontFamily:C.mono, fontSize:11, color:C.muted, flexShrink:0, paddingRight:10, fontVariantNumeric:"tabular-nums", width:140 }}>
                  {new Date(log.ts).toLocaleDateString("en-IN", { day:"2-digit", month:"short", year:"2-digit" })}
                  {"  "}{new Date(log.ts).toLocaleTimeString("en-IN", { hour12:false })}
                </span>

                {/* Level pill */}
                <span style={{
                  fontFamily:C.mono, fontSize:9, fontWeight:700,
                  color: lvl.pillText,
                  background: lvl.pill,
                  padding:"2px 7px", borderRadius:4,
                  flexShrink:0, width:54, textAlign:"center",
                  marginRight:10, letterSpacing:"0.06em",
                }}>
                  {lvl.tag}
                </span>

                {/* Company chip — only in "all" view */}
                {!company && log.company && (
                  <span style={{
                    fontFamily:C.mono, fontSize:10, color:"#79c0ff",
                    background:"#1f2d3d", border:"1px solid #264466",
                    borderRadius:4, padding:"1px 6px",
                    marginRight:10, flexShrink:0,
                    maxWidth:116, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap",
                  }}>
                    {log.company.split(/[\s\-(]/)[0]}
                  </span>
                )}

                {/* Message */}
                <span style={{
                  fontFamily:C.mono, fontSize:12, fontWeight: log.level === "info" ? 400 : 600,
                  color: log.level === "info" ? C.sub : lvl.color,
                  flex:1, lineHeight:1.5,
                  wordBreak:"break-word", paddingRight:18,
                }}>
                  {log.message}
                </span>
              </div>
            );
          })}
        </div>

        {/* Status bar */}
        <div style={{
          background:C.surface,
          borderTop:`1.5px solid ${C.border}`,
          padding:"7px 18px",
          display:"flex", alignItems:"center", justifyContent:"space-between",
        }}>
          <div style={{ display:"flex", gap:20 }}>
            {[
              { key:"error",   label:"Errors",   color:C.red   },
              { key:"warn",    label:"Warnings", color:C.amber },
              { key:"success", label:"Success",  color:C.green },
              { key:"info",    label:"Info",     color:C.dimmed},
            ].map(({ key, label, color }) => (
              <span key={key} style={{ fontFamily:C.sans, fontSize:12, fontWeight:500, color: counts[key] > 0 ? color : C.dimmed }}>
                {label}: <strong style={{ fontWeight:700 }}>{counts[key] || 0}</strong>
              </span>
            ))}
          </div>
          <span style={{ fontFamily:C.sans, fontSize:11, color:C.dimmed }}>
            {logs.length} total entries · 15-day retention
          </span>
        </div>
      </div>
    </div>
  );
}