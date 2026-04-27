import { useState, useEffect, useRef, useCallback } from "react";
import { tallyAPI } from "../api/tallyAPI";

const TODAY      = new Date().toISOString().slice(0, 10);
function getFYStart() {
  const now = new Date();
  const year = now.getMonth() >= 3 ? now.getFullYear() : now.getFullYear() - 1;
  return `${year}-04-01`;
}
const YEAR_START = getFYStart();
const BASE_URL   = process.env.REACT_APP_API_URL || "http://localhost:4000/api";
const ACTIVE_JOB_KEY = "sync_active_job";
const AUTO_SYNC_OPTS_KEY = "auto_sync_options"; // persisted separately from manual opts

const C = {
  card:"#ffffff",surface:"#f0f3fa",bg:"#e8edf6",border:"#d8dff0",borderH:"#b0bcd8",
  ink:"#0a0e1a",muted:"#5a6482",dim:"#8a94b0",
  accent:"#2563eb",accentD:"#1d4ed8",accentL:"#eef4ff",accentB:"#b8d0fe",
  green:"#16a34a",greenD:"#15803d",greenL:"#f0fdf4",greenB:"#bbf7d0",
  amber:"#d97706",amberL:"#fffbeb",amberB:"#fde68a",
  red:"#dc2626",redL:"#fef2f2",redB:"#fecaca",
  teal:"#0d9488",tealL:"#f0fdfa",tealB:"#99f6e4",
  mono:"'JetBrains Mono','Fira Code',monospace",
  sans:"'DM Sans','Plus Jakarta Sans',sans-serif",
  title:"'Syne','Plus Jakarta Sans',sans-serif",
};

const INTERVALS=[
  {label:"15 min",value:15*60*1000},{label:"30 min",value:30*60*1000},
  {label:"1 hour",value:60*60*1000},{label:"2 hours",value:120*60*1000},
  {label:"4 hours",value:240*60*1000},{label:"8 hours",value:480*60*1000},
  {label:"Daily",value:24*60*60*1000},
];
const SYNC_OPTIONS=[
  {key:"syncChartOfAccounts",icon:"🗂",label:"Chart of Accounts",sub:"→ Account Groups (run first)",individual:"chart-of-accounts"},
  {key:"syncLedgers",icon:"👥",label:"Ledgers",sub:"→ Customers / Suppliers",individual:"ledgers"},
  {key:"syncSmartLedgers",icon:"⚡",label:"Smart Ledgers",sub:"→ Only used in date range",individual:"smart-ledgers"},
  {key:"syncOpeningBalances",icon:"💰",label:"Opening Balances",sub:"→ Opening Entry",individual:"opening-balances"},
  {key:"syncGodowns",icon:"🏭",label:"Godowns",sub:"→ Warehouses",individual:"godowns"},
  {key:"syncCostCentres",icon:"📁",label:"Cost Centres",sub:"→ ERPNext Cost Centers",individual:"cost-centres"},
  {key:"syncStock",icon:"📦",label:"Stock Items",sub:"→ Items",individual:"stock"},
  {key:"syncVouchers",icon:"🧾",label:"Vouchers",sub:"→ Journal Entries",individual:"vouchers"},
  {key:"syncInvoices",icon:"🧾",label:"Invoices",sub:"Sales/Purchase → Invoices",individual:"invoices"},
];

const EMPTY_OPTS = Object.fromEntries(SYNC_OPTIONS.map(o => [o.key, false]));

const CREDS_KEY="erp_creds";
function loadAllCreds(){try{return JSON.parse(localStorage.getItem(CREDS_KEY)||"{}");}catch{return {};}}
function saveAllCreds(all){localStorage.setItem(CREDS_KEY,JSON.stringify(all));}
function fmt(n){return(n??0).toLocaleString();}
function fmtTime(ms){if(!ms)return"—";const s=Math.floor(ms/1000);if(s<60)return`${s}s`;const m=Math.floor(s/60);if(m<60)return`${m}m ${s%60}s`;return`${Math.floor(m/60)}h ${m%60}m`;}

function Spinner({size=14,color=C.accent}){return <span style={{display:"inline-block",width:size,height:size,borderRadius:"50%",border:`2px solid ${color}20`,borderTopColor:color,animation:"se-spin .7s linear infinite",flexShrink:0}}/>;}
const inp=(extra={})=>({width:"100%",padding:"9px 13px",border:`1.5px solid ${C.border}`,borderRadius:9,fontFamily:C.sans,fontSize:13,color:C.ink,background:C.surface,outline:"none",transition:"border-color .15s,background .15s,box-shadow .15s",boxSizing:"border-box",...extra});
const onFocus=(e)=>{e.target.style.borderColor=C.accent;e.target.style.background=C.card;e.target.style.boxShadow=`0 0 0 3px ${C.accentB}55`;};
const onBlur=(e)=>{e.target.style.borderColor=C.border;e.target.style.background=C.surface;e.target.style.boxShadow="none";};

function StatusBadge({status}){
  const map={ok:{bg:C.greenL,bd:C.greenB,color:C.green,label:"SUCCESS"},uptodate:{bg:C.tealL,bd:C.tealB,color:C.teal,label:"UP TO DATE"},warning:{bg:C.amberL,bd:C.amberB,color:C.amber,label:"WARNING"},failed:{bg:C.redL,bd:C.redB,color:C.red,label:"FAILED"},running:{bg:C.accentL,bd:C.accentB,color:C.accent,label:"RUNNING"}};
  const s=map[status]||{bg:C.surface,bd:C.border,color:C.muted,label:(status||"—").toUpperCase()};
  return <span style={{fontFamily:C.mono,fontSize:9,fontWeight:700,letterSpacing:"0.12em",padding:"3px 9px",borderRadius:20,background:s.bg,border:`1px solid ${s.bd}`,color:s.color}}>{s.label}</span>;
}
function StatPill({label,value,color="blue"}){
  const map={blue:{bg:C.accentL,bd:C.accentB,color:C.accentD},green:{bg:C.greenL,bd:C.greenB,color:C.greenD},amber:{bg:C.amberL,bd:C.amberB,color:C.amber},red:{bg:C.redL,bd:C.redB,color:C.red}};
  const s=map[color];
  return <span style={{fontFamily:C.mono,fontSize:10,fontWeight:600,background:s.bg,border:`1px solid ${s.bd}`,color:s.color,borderRadius:8,padding:"3px 9px"}}>{label}: <strong>{value}</strong></span>;
}
function StepResult({title,data}){
  if(!data)return null;
  const isOk=data.status==="ok"||data.status==="warn";
  return(
    <div style={{background:isOk?C.greenL:C.redL,border:`1.5px solid ${isOk?C.greenB:C.redB}`,borderRadius:10,padding:"11px 15px",display:"flex",flexDirection:"column",gap:7}}>
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between"}}><span style={{fontFamily:C.title,fontSize:12,fontWeight:700,color:C.ink}}>{title}</span><StatusBadge status={data.status==="warn"?"warning":data.status}/></div>
      {data.error&&<p style={{fontFamily:C.mono,fontSize:11,color:C.red,margin:0}}>{data.error}</p>}
      <div style={{display:"flex",flexWrap:"wrap",gap:5}}>
        {data.customers&&<><StatPill label="Customers +" value={fmt(data.customers.created)} color="green"/><StatPill label="Updated" value={fmt(data.customers.updated)} color="blue"/>{data.customers.failed>0&&<StatPill label="Failed" value={fmt(data.customers.failed)} color="red"/>}</>}
        {data.suppliers&&<><StatPill label="Suppliers +" value={fmt(data.suppliers.created)} color="green"/>{data.suppliers.failed>0&&<StatPill label="Failed" value={fmt(data.suppliers.failed)} color="red"/>}</>}
        {data.sales&&<><StatPill label="Sales +" value={fmt(data.sales.created)} color="green"/>{data.sales.failed>0&&<StatPill label="Failed" value={fmt(data.sales.failed)} color="red"/>}</>}
        {data.purchase&&<><StatPill label="Purchase +" value={fmt(data.purchase.created)} color="green"/>{data.purchase.failed>0&&<StatPill label="Failed" value={fmt(data.purchase.failed)} color="red"/>}</>}
        {data.created!==undefined&&!data.customers&&!data.sales&&<><StatPill label="Created" value={fmt(data.created)} color="green"/><StatPill label="Updated" value={fmt(data.updated)} color="blue"/>{data.failed>0&&<StatPill label="Failed" value={fmt(data.failed)} color="red"/>}</>}
        {data.journalEntries&&<><StatPill label="JE Created" value={fmt(data.journalEntries.created)} color="green"/>{data.journalEntries.failed>0&&<StatPill label="JE Failed" value={fmt(data.journalEntries.failed)} color="red"/>}</>}
        {data.accounts!==undefined&&<StatPill label="Accounts" value={fmt(data.accounts)} color="blue"/>}
        {data.skipped!==undefined&&<StatPill label="Skipped" value={fmt(data.skipped)} color="amber"/>}
      </div>
    </div>
  );
}

// ── Up To Date Banner ─────────────────────────────────────────────────────────
function UpToDateBanner({finishedAt,mode="manual"}){
  return(
    <div style={{background:C.card,border:`2px solid ${C.tealB}`,borderRadius:14,padding:"18px 20px",display:"flex",flexDirection:"column",gap:11,boxShadow:`0 4px 20px ${C.teal}18`,animation:"se-pop .25s ease"}}>
      <div style={{display:"flex",alignItems:"center",gap:12}}>
        <div style={{width:40,height:40,borderRadius:11,background:C.tealL,border:`2px solid ${C.tealB}`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:20,flexShrink:0}}>✓</div>
        <div style={{flex:1}}>
          <p style={{fontFamily:C.title,fontSize:14,fontWeight:800,color:C.ink,margin:0,letterSpacing:"-0.3px"}}>Already Up to Date</p>
          <p style={{fontFamily:C.mono,fontSize:10,color:C.teal,margin:"3px 0 0"}}>
            {mode==="auto"?"Auto-sync checked — no new or changed data found":"No new or changed data since the last sync"}
          </p>
        </div>
        <StatusBadge status="uptodate"/>
      </div>
      <div style={{padding:"10px 14px",borderRadius:9,background:C.tealL,border:`1.5px solid ${C.tealB}`}}>
        <p style={{fontFamily:C.mono,fontSize:10,color:C.teal,margin:0,lineHeight:1.7}}>
          All masters have the same ALTERID as the last sync and the voucher date window is already covered.
          <strong> Nothing was pushed to ERPNext.</strong>
        </p>
      </div>
      {finishedAt&&<p style={{fontFamily:C.mono,fontSize:10,color:C.muted,margin:0}}>Checked at {new Date(finishedAt).toLocaleTimeString("en-IN")}</p>}
    </div>
  );
}

function CountdownRing({remainingMs,totalMs}){
  const pct=totalMs>0?Math.max(0,remainingMs/totalMs):0;
  const r=20,circ=2*Math.PI*r;
  return(
    <svg width={50} height={50} style={{flexShrink:0}}>
      <circle cx={25} cy={25} r={r} fill="none" stroke={C.border} strokeWidth={3}/>
      <circle cx={25} cy={25} r={r} fill="none" stroke={C.accent} strokeWidth={3} strokeDasharray={circ} strokeDashoffset={circ*(1-pct)} strokeLinecap="round" transform="rotate(-90 25 25)" style={{transition:"stroke-dashoffset 1s linear"}}/>
      <text x={25} y={30} textAnchor="middle" style={{fontFamily:C.mono,fontSize:9,fill:C.accent,fontWeight:700}}>{fmtTime(remainingMs)}</text>
    </svg>
  );
}
function JobProgressBanner({jobId,type}){
  const [elapsed,setElapsed]=useState(0);
  useEffect(()=>{const t=setInterval(()=>setElapsed(s=>s+1),1000);return()=>clearInterval(t);},[]);
  return(
    <div style={{background:C.accentL,border:`1.5px solid ${C.accentB}`,borderRadius:13,padding:"15px 18px",display:"flex",alignItems:"center",gap:14,animation:"se-fade .2s ease"}}>
      <div style={{width:40,height:40,borderRadius:10,flexShrink:0,background:C.card,border:`1.5px solid ${C.accentB}`,display:"flex",alignItems:"center",justifyContent:"center"}}><Spinner size={16}/></div>
      <div style={{flex:1}}>
        <p style={{fontFamily:C.title,fontSize:13,fontWeight:700,color:C.accentD,margin:0}}>Syncing <span style={{textTransform:"capitalize"}}>{type}</span> to ERPNext…</p>
        <p style={{fontFamily:C.mono,fontSize:10,color:C.muted,margin:"3px 0 0"}}>Running in background · {fmtTime(elapsed*1000)} elapsed · Job: {jobId}</p>
        <p style={{fontFamily:C.mono,fontSize:10,color:C.muted,margin:"2px 0 0"}}>Watch progress in the <strong>Live Logs</strong> tab ↗</p>
      </div>
    </div>
  );
}
function ErpCredentialsPanel({company,onSaved}){
  const all=loadAllCreds(),saved=all[company]||{};
  const [url,setUrl]=useState(saved.url||"");
  const [key,setKey]=useState(saved.apiKey||"");
  const [secret,setSecret]=useState(saved.apiSecret||"");
  const [show,setShow]=useState(false);
  const [saved2,setSaved2]=useState(false);
  useEffect(()=>{const c=loadAllCreds()[company]||{};setUrl(c.url||"");setKey(c.apiKey||"");setSecret(c.apiSecret||"");setSaved2(false);},[company]);
  function handleSave(){const all2=loadAllCreds();all2[company]={url:url.trim(),apiKey:key.trim(),apiSecret:secret.trim()};saveAllCreds(all2);setSaved2(true);onSaved&&onSaved(all2[company]);setTimeout(()=>setSaved2(false),2000);}
  const hasCreds=!!(saved.url&&saved.apiKey&&saved.apiSecret);
  return(
    <div style={{background:C.card,border:`1.5px solid ${hasCreds?C.greenB:C.amberB}`,borderRadius:11,overflow:"hidden",marginBottom:14}}>
      <button onClick={()=>setShow(s=>!s)} style={{width:"100%",display:"flex",alignItems:"center",gap:11,padding:"12px 15px",background:"none",border:"none",cursor:"pointer",textAlign:"left"}}>
        <div style={{width:32,height:32,borderRadius:8,flexShrink:0,background:hasCreds?C.greenL:C.amberL,border:`1.5px solid ${hasCreds?C.greenB:C.amberB}`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:15}}>{hasCreds?"🔐":"⚙️"}</div>
        <div style={{flex:1}}>
          <p style={{fontFamily:C.title,fontSize:12,fontWeight:700,color:C.ink,margin:0}}>ERPNext Credentials{company&&<span style={{fontFamily:C.mono,fontSize:10,color:C.muted,marginLeft:8,fontWeight:400}}>for {company.slice(0,28)}{company.length>28?"…":""}</span>}</p>
          <p style={{fontFamily:C.mono,fontSize:10,color:hasCreds?C.green:C.amber,margin:"2px 0 0"}}>{hasCreds?`✓ Saved — ${saved.url}`:"⚠ No credentials saved for this company"}</p>
        </div>
        <span style={{fontFamily:C.mono,fontSize:10,color:C.dim,transform:show?"rotate(90deg)":"none",transition:"transform .2s",flexShrink:0}}>▶</span>
      </button>
      {show&&(
        <div style={{padding:"0 15px 15px",display:"flex",flexDirection:"column",gap:11,borderTop:`1px solid ${C.border}`,background:C.surface}}>
          <p style={{fontFamily:C.mono,fontSize:10,color:C.muted,paddingTop:11,margin:0,lineHeight:1.6}}>Credentials are saved locally per Tally company and override .env values for this session.</p>
          <div>
            <label style={{display:"block",fontFamily:C.mono,fontSize:9,color:C.muted,textTransform:"uppercase",letterSpacing:"0.12em",marginBottom:5,fontWeight:700}}>ERPNext URL</label>
            <input value={url} onChange={e=>setUrl(e.target.value)} placeholder="https://yoursite.frappe.cloud" style={inp({fontFamily:C.mono,fontSize:12})} onFocus={onFocus} onBlur={onBlur}/>
          </div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10}}>
            {[["API Key",key,setKey,"api_key","text"],["API Secret",secret,setSecret,"api_secret","password"]].map(([lbl,val,setter,ph,type])=>(
              <div key={lbl}>
                <label style={{display:"block",fontFamily:C.mono,fontSize:9,color:C.muted,textTransform:"uppercase",letterSpacing:"0.12em",marginBottom:5,fontWeight:700}}>{lbl}</label>
                <input value={val} onChange={e=>setter(e.target.value)} type={type} placeholder={ph} style={inp({fontFamily:C.mono,fontSize:12})} onFocus={onFocus} onBlur={onBlur}/>
              </div>
            ))}
          </div>
          <button onClick={handleSave} disabled={!url||!key||!secret} style={{padding:"10px 18px",borderRadius:9,border:"none",background:saved2?C.green:!url||!key||!secret?C.surface:C.accentD,color:!url||!key||!secret?C.dim:"#fff",fontFamily:C.title,fontSize:12,fontWeight:700,cursor:!url||!key||!secret?"not-allowed":"pointer",transition:"background .2s"}}>{saved2?"✓ Saved!":"Save Credentials"}</button>
        </div>
      )}
    </div>
  );
}
function SectionHead({step,title,done,badge}){
  return(
    <div style={{display:"flex",alignItems:"center",gap:11,marginBottom:16,paddingBottom:13,borderBottom:`1px solid ${C.border}`}}>
      <div style={{width:25,height:25,borderRadius:7,flexShrink:0,background:done?C.green:`linear-gradient(135deg,${C.accent},${C.accentD})`,display:"flex",alignItems:"center",justifyContent:"center",boxShadow:done?`0 2px 8px ${C.green}44`:`0 2px 8px ${C.accent}44`}}>
        <span style={{fontFamily:C.mono,fontSize:10,fontWeight:800,color:"#fff"}}>{done?"✓":step}</span>
      </div>
      <h2 style={{fontFamily:C.title,fontWeight:800,fontSize:13.5,color:C.ink,flex:1,letterSpacing:"-0.3px",margin:0}}>{title}</h2>
      {badge}
    </div>
  );
}

// ── Sync Option Checkboxes — reusable for both manual and auto ────────────────
function SyncCheckboxGrid({opts,onToggle,onIndividualSync,busy,co,loadingItem,mode="manual"}){
  const selectedCount=Object.values(opts).filter(Boolean).length;
  const accentColor=mode==="auto"?C.teal:C.accent;
  const accentLColor=mode==="auto"?C.tealL:C.accentL;
  const accentBColor=mode==="auto"?C.tealB:C.accentB;
  const accentDColor=mode==="auto"?C.teal:C.accentD;
  return(
    <>
      <div style={{display:"flex",gap:7,marginBottom:11,alignItems:"center"}}>
        <span style={{fontFamily:C.mono,fontSize:10,color:C.muted,flex:1}}>
          {selectedCount}/{SYNC_OPTIONS.length} selected
        </span>
        <button
          onClick={()=>SYNC_OPTIONS.forEach(o=>onToggle(o.key,true))}
          style={{padding:"4px 12px",borderRadius:7,border:`1.5px solid ${accentBColor}`,background:accentLColor,color:accentDColor,fontFamily:C.mono,fontSize:10,fontWeight:700,cursor:"pointer",transition:"all .15s"}}
        >☑ Select All</button>
        <button
          onClick={()=>SYNC_OPTIONS.forEach(o=>onToggle(o.key,false))}
          style={{padding:"4px 12px",borderRadius:7,border:`1.5px solid ${C.border}`,background:C.surface,color:C.muted,fontFamily:C.mono,fontSize:10,fontWeight:600,cursor:"pointer",transition:"all .15s"}}
        >☐ Clear</button>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:7}}>
        {SYNC_OPTIONS.map(opt=>(
          <label key={opt.key} className="se-opt" style={{display:"flex",alignItems:"center",gap:9,cursor:"pointer",padding:"10px 13px",borderRadius:10,background:opts[opt.key]?accentLColor:C.surface,border:`1.5px solid ${opts[opt.key]?accentBColor:C.border}`,transition:"all .15s",userSelect:"none",boxShadow:opts[opt.key]?`0 2px 8px ${accentColor}18`:"none"}}>
            <input type="checkbox" checked={opts[opt.key]} onChange={()=>onToggle(opt.key)} style={{accentColor:accentColor,width:14,height:14,flexShrink:0}}/>
            <div style={{width:30,height:30,borderRadius:7,flexShrink:0,background:opts[opt.key]?C.card:`${C.border}88`,border:`1px solid ${opts[opt.key]?accentBColor:C.border}`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:14,transition:"all .15s"}}>{opt.icon}</div>
            <div style={{minWidth:0,flex:1}}>
              <p style={{fontFamily:C.title,fontSize:11.5,fontWeight:700,color:opts[opt.key]?accentDColor:C.ink,margin:0,letterSpacing:"-0.2px"}}>{opt.label}</p>
              <p style={{fontFamily:C.mono,fontSize:9,color:C.muted,margin:"1px 0 0"}}>{opt.sub}</p>
            </div>
            {/* Individual sync button only shown in manual mode */}
            {mode==="manual"&&onIndividualSync&&(
              <button onClick={e=>{e.preventDefault();e.stopPropagation();onIndividualSync(opt.individual);}} disabled={busy||!co} title={`Sync ${opt.label} only`}
                style={{padding:"4px 9px",borderRadius:6,border:`1px solid ${C.accentB}`,background:C.card,color:C.accentD,fontFamily:C.mono,fontSize:9,fontWeight:700,cursor:busy||!co?"not-allowed":"pointer",flexShrink:0,opacity:busy||!co?0.4:1,transition:"all .15s"}}>
                {loadingItem===opt.individual?<Spinner size={9}/>:"↑"}
              </button>
            )}
          </label>
        ))}
      </div>
    </>
  );
}

export function SyncToErpNext({companies}){
  const [company,setCompany]=useState(()=>{const last=localStorage.getItem("last_tally_company");const found=companies?.find(c=>c.name===last);return found?found.name:(companies?.[0]?.name||"");});
  const [fromDate,setFromDate]=useState(YEAR_START);
  const [toDate,setToDate]=useState(TODAY);
  const [erpPing,setErpPing]=useState(null);
  const [pinging,setPinging]=useState(false);
  const [erpCompany,setErpCompany]=useState(()=>{const initCo=companies?.[0]?.name||"";const allMappings=JSON.parse(localStorage.getItem("erp_company_map")||"{}");return allMappings[initCo]||"";});
  const [result,setResult]=useState(null);
  const [loading,setLoading]=useState(null);
  const [activeJob,setActiveJob]=useState(null);
  const [cancelling,setCancelling]=useState(false);
  const pollRef=useRef(null);

  // ── Manual sync options ───────────────────────────────────────────────────
  const [syncOpts,setSyncOpts]=useState({...EMPTY_OPTS});
  const toggleOpt=(key,forceTo)=>setSyncOpts(o=>({...o,[key]:forceTo!==undefined?forceTo:!o[key]}));

  // ── Auto sync options — SEPARATE from manual, persisted to localStorage ───
  const [autoSyncOpts,setAutoSyncOpts]=useState(()=>{
    try{
      const saved=JSON.parse(localStorage.getItem(AUTO_SYNC_OPTS_KEY)||"null");
      if(saved)return saved;
    }catch{}
    // Sensible defaults for first time
    return {...EMPTY_OPTS,syncLedgers:true,syncStock:true,syncVouchers:true};
  });
  const toggleAutoOpt=(key,forceTo)=>{
    setAutoSyncOpts(o=>{
      const next={...o,[key]:forceTo!==undefined?forceTo:!o[key]};
      localStorage.setItem(AUTO_SYNC_OPTS_KEY,JSON.stringify(next));
      return next;
    });
  };

  const [lastSyncDates,setLastSyncDates]=useState(()=>{try{return JSON.parse(localStorage.getItem("last_sync_dates")||"{}");}catch{return {};}});
  function saveLastSyncDate(co,date){const updated={...lastSyncDates,[co]:date};setLastSyncDates(updated);localStorage.setItem("last_sync_dates",JSON.stringify(updated));}

  const [autoMode,setAutoMode]=useState(false);
  const [autoInterval,setAutoInterval]=useState(INTERVALS[2].value);
  const [autoRunning,setAutoRunning]=useState(false);
  const [autoNextRun,setAutoNextRun]=useState(null);
  const [autoRemainingMs,setAutoRemainingMs]=useState(0);
  const [autoHistory,setAutoHistory]=useState([]);
  const [autoRunCount,setAutoRunCount]=useState(0);
  const [autoSyncing,setAutoSyncing]=useState(false);
  // Last auto-sync result for "up to date" display in auto panel
  const [lastAutoResult,setLastAutoResult]=useState(null);
  const timerRef=useRef(null);const countdownRef=useRef(null);
  const [companyCreds,setCompanyCreds]=useState({});

  useEffect(()=>{if(!company)return;const saved=loadAllCreds()[company]||{};setCompanyCreds(saved);const allMappings=JSON.parse(localStorage.getItem("erp_company_map")||"{}");setErpCompany(allMappings[company]||"");},[company]);
  useEffect(()=>()=>clearTimeout(pollRef.current),[]);

  // Restore active job on page load
  useEffect(()=>{
    const stored=sessionStorage.getItem(ACTIVE_JOB_KEY);if(!stored)return;
    try{const job=JSON.parse(stored);if(!job?.jobId||!job?.type){sessionStorage.removeItem(ACTIVE_JOB_KEY);return;}
      fetch(`${BASE_URL}/sync/status/${job.jobId}`).then(r=>{
        if(!r.ok){sessionStorage.removeItem(ACTIVE_JOB_KEY);return null;}
        return r.json();
      }).then(data=>{
        if(!data)return;
        if(data?.job?.status==="running"){setActiveJob(job);setLoading(job.type);pollUntilDone(job.jobId,job.type).then(polled=>{setActiveJob(null);setLoading(null);sessionStorage.removeItem(ACTIVE_JOB_KEY);setResult(polled);});}
        else{sessionStorage.removeItem(ACTIVE_JOB_KEY);}
      }).catch(()=>sessionStorage.removeItem(ACTIVE_JOB_KEY));
    }catch{sessionStorage.removeItem(ACTIVE_JOB_KEY);}
  },[]); // eslint-disable-line

  useEffect(()=>{
    if(sessionStorage.getItem(ACTIVE_JOB_KEY))return;
    fetch(`${BASE_URL}/sync/jobs`).then(r=>r.json()).then(data=>{
      const running=data?.jobs?.[0];if(!running)return;
      const job={jobId:running.id,type:running.type};setActiveJob(job);setLoading(job.type);
      sessionStorage.setItem(ACTIVE_JOB_KEY,JSON.stringify(job));
      pollUntilDone(job.jobId,job.type).then(polled=>{setActiveJob(null);setLoading(null);sessionStorage.removeItem(ACTIVE_JOB_KEY);setResult(polled);});
    }).catch(()=>{});
  },[]); // eslint-disable-line

  async function cancelActiveJob(){
    if(!activeJob?.jobId)return;setCancelling(true);
    try{await fetch(`${BASE_URL}/sync/cancel/${activeJob.jobId}`,{method:"POST"});clearTimeout(pollRef.current);setActiveJob(null);setLoading(null);sessionStorage.removeItem(ACTIVE_JOB_KEY);setResult({type:activeJob.type,error:"Sync was stopped by you."});}
    catch(e){setResult({type:activeJob?.type,error:"Failed to stop: "+e.message});}
    finally{setCancelling(false);}
  }

  function credsOverride(){if(companyCreds.url&&companyCreds.apiKey&&companyCreds.apiSecret){return{erpnextUrl:companyCreds.url,erpnextApiKey:companyCreds.apiKey,erpnextApiSecret:companyCreds.apiSecret};}return{};}

  async function pollUntilDone(jobId,type){
    const POLL_MS=3000,MAX_MS=4*60*60*1000,deadline=Date.now()+MAX_MS;
    return new Promise(resolve=>{
      function tick(){
        if(Date.now()>deadline){resolve({type,error:"Timed out waiting for sync job"});return;}
        pollRef.current=setTimeout(async()=>{
          try{
            const res=await fetch(`${BASE_URL}/sync/status/${jobId}`);
            if(res.status===404){sessionStorage.removeItem(ACTIVE_JOB_KEY);resolve({type,data:{ok:true,result:{nothingToSync:false,serverRestarted:true,note:"Server restarted while syncing — the sync likely completed. Please check ERPNext to confirm."}}});return;}
            const data=await res.json();const job=data.job;
            if(!job){sessionStorage.removeItem(ACTIVE_JOB_KEY);resolve({type,data:{ok:true,result:{note:"Job completed (no longer tracked by server)"}}});return;}
            if(job.status==="done"){resolve({type,data:{ok:true,result:job.result}});return;}
            if(job.status==="failed"){resolve({type,error:job.error||"Sync failed"});return;}
            if(job.status==="cancelled"){sessionStorage.removeItem(ACTIVE_JOB_KEY);resolve({type,error:job.error||"Sync was stopped by you."});return;}
            tick();}catch(_){tick();}
        },POLL_MS);
      }tick();
    });
  }

  const runSync=useCallback(async(type)=>{
    const co=company.trim();if(!co)return;
    setLoading(type);setResult(null);setActiveJob(null);
    try{
      const creds=credsOverride();const erpCoName=erpCompany&&erpCompany.trim();
      if(!erpCoName){setResult({type,error:"Please enter the ERPNext Company Name before syncing."});setLoading(null);return;}
      const syncCreds={...creds,erpnextCompany:erpCoName};let apiRes;
      if(type==="full"){
        apiRes=await tallyAPI.syncFull(co,fromDate,toDate,{
          ...syncCreds,
          syncChartOfAccounts: syncOpts.syncChartOfAccounts,
          syncLedgers:         syncOpts.syncLedgers,
          syncSmartLedgers:    syncOpts.syncSmartLedgers,
          syncOpeningBalances: syncOpts.syncOpeningBalances,
          syncGodowns:         syncOpts.syncGodowns,
          syncCostCentres:     syncOpts.syncCostCentres,
          syncStock:           syncOpts.syncStock,
          syncVouchers:        syncOpts.syncVouchers,
          syncInvoices:        syncOpts.syncInvoices,
        });
        saveLastSyncDate(co,toDate);
      }
      else if(type==="chart-of-accounts")apiRes=await tallyAPI.syncChartOfAccounts(co,syncCreds);
      else if(type==="ledgers")apiRes=await tallyAPI.syncLedgers(co,syncCreds);
      else if(type==="smart-ledgers")apiRes=await tallyAPI.syncSmartLedgers(co,fromDate,toDate,syncCreds);
      else if(type==="stock")apiRes=await tallyAPI.syncStock(co,syncCreds);
      else if(type==="vouchers")apiRes=await tallyAPI.syncVouchers(co,fromDate,toDate,syncCreds);
      else if(type==="godowns")apiRes=await tallyAPI.syncGodowns(co,syncCreds);
      else if(type==="opening-balances")apiRes=await tallyAPI.syncOpeningBalances(co,syncCreds);
      else if(type==="cost-centres")apiRes=await tallyAPI.syncCostCentres(co,syncCreds);
      else if(type==="invoices")apiRes=await tallyAPI.syncInvoices(co,fromDate,toDate,syncCreds);
      if(apiRes?.jobId){const job={jobId:apiRes.jobId,type};setActiveJob(job);sessionStorage.setItem(ACTIVE_JOB_KEY,JSON.stringify(job));const polled=await pollUntilDone(apiRes.jobId,type);setActiveJob(null);sessionStorage.removeItem(ACTIVE_JOB_KEY);setResult(polled);}
      else{setResult({type,data:apiRes});}
    }catch(e){setActiveJob(null);sessionStorage.removeItem(ACTIVE_JOB_KEY);setResult({type,error:e.message});}
    finally{setLoading(null);}
  },[company,fromDate,toDate,syncOpts,companyCreds,erpCompany]); // eslint-disable-line

  // ── Auto sync — uses autoSyncOpts, not syncOpts ───────────────────────────
  const runAutoSync=useCallback(async()=>{
    const co=company.trim();if(!co||autoSyncing)return;setAutoSyncing(true);
    setLastAutoResult(null);
    const started=new Date();
    try{
      const creds=credsOverride();
      const erpCoName=erpCompany&&erpCompany.trim();
      const todayStr=new Date().toISOString().slice(0,10);

      let autoFromDate=getFYStart();
      const lastSynced=lastSyncDates[co];
      if(lastSynced){
        const d=new Date(lastSynced);
        d.setDate(d.getDate()-3);
        autoFromDate=d.toISOString().slice(0,10);
      }

      // Use the user's chosen auto-sync options (autoSyncOpts), not manual syncOpts
      const hasSelection=Object.values(autoSyncOpts).some(Boolean);
      if(!hasSelection){
        setLastAutoResult({nothingSelected:true});
        setAutoHistory(h=>[{at:started,status:"warning",error:"No data types selected for auto-sync"},...h].slice(0,8));
        return;
      }

      const apiRes=await tallyAPI.syncFull(co,autoFromDate,todayStr,{
        ...creds,
        erpnextCompany: erpCoName,
        syncChartOfAccounts: autoSyncOpts.syncChartOfAccounts,
        syncLedgers:         autoSyncOpts.syncLedgers,
        syncSmartLedgers:    autoSyncOpts.syncSmartLedgers,
        syncOpeningBalances: autoSyncOpts.syncOpeningBalances,
        syncGodowns:         autoSyncOpts.syncGodowns,
        syncCostCentres:     autoSyncOpts.syncCostCentres,
        syncStock:           autoSyncOpts.syncStock,
        syncVouchers:        autoSyncOpts.syncVouchers,
        syncInvoices:        autoSyncOpts.syncInvoices,
      });

      let finalStatus="ok";
      let isUpToDate=false;

      if(apiRes?.jobId){
        const polled=await pollUntilDone(apiRes.jobId,"full");
        finalStatus=polled.error?"failed":(polled.data?.result?.status||"ok");
        isUpToDate=!polled.error&&!!polled.data?.result?.nothingToSync;
        setLastAutoResult({upToDate:isUpToDate,error:polled.error,result:polled.data?.result});
        if(finalStatus!=="failed"&&!isUpToDate) saveLastSyncDate(co,todayStr);
        setAutoRunCount(c=>c+1);
        setAutoHistory(h=>[{at:started,status:isUpToDate?"uptodate":finalStatus,error:polled.error,from:autoFromDate,to:todayStr,upToDate:isUpToDate},...h].slice(0,8));
      }else{
        isUpToDate=!!apiRes?.result?.nothingToSync;
        setLastAutoResult({upToDate:isUpToDate,result:apiRes?.result});
        setAutoRunCount(c=>c+1);
        setAutoHistory(h=>[{at:started,status:isUpToDate?"uptodate":apiRes?.result?.status||"ok",from:autoFromDate,to:todayStr,upToDate:isUpToDate},...h].slice(0,8));
      }
    }catch(e){
      setLastAutoResult({error:e.message});
      setAutoHistory(h=>[{at:started,status:"failed",error:e.message},...h].slice(0,8));
    }
    finally{setAutoSyncing(false);}
  },[company,autoSyncOpts,autoSyncing,companyCreds,erpCompany,lastSyncDates]); // eslint-disable-line

  useEffect(()=>{
    if(!autoRunning){clearInterval(timerRef.current);clearInterval(countdownRef.current);setAutoNextRun(null);setAutoRemainingMs(0);return;}
    const scheduleNext=()=>{setAutoNextRun(new Date(Date.now()+autoInterval));setAutoRemainingMs(autoInterval);};
    runAutoSync();scheduleNext();
    timerRef.current=setInterval(()=>{runAutoSync();scheduleNext();},autoInterval);
    countdownRef.current=setInterval(()=>setAutoRemainingMs(p=>Math.max(0,p-1000)),1000);
    return()=>{clearInterval(timerRef.current);clearInterval(countdownRef.current);};
  },[autoRunning,autoInterval]); // eslint-disable-line

  const busy=!!loading,co=company.trim(),noSync=!Object.values(syncOpts).some(Boolean);
  const noAutoSync=!Object.values(autoSyncOpts).some(Boolean);
  const hasCreds=!!(companyCreds.url&&companyCreds.apiKey&&companyCreds.apiSecret);
  const isReady=co&&erpCompany&&!noSync;
  const dayCount=fromDate&&toDate?Math.round((new Date(toDate)-new Date(fromDate))/(1000*60*60*24)):0;
  const chunkCount=Math.ceil(dayCount/15);

  const card={background:C.card,border:`1.5px solid ${C.border}`,borderRadius:14,padding:20,boxShadow:"0 4px 24px rgba(13,21,50,.08), 0 1px 0 rgba(255,255,255,.9) inset"};

  return(
    <div style={{display:"flex",flexDirection:"column",gap:14,fontFamily:C.sans}}>
      <style>{`
        @keyframes se-spin{to{transform:rotate(360deg)}}
        @keyframes se-fade{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
        @keyframes se-pop{0%{transform:scale(.96);opacity:0}100%{transform:scale(1);opacity:1}}
        @keyframes se-pulse{0%,100%{opacity:1}50%{opacity:.3}}
        .se-opt:hover{box-shadow:0 2px 10px rgba(0,0,0,.07)!important}
        .se-btn:hover:not(:disabled){transform:translateY(-1px);box-shadow:0 8px 24px rgba(37,99,235,.35)!important}
        .se-btn:active:not(:disabled){transform:translateY(0)}
      `}</style>

      {/* Floating stop */}
      {(loading||activeJob)&&(
        <div style={{position:"fixed",bottom:24,right:24,zIndex:9999,display:"flex",flexDirection:"column",alignItems:"flex-end",gap:9,animation:"se-pop .2s ease"}}>
          <div style={{background:C.card,border:`1.5px solid ${C.accentB}`,borderRadius:12,padding:"10px 16px",display:"flex",alignItems:"center",gap:9,boxShadow:"0 8px 32px rgba(0,0,0,.12)"}}>
            <Spinner size={12}/><span style={{fontFamily:C.mono,fontSize:11,color:C.muted}}>{activeJob?`Syncing ${activeJob.type}…`:`Syncing ${loading}…`}</span>
          </div>
          <button
            onClick={activeJob?cancelActiveJob:()=>{setLoading(null);sessionStorage.removeItem(ACTIVE_JOB_KEY);setResult({type:loading,error:"Sync stopped by you."});}}
            disabled={cancelling}
            style={{display:"flex",alignItems:"center",gap:7,padding:"12px 22px",borderRadius:11,border:"none",background:cancelling?C.surface:C.red,color:cancelling?C.muted:"#fff",fontFamily:C.title,fontSize:13,fontWeight:800,cursor:cancelling?"not-allowed":"pointer",boxShadow:cancelling?"none":"0 6px 20px rgba(220,38,38,.35)",transition:"all .15s"}}
          >
            {cancelling?<Spinner size={12} color={C.muted}/>:<span>■</span>}{cancelling?"Stopping…":"Stop Sync"}
          </button>
        </div>
      )}

      {/* Step 1 — ERPNext Setup */}
      <div style={card}>
        <SectionHead step="1" title="ERPNext Setup" done={hasCreds&&!!erpCompany}/>
        {co&&<ErpCredentialsPanel company={co} onSaved={creds=>setCompanyCreds(creds)}/>}
        <div style={{marginBottom:14}}>
          <label style={{display:"block",fontFamily:C.mono,fontSize:9,color:C.muted,letterSpacing:"0.14em",textTransform:"uppercase",marginBottom:6,fontWeight:700}}>ERPNext Company Name <span style={{color:C.amber}}>— must match exactly</span></label>
          <div style={{display:"flex",gap:9}}>
            <input value={erpCompany||""} onChange={e=>setErpCompany(e.target.value)} placeholder="e.g. Test Company" style={{...inp({flex:1}),borderColor:erpCompany?C.greenB:C.amberB,background:erpCompany?C.greenL:C.amberL}} onFocus={onFocus} onBlur={onBlur}/>
            <button onClick={()=>{const all=JSON.parse(localStorage.getItem("erp_company_map")||"{}");all[company]=erpCompany;localStorage.setItem("erp_company_map",JSON.stringify(all));}} disabled={!erpCompany} style={{padding:"9px 15px",borderRadius:9,border:"none",background:erpCompany?C.accentD:C.surface,color:erpCompany?"#fff":C.dim,fontFamily:C.title,fontSize:11,fontWeight:700,cursor:erpCompany?"pointer":"not-allowed",flexShrink:0,transition:"all .15s"}}>Save</button>
          </div>
          {erpCompany&&<p style={{fontFamily:C.mono,fontSize:10,color:C.green,margin:"5px 0 0"}}>✓ Will sync to: <strong>{erpCompany}</strong></p>}
        </div>
        <div style={{display:"flex",alignItems:"center",gap:9}}>
          <button onClick={async()=>{setPinging(true);setErpPing(null);try{const creds=credsOverride();if(creds.erpnextUrl){const res=await fetch(`${BASE_URL}/erpnext/ping`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(creds)});let _d;try{_d=await res.json();}catch(_){_d={connected:false,error:"Backend returned non-JSON (status "+res.status+")"};} setErpPing(_d);}else{setErpPing(await tallyAPI.erpnextPing());}}catch(e){const m=e.message||"Unknown";setErpPing({connected:false,error:m.includes("fetch")?`Cannot reach backend at ${BASE_URL} — is the server running on port 4000?`:m});}finally{setPinging(false);}}} disabled={pinging}
            style={{display:"flex",alignItems:"center",gap:6,padding:"8px 16px",borderRadius:9,background:C.accentL,border:`1.5px solid ${C.accentB}`,color:C.accentD,fontFamily:C.mono,fontSize:11,fontWeight:600,cursor:pinging?"not-allowed":"pointer",opacity:pinging?0.65:1,transition:"all .15s",flexShrink:0}}>
            {pinging&&<Spinner size={10}/>}{pinging?"Testing…":"🔌 Test Connection"}
          </button>
          {erpPing&&(
            <div style={{display:"flex",alignItems:"center",gap:7,padding:"8px 13px",borderRadius:9,flex:1,background:erpPing.connected?C.greenL:C.redL,border:`1.5px solid ${erpPing.connected?C.greenB:C.redB}`,animation:"se-fade .2s ease"}}>
              <span style={{fontSize:13}}>{erpPing.connected?"✓":"✗"}</span>
              <span style={{fontFamily:C.mono,fontSize:11,color:erpPing.connected?C.green:C.red}}>{erpPing.connected?`Connected as ${erpPing.user} · ${erpPing.latencyMs}ms`:erpPing.error}</span>
            </div>
          )}
        </div>
      </div>

      {/* Step 2 — Company & Dates */}
      <div style={card}>
        <SectionHead step="2" title="Tally Company & Date Range" done={!!co}/>
        <div style={{marginBottom:13}}>
          <label style={{display:"block",fontFamily:C.mono,fontSize:9,color:C.muted,letterSpacing:"0.14em",textTransform:"uppercase",marginBottom:6,fontWeight:700}}>Tally Company</label>
          {companies?.length>0?(
            <select value={company} onChange={e=>{setCompany(e.target.value);localStorage.setItem("last_tally_company",e.target.value);}} style={{...inp(),cursor:"pointer",appearance:"none"}} onFocus={onFocus} onBlur={onBlur}>
              <option value="">— select company —</option>
              {companies.map(c=><option key={c.guid||c.name} value={c.name}>{c.name}</option>)}
            </select>
          ):<input value={company} onChange={e=>setCompany(e.target.value)} placeholder="Company name" style={inp()} onFocus={onFocus} onBlur={onBlur}/>}
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
          {[["From Date",fromDate,setFromDate],["To Date",toDate,setToDate]].map(([lbl,v,setter])=>(
            <div key={lbl}>
              <label style={{display:"block",fontFamily:C.mono,fontSize:9,color:C.muted,letterSpacing:"0.14em",textTransform:"uppercase",marginBottom:6,fontWeight:700}}>{lbl}</label>
              <input type="date" value={v} onChange={e=>setter(e.target.value)} style={{...inp(),fontFamily:C.mono,fontSize:12}} onFocus={onFocus} onBlur={onBlur}/>
            </div>
          ))}
        </div>
        {lastSyncDates[company]&&(
          <div style={{marginTop:10,padding:"8px 12px",borderRadius:8,background:C.greenL,border:`1px solid ${C.greenB}`,display:"flex",gap:8,alignItems:"center"}}>
            <span style={{fontSize:11}}>✓</span>
            <p style={{fontFamily:C.mono,fontSize:10,color:C.green,margin:0}}>
              Last synced up to <strong>{lastSyncDates[company]}</strong> — next auto sync will start from <strong>{(()=>{const d=new Date(lastSyncDates[company]);d.setDate(d.getDate()-3);return d.toISOString().slice(0,10);})()}</strong> (3-day overlap)
            </p>
            <button onClick={()=>{const u={...lastSyncDates};delete u[company];setLastSyncDates(u);localStorage.setItem("last_sync_dates",JSON.stringify(u));}} title="Reset — force full re-sync next time" style={{marginLeft:"auto",padding:"2px 8px",borderRadius:5,border:`1px solid ${C.greenB}`,background:"none",color:C.green,fontFamily:C.mono,fontSize:9,cursor:"pointer",flexShrink:0}}>Reset</button>
          </div>
        )}
        {dayCount>90&&(
          <div style={{marginTop:12,padding:"10px 13px",borderRadius:9,background:C.amberL,border:`1.5px solid ${C.amberB}`,display:"flex",gap:8,alignItems:"flex-start"}}>
            <span style={{fontSize:13,flexShrink:0}}>⏱</span>
            <p style={{fontFamily:C.mono,fontSize:10,color:C.amber,margin:0,lineHeight:1.65}}><strong>Large date range ({dayCount} days)</strong> — Voucher sync will be split into <strong>~{chunkCount} chunks of 15 days</strong> to prevent Tally from hanging.</p>
          </div>
        )}
      </div>

      {/* Step 3 — Sync Mode Tabs */}
      <div style={card}>
        <SectionHead step="3" title="Sync Mode" done={false}/>
        <div style={{display:"flex",gap:9,marginBottom:20}}>
          {[{id:false,icon:"🖱",label:"Manual",desc:"Sync on demand"},{id:true,icon:"⏱",label:"Auto Sync",desc:"Runs on schedule"}].map(m=>(
            <button key={String(m.id)} onClick={()=>{setAutoMode(m.id);if(!m.id)setAutoRunning(false);}}
              style={{flex:1,padding:"12px 14px",borderRadius:10,border:`1.5px solid ${autoMode===m.id?C.accentD:C.border}`,background:autoMode===m.id?`linear-gradient(135deg,${C.accent},${C.accentD})`:C.surface,cursor:"pointer",transition:"all .15s",display:"flex",flexDirection:"column",alignItems:"center",gap:4,boxShadow:autoMode===m.id?`0 4px 14px ${C.accent}33`:"none"}}>
              <span style={{fontSize:20}}>{m.icon}</span>
              <span style={{fontFamily:C.title,fontSize:12.5,fontWeight:700,color:autoMode===m.id?"#fff":C.ink}}>{m.label}</span>
              <span style={{fontFamily:C.mono,fontSize:9,color:autoMode===m.id?"rgba(255,255,255,.55)":C.muted}}>{m.desc}</span>
            </button>
          ))}
        </div>

        {/* ── MANUAL MODE ───────────────────────────────────────────────── */}
        {!autoMode&&(
          <div style={{animation:"se-fade .2s ease"}}>
            <div style={{background:C.accentL,border:`1.5px solid ${C.accentB}`,borderRadius:9,padding:"10px 14px",marginBottom:16,display:"flex",gap:9,alignItems:"flex-start"}}>
              <span style={{fontSize:14,flexShrink:0}}>💡</span>
              <p style={{fontFamily:C.mono,fontSize:10,color:C.accentD,margin:0,lineHeight:1.65}}><strong>Recommended order:</strong> 🗂 Chart of Accounts → 👥 Ledgers → ⚡ Smart Ledgers → 🧾 Vouchers<br/>Always sync ledgers before vouchers to avoid "Account not found" errors.</p>
            </div>
            <SyncCheckboxGrid
              opts={syncOpts}
              onToggle={toggleOpt}
              onIndividualSync={(individual)=>runSync(individual)}
              busy={busy}
              co={co}
              loadingItem={loading}
              mode="manual"
            />
            <div style={{marginTop:20}}>
              <button className="se-btn" onClick={()=>runSync("full")} disabled={busy||!co||noSync||!erpCompany}
                style={{width:"100%",display:"flex",alignItems:"center",justifyContent:"center",gap:11,padding:"15px 22px",borderRadius:11,border:"none",background:busy||!isReady?C.surface:`linear-gradient(135deg,${C.accent},${C.accentD})`,color:busy||!isReady?C.dim:"#fff",fontFamily:C.title,fontSize:14,fontWeight:800,letterSpacing:"-0.3px",cursor:busy||!isReady?"not-allowed":"pointer",boxShadow:busy||!isReady?"none":`0 4px 18px ${C.accent}44`,transition:"all .2s"}}>
                {loading==="full"?<><Spinner color={busy&&isReady?"#fff":C.dim} size={15}/> Syncing to ERPNext…</>:<><span style={{fontSize:16}}>⇄</span> Sync Selected → ERPNext</>}
                {!busy&&isReady&&<span style={{fontFamily:C.mono,fontSize:10,opacity:0.6,fontWeight:400}}>{Object.values(syncOpts).filter(Boolean).length} item{Object.values(syncOpts).filter(Boolean).length!==1?"s":""} selected</span>}
              </button>
              {!isReady&&!busy&&(
                <div style={{marginTop:9,padding:"9px 13px",borderRadius:9,background:C.amberL,border:`1.5px solid ${C.amberB}`}}>
                  <p style={{fontFamily:C.mono,fontSize:10,color:C.amber,margin:0}}>{!co?"⚠ Select a Tally company first":!erpCompany?"⚠ Enter ERPNext company name in Step 1":noSync?"⚠ Select at least one item to sync":""}</p>
                </div>
              )}
            </div>
          </div>
        )}

        {/* ── AUTO MODE ─────────────────────────────────────────────────── */}
        {autoMode&&(
          <div style={{animation:"se-fade .2s ease"}}>

            {/* Auto-sync specific checkbox selection */}
            <div style={{marginBottom:18}}>
              <div style={{display:"flex",alignItems:"center",gap:9,marginBottom:13,padding:"10px 14px",borderRadius:9,background:C.tealL,border:`1.5px solid ${C.tealB}`}}>
                <span style={{fontSize:15,flexShrink:0}}>⚙️</span>
                <div style={{flex:1}}>
                  <p style={{fontFamily:C.title,fontSize:12,fontWeight:700,color:C.teal,margin:0}}>Auto-Sync Data Selection</p>
                  <p style={{fontFamily:C.mono,fontSize:9,color:C.teal,margin:"2px 0 0",opacity:0.8}}>Choose what gets synced on every scheduled run — saved separately from manual sync</p>
                </div>
                <span style={{fontFamily:C.mono,fontSize:9,fontWeight:700,padding:"3px 9px",borderRadius:12,background:C.tealB,color:C.teal}}>
                  {Object.values(autoSyncOpts).filter(Boolean).length}/{SYNC_OPTIONS.length}
                </span>
              </div>
              <SyncCheckboxGrid
                opts={autoSyncOpts}
                onToggle={toggleAutoOpt}
                mode="auto"
              />
              {noAutoSync&&(
                <div style={{marginTop:9,padding:"9px 13px",borderRadius:9,background:C.amberL,border:`1.5px solid ${C.amberB}`}}>
                  <p style={{fontFamily:C.mono,fontSize:10,color:C.amber,margin:0}}>⚠ Select at least one data type to enable auto-sync</p>
                </div>
              )}
            </div>

            {/* Interval picker */}
            <label style={{display:"block",fontFamily:C.mono,fontSize:9,color:C.muted,letterSpacing:"0.14em",textTransform:"uppercase",marginBottom:7,fontWeight:700}}>Sync Interval</label>
            <div style={{display:"flex",gap:5,flexWrap:"wrap",marginBottom:14}}>
              {INTERVALS.map(iv=>(
                <button key={iv.value} onClick={()=>setAutoInterval(iv.value)} disabled={autoRunning}
                  style={{padding:"5px 12px",borderRadius:20,background:autoInterval===iv.value?C.accentD:C.surface,border:`1.5px solid ${autoInterval===iv.value?C.accentD:C.border}`,color:autoInterval===iv.value?"#fff":C.muted,fontFamily:C.mono,fontSize:10,fontWeight:600,cursor:autoRunning?"not-allowed":"pointer",opacity:autoRunning&&autoInterval!==iv.value?0.4:1,transition:"all .15s"}}>
                  {iv.label}
                </button>
              ))}
            </div>

            {/* Start/Stop button */}
            <div style={{display:"flex",alignItems:"center",gap:12}}>
              <button onClick={()=>setAutoRunning(r=>!r)} disabled={!co||noAutoSync||!erpCompany}
                style={{flex:1,display:"flex",alignItems:"center",justifyContent:"center",gap:8,padding:"11px 16px",borderRadius:9,border:"none",background:autoRunning?C.red:C.green,color:"#fff",fontFamily:C.title,fontSize:13,fontWeight:700,cursor:!co||noAutoSync||!erpCompany?"not-allowed":"pointer",opacity:!co||noAutoSync||!erpCompany?0.5:1,transition:"all .15s",boxShadow:autoRunning?"0 4px 14px rgba(220,38,38,.28)":"0 4px 14px rgba(22,163,74,.28)"}}>
                {autoRunning?<><span>■</span> Stop Auto-Sync</>:<><span>▶</span> Start Auto-Sync</>}
              </button>
              {autoRunning&&<div style={{display:"flex",flexDirection:"column",alignItems:"center",gap:2}}><CountdownRing remainingMs={autoRemainingMs} totalMs={autoInterval}/><span style={{fontFamily:C.mono,fontSize:9,color:C.muted}}>next run</span></div>}
            </div>

            {/* Running status bar */}
            {autoRunning&&(
              <div style={{marginTop:12,padding:"11px 14px",borderRadius:9,background:C.accentL,border:`1.5px solid ${C.accentB}`,display:"flex",alignItems:"center",gap:9}}>
                <span style={{width:7,height:7,borderRadius:"50%",background:C.accent,flexShrink:0,animation:"se-pulse 1.4s ease-in-out infinite"}}/>
                <div style={{flex:1}}>
                  <p style={{fontFamily:C.mono,fontSize:11,color:C.accentD,fontWeight:600,margin:0}}>Auto-sync active — every {INTERVALS.find(i=>i.value===autoInterval)?.label}</p>
                  <p style={{fontFamily:C.mono,fontSize:10,color:C.muted,margin:"2px 0 0"}}>{autoRunCount} run{autoRunCount!==1?"s":""} completed{autoNextRun&&` · Next at ${autoNextRun.toLocaleTimeString("en-IN",{hour12:false})}`}</p>
                </div>
                {autoSyncing&&<Spinner size={12}/>}
              </div>
            )}

            {/* Last auto-sync result — up to date or error */}
            {lastAutoResult&&!autoSyncing&&(
              <div style={{marginTop:12,animation:"se-fade .2s ease"}}>
                {lastAutoResult.upToDate?(
                  <UpToDateBanner finishedAt={lastAutoResult.result?.finishedAt} mode="auto"/>
                ):lastAutoResult.nothingSelected?(
                  <div style={{padding:"11px 14px",borderRadius:9,background:C.amberL,border:`1.5px solid ${C.amberB}`}}>
                    <p style={{fontFamily:C.mono,fontSize:10,color:C.amber,margin:0}}>⚠ Auto-sync ran but no data types were selected — nothing was synced.</p>
                  </div>
                ):lastAutoResult.error?(
                  <div style={{padding:"11px 14px",borderRadius:9,background:C.redL,border:`1.5px solid ${C.redB}`}}>
                    <p style={{fontFamily:C.mono,fontSize:10,color:C.red,margin:0,fontWeight:700}}>✗ Auto-sync failed</p>
                    <p style={{fontFamily:C.mono,fontSize:10,color:C.red,margin:"4px 0 0"}}>{lastAutoResult.error}</p>
                  </div>
                ):(
                  <div style={{padding:"11px 14px",borderRadius:9,background:C.greenL,border:`1.5px solid ${C.greenB}`,display:"flex",alignItems:"center",gap:9}}>
                    <span style={{fontSize:14}}>✓</span>
                    <p style={{fontFamily:C.mono,fontSize:10,color:C.green,margin:0,fontWeight:600}}>Auto-sync completed successfully</p>
                  </div>
                )}
              </div>
            )}

            {/* Sync history */}
            {autoHistory.length>0&&(
              <div style={{marginTop:14}}>
                <label style={{display:"block",fontFamily:C.mono,fontSize:9,color:C.muted,letterSpacing:"0.14em",textTransform:"uppercase",marginBottom:7,fontWeight:700}}>Sync History</label>
                <div style={{display:"flex",flexDirection:"column",gap:4}}>
                  {autoHistory.map((h,i)=>(
                    <div key={i} style={{display:"flex",alignItems:"center",gap:9,padding:"8px 12px",borderRadius:8,background:h.upToDate?C.tealL:C.surface,border:`1px solid ${h.upToDate?C.tealB:C.border}`}}>
                      <span style={{width:7,height:7,borderRadius:"50%",flexShrink:0,background:h.upToDate?C.teal:h.status==="ok"?C.green:h.status==="failed"?C.red:C.amber}}/>
                      <div style={{flex:1}}>
                        <span style={{fontFamily:C.mono,fontSize:10,color:C.muted,display:"block"}}>{h.at.toLocaleTimeString("en-IN",{hour12:false})}</span>
                        {h.from&&<span style={{fontFamily:C.mono,fontSize:9,color:C.dim,display:"block"}}>{h.from} → {h.to}</span>}
                      </div>
                      <StatusBadge status={h.upToDate?"uptodate":h.status}/>
                      {h.error&&<span style={{fontFamily:C.mono,fontSize:9,color:C.red,maxWidth:160,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{h.error}</span>}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Progress banner (manual mode) */}
      {!autoMode&&activeJob&&<div style={{animation:"se-fade .2s ease"}}><JobProgressBanner jobId={activeJob.jobId} type={activeJob.type}/></div>}

      {/* Result panel (manual mode) */}
      {!autoMode&&result&&!activeJob&&(
        <div style={{animation:"se-pop .25s ease"}}>
          {result.error?(
            <div style={{background:C.redL,border:`1.5px solid ${C.redB}`,borderRadius:13,padding:"15px 18px"}}>
              <p style={{fontFamily:C.mono,fontSize:12,color:C.red,fontWeight:700,margin:0}}>✗ {result.error==="Sync was stopped by you."?"Sync stopped":"Sync failed"}</p>
              <p style={{fontFamily:C.mono,fontSize:11,color:C.red,margin:"5px 0 0"}}>{result.error}</p>
            </div>
          ):result.data?.ok&&result.data?.result?.serverRestarted?(
            <div style={{background:C.card,border:`1.5px solid ${C.amberB}`,borderRadius:14,padding:18,display:"flex",flexDirection:"column",gap:10}}>
              <div style={{display:"flex",alignItems:"center",gap:11}}>
                <div style={{width:36,height:36,borderRadius:10,background:C.amberL,border:`1.5px solid ${C.amberB}`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:20,flexShrink:0}}>⚠</div>
                <div style={{flex:1}}>
                  <p style={{fontFamily:C.title,fontSize:13.5,fontWeight:800,color:C.ink,margin:0,letterSpacing:"-0.3px"}}>Server Restarted During Sync</p>
                  <p style={{fontFamily:C.mono,fontSize:10,color:C.amber,margin:"3px 0 0"}}>The sync likely completed — please verify in ERPNext</p>
                </div>
                <StatusBadge status="warning"/>
              </div>
              <p style={{fontFamily:C.mono,fontSize:10,color:C.muted,margin:0,lineHeight:1.6}}>{result.data.result.note}</p>
            </div>
          ):result.data?.ok&&result.data?.result?.nothingToSync?(
            /* ── Manual "Up to Date" banner ── */
            <UpToDateBanner finishedAt={result.data.result?.finishedAt} mode="manual"/>
          ):result.data?.ok?(
            <div style={{background:C.card,border:`1.5px solid ${C.greenB}`,borderRadius:14,padding:18,display:"flex",flexDirection:"column",gap:11,boxShadow:`0 4px 18px ${C.green}14`}}>
              <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",paddingBottom:13,borderBottom:`1px solid ${C.border}`}}>
                <div style={{display:"flex",alignItems:"center",gap:9}}>
                  <div style={{width:30,height:30,borderRadius:8,background:C.greenL,border:`1.5px solid ${C.greenB}`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:14}}>✓</div>
                  <span style={{fontFamily:C.title,fontSize:13.5,fontWeight:800,color:C.ink,letterSpacing:"-0.3px"}}>Sync Complete</span>
                </div>
                <StatusBadge status={result.data.result?.status||"ok"}/>
              </div>
              {result.data.result?.steps&&(
                <div style={{display:"flex",flexDirection:"column",gap:7}}>
                  <StepResult title="ERPNext Ping"       data={result.data.result.steps.erpnextPing}/>
                  <StepResult title="Chart of Accounts"  data={result.data.result.steps.chartOfAccounts}/>
                  <StepResult title="Ledgers"            data={result.data.result.steps.ledgers}/>
                  <StepResult title="Opening Balances"   data={result.data.result.steps.openingBalances}/>
                  <StepResult title="Godowns/Warehouses" data={result.data.result.steps.godowns}/>
                  <StepResult title="Cost Centres"       data={result.data.result.steps.costCentres}/>
                  <StepResult title="Stock Items"        data={result.data.result.steps.stockItems}/>
                  <StepResult title="Vouchers"           data={result.data.result.steps.vouchers}/>
                  <StepResult title="Invoices"           data={result.data.result.steps.invoices}/>
                </div>
              )}
              {!result.data.result?.steps&&result.data.result&&<StepResult title={result.type} data={result.data.result}/>}
              {result.data.result?.finishedAt&&<p style={{fontFamily:C.mono,fontSize:10,color:C.muted,margin:0}}>Completed at {new Date(result.data.result.finishedAt).toLocaleTimeString("en-IN")}</p>}
            </div>
          ):null}
        </div>
      )}
    </div>
  );
}