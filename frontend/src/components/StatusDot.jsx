export function StatusDot({ status }) {
  const map = {
    ok:      "bg-emerald-500",
    warn:    "bg-amber-400",
    fail:    "bg-rose-500",
    pending: "bg-slate-300",
    running: "bg-blue-500",
  };
  const ping = status === "running" || status === "ok";
  return (
    <span className="relative inline-flex items-center justify-center w-2.5 h-2.5 flex-shrink-0">
      {ping && (
        <span className={`absolute inline-flex w-full h-full rounded-full opacity-60 animate-ping ${map[status]}`} />
      )}
      <span className={`relative inline-flex rounded-full w-2 h-2 shadow-sm ${map[status] || "bg-slate-300"}`} />
    </span>
  );
}