type StatusTone = "ready" | "warning" | "danger" | "neutral" | "info";

const TONE_STYLES: Record<StatusTone, string> = {
  ready:
    "border-emerald-700/20 bg-emerald-100 text-emerald-900 shadow-[inset_0_1px_0_rgba(255,255,255,0.45)]",
  warning:
    "border-amber-700/20 bg-amber-100 text-amber-900 shadow-[inset_0_1px_0_rgba(255,255,255,0.45)]",
  danger:
    "border-rose-700/20 bg-rose-100 text-rose-900 shadow-[inset_0_1px_0_rgba(255,255,255,0.45)]",
  neutral:
    "border-slate-900/10 bg-white/70 text-slate-800 shadow-[inset_0_1px_0_rgba(255,255,255,0.45)]",
  info: "border-cyan-700/20 bg-cyan-100 text-cyan-900 shadow-[inset_0_1px_0_rgba(255,255,255,0.45)]",
};

export function statusToneFromValue(value: string | null | undefined): StatusTone {
  const normalized = value?.toLowerCase() ?? "";
  if (["complete", "completed", "ready", "ok", "sent", "drafted"].includes(normalized)) {
    return "ready";
  }
  if (["warning", "pending-live-validation", "queued", "running", "draft"].includes(normalized)) {
    return "warning";
  }
  if (["failed", "missing", "error"].includes(normalized)) {
    return "danger";
  }
  if (["info", "skipped", "idle", "standby", "manual"].includes(normalized)) {
    return "info";
  }
  return "neutral";
}

export function StatusBadge({
  label,
  value,
}: {
  label?: string;
  value: string;
}) {
  const tone = statusToneFromValue(value);

  return (
    <span
      className={`inline-flex items-center gap-2 rounded-full border px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.24em] ${TONE_STYLES[tone]}`}
    >
      {label ? <span className="opacity-60">{label}</span> : null}
      <span>{value.replaceAll("-", " ")}</span>
    </span>
  );
}
