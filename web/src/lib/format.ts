import type { Mark } from "./state";

export function bytes(value?: number): string {
  if (value === undefined) return "Unknown";
  const scale = Math.min(4, Math.floor(Math.log(Math.max(1, value)) / Math.log(1024)));
  return `${(value / 1024 ** scale).toLocaleString(undefined, { maximumFractionDigits: 1 })} ${["B", "KiB", "MiB", "GiB", "TiB"][scale]}`;
}

export function duration(seconds?: number): string {
  if (seconds === undefined) return "No good snapshot";
  if (seconds < 60) return "Less than a minute";
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} h ${Math.floor(seconds % 3600 / 60)} min`;
  return `${Math.floor(seconds / 86400)} d ${Math.floor(seconds % 86400 / 3600)} h`;
}

export function timestamp(value?: string | null): string {
  if (!value || value.startsWith("0001-")) return "Not reported";
  return new Date(value).toLocaleString(undefined, { timeZone: "UTC", year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" }) + " UTC";
}

export function protectionMark(state: string): Mark {
  switch (state) {
    case "protected": return { glyph: "✓", shape: "circle", tone: "ok", label: "Protected" };
    case "failed": case "blocked": return { glyph: "✕", shape: "diamond", tone: "fail", label: state === "blocked" ? "Blocked" : "Failed" };
    case "overdue": case "partial": case "warning": return { glyph: "!", shape: "triangle", tone: "attn", label: state.charAt(0).toUpperCase() + state.slice(1) };
    case "pending": case "running": return { glyph: "⇢", shape: "circle-open", tone: "active", label: state === "pending" ? "Pending" : "Running" };
    case "waiting": return { glyph: "?", shape: "diamond-hatched", tone: "unc", label: "Waiting" };
    case "retained": return { glyph: "▪", shape: "square", tone: "neutral", label: "Retained copies" };
    case "unprotected": return { glyph: "–", shape: "square-open", tone: "neutral", label: "Unprotected" };
    default: return { glyph: "?", shape: "circle-dashed", tone: "neutral", label: "Unknown" };
  }
}
