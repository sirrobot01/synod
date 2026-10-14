import type { Mark } from "./state";

export function bytes(value?: number): string {
  if (value === undefined) return "Unknown";
  const scale = Math.min(4, Math.floor(Math.log(Math.max(1, value)) / Math.log(1024)));
  return `${(value / 1024 ** scale).toLocaleString(undefined, { maximumFractionDigits: 1 })} ${["B", "KiB", "MiB", "GiB", "TiB"][scale]}`;
}

/** duration reads a data-loss window: the time since the newest good snapshot. */
export function duration(seconds?: number): string {
  if (seconds === undefined) return "no snapshot";
  if (seconds < 60) return "Less than a minute";
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} h ${Math.floor(seconds % 3600 / 60)} min`;
  return `${Math.floor(seconds / 86400)} d ${Math.floor(seconds % 86400 / 3600)} h`;
}

/** timestamp is the exact time in UTC, for support and for the record of one operation. */
export function timestamp(value?: string | null): string {
  if (!value || value.startsWith("0001-")) return "Not reported";
  return new Date(value).toLocaleString(undefined, { timeZone: "UTC", year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" }) + " UTC";
}

// Each limit keeps a snapshot from full protection. The text says how to remove it.
export const limits: Record<string, { short: string; text: string }> = {
  app_consistency_unverified: { short: "No single filesystem snapshot", text: "Files and databases were not read from one btrfs, ZFS, or LVM snapshot. Put the app data on one such volume for a point-in-time copy. Synod does not pause writes." },
  data_excluded: { short: "Data excluded", text: "Large paths, media, or files over 20 GiB were skipped. Include them with Edit schedule, or accept the gap." },
  image_platforms_limited: { short: "Images for one architecture", text: "The registry was unreachable, so only the source architecture of an image was saved. The next backup with registry access saves the rest." },
  host_requirements_unverified: { short: "Host requirements unverified", text: "Host ports or devices of a host-network service were not recorded. Run the agent as root." },
  container_layer_data: { short: "Data in container layer", text: "A container holds more than 64 MiB in its writable layer, which no backup copies. Move it to a volume or bind mount." },
  restore_unproven: { short: "Restore untested", text: "No restore test has passed. The monthly test runs after the first good backup. A health check or a published port lets the test verify each service." },
};

export function protectionMark(state: string): Mark {
  switch (state) {
    case "protected": return { glyph: "✓", shape: "circle", tone: "ok", label: "Restorable" };
    case "failed": return { glyph: "✕", shape: "diamond", tone: "fail", label: "Failed" };
    case "blocked": return { glyph: "✕", shape: "diamond", tone: "fail", label: "Blocked" };
    case "partial": return { glyph: "◐", shape: "circle-half", tone: "attn", label: "Partial" };
    case "overdue": return { glyph: "!", shape: "triangle", tone: "attn", label: "Overdue" };
    case "warning": return { glyph: "!", shape: "triangle", tone: "attn", label: "Warning" };
    case "pending": return { glyph: "⋯", shape: "circle-open", tone: "active", label: "Scheduled" };
    case "running": return { glyph: "⇢", shape: "circle-half", tone: "active", label: "Running" };
    case "waiting": return { glyph: "?", shape: "diamond-hatched", tone: "unc", label: "Waiting" };
    case "retained": return { glyph: "▪", shape: "square", tone: "neutral", label: "Retained" };
    case "unprotected": return { glyph: "–", shape: "square-open", tone: "neutral", label: "Not scheduled" };
    default: return { glyph: "?", shape: "circle-dashed", tone: "neutral", label: "Unknown" };
  }
}
