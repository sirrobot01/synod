// Derivations from API data. Every value on a screen comes from a real API answer.

import type { App, AppPlan, Destination, DestinationList, Node, Operation, ProtectionPlan, Snapshot } from "./api";
import type { Mark, Tone } from "./state";
import { bytes, limits, protectionMark } from "./format";
import type { ScreenName } from "./router.svelte";

/** A device that reported in the last two minutes is online. The controller uses the same limit. */
export const onlineWindow = 120_000;

export const nilID = "00000000-0000-0000-0000-000000000000";

export function online(node: Node, now: number): boolean {
  return !!node.last_seen_at && now - Date.parse(node.last_seen_at) <= onlineWindow;
}

// Times -----------------------------------------------------------------

export function valid(value?: string | null): value is string {
  return !!value && !value.startsWith("0001-") && !Number.isNaN(Date.parse(value));
}

export function clock(value: string): string {
  return new Date(value).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false });
}

/** day names a date relative to now: today, yesterday, tomorrow, or the date. */
export function day(value: string, now: number): string {
  const date = new Date(value);
  const today = new Date(now);
  const start = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  const days = Math.round((start(date) - start(today)) / 86_400_000);
  if (days === 0) return "today";
  if (days === -1) return "yesterday";
  if (days === 1) return "tomorrow";
  return date.toLocaleDateString("en-US", { month: "short", day: "2-digit", ...(date.getFullYear() !== today.getFullYear() ? { year: "numeric" } : {}) });
}

/** when reads a time as "03:12 today" or "03:12 Oct 12". */
export function when(value: string | null | undefined, now: number, fallback = "never"): string {
  if (!valid(value)) return fallback;
  return `${clock(value)} ${day(value, now)}`;
}

/** short reads a time as "03:12" today and as "Oct 12" on another day. */
export function short(value: string | null | undefined, now: number): string {
  if (!valid(value)) return "";
  if (day(value, now) === "today") return clock(value);
  const date = new Date(value);
  return date.toLocaleDateString("en-US", { month: "short", day: "2-digit", ...(date.getFullYear() !== new Date(now).getFullYear() ? { year: "numeric" } : {}) });
}

/** span reads a length of time compactly: 12m, 9h, 3d. */
export function span(ms: number): string {
  const minutes = Math.floor(ms / 60_000);
  if (minutes < 1) return "<1m";
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h`;
  return `${Math.floor(hours / 24)}d`;
}

export function ago(value: string | null | undefined, now: number): string {
  return valid(value) ? `${span(now - Date.parse(value))} ago` : "";
}

// Devices ---------------------------------------------------------------

export function deviceState(node: Node, now: number): { label: string; mark: Mark } {
  if (!node.approved_at) return { label: "Not approved", mark: { glyph: "!", shape: "square-half", tone: "attn", label: "Not approved" } };
  if (online(node, now)) return { label: "Online", mark: { glyph: "✓", shape: "circle", tone: "ok", label: "Online" } };
  if (!valid(node.last_seen_at)) return { label: "Never connected", mark: { glyph: "?", shape: "circle-dashed", tone: "neutral", label: "Never connected" } };
  const label = `Offline ${span(now - Date.parse(node.last_seen_at))}`;
  return { label, mark: { glyph: "–", shape: "circle-open", tone: "neutral", label } };
}

export function nodeName(nodes: Node[], id: string | undefined): string {
  if (!id) return "";
  const node = nodes.find((n) => n.id === id);
  return node?.name || node?.id || "unknown device";
}

// Apps ------------------------------------------------------------------

/** newest is the time of the newest good snapshot of an app in any storage. */
export function newest(item: AppPlan): string | undefined {
  let found: string | undefined;
  for (const target of item.targets ?? []) {
    const at = target.last_good?.completed_at;
    if (valid(at) && (!found || Date.parse(at) > Date.parse(found))) found = at;
  }
  return found;
}

/** nextRun is the earliest due time of a selected storage. */
export function nextRun(item: AppPlan): string | undefined {
  let next: string | undefined;
  for (const target of item.targets ?? []) {
    if (!target.selected || !valid(target.due_at)) continue;
    if (!next || Date.parse(target.due_at) < Date.parse(next)) next = target.due_at;
  }
  return next;
}

/** One line under the status of an app. The mark holds the status word. */
const stateTexts: Record<string, string> = {
  protected: "Every path and database is in the newest snapshot.",
  partial: "The newest snapshot is usable but has limits.",
  overdue: "The scheduled backup did not run on time.",
  failed: "The last backup failed. Older snapshots are intact.",
  blocked: "Backups cannot run until the problem below is fixed.",
  warning: "The last backup finished with a warning.",
  pending: "The first backup runs at the next scheduled time.",
  running: "A backup is running.",
  waiting: "The device must inspect unfinished work.",
  retained: "Old snapshots are kept. No new backups run.",
  unprotected: "No backups run for this app.",
};

export function stateWords(state: string): { label: string; text: string } {
  return { label: protectionMark(state).label, text: stateTexts[state] ?? "" };
}

export const blocked: Record<string, string> = {
  app_missing: "The app was not in the last scan of its device.",
  app_definition_incomplete: "The app definition could not be read in full.",
};

// One short line for each failure code that an operation can report.
const failures: Record<string, string> = {
  destination_unavailable: "Storage unreachable.",
  repository_open_failed: "Repository did not open. Check the password and access.",
  repository_installation_conflict: "Repository belongs to another Synod installation.",
  repository_uninitialized: "Repository holds no snapshots yet.",
  app_scan_failed: "App scan failed on the device.",
  image_capture_failed: "Image export failed.",
  database_dump_failed: "Database dump failed.",
  database_unavailable: "Database did not answer.",
  database_not_detected: "Expected database not found.",
  app_definition_incomplete: "App definition incomplete.",
  app_state_unsafe: "App was in a state that cannot be copied safely.",
  app_definition_limit: "App definition exceeds the size limit.",
  standalone_definition_unsupported: "Standalone container type not supported.",
  source_unavailable: "A source path was missing.",
  app_definition_excluded: "App definition is excluded.",
  first_copy_failed: "First copy pass failed.",
  journal_save_failed: "Device could not write its journal.",
  app_stop_failed: "App did not stop for the final pass.",
  app_changed_during_backup: "App changed during the copy. The next run retries.",
  final_copy_failed: "Final copy pass failed.",
  sqlite_copy_failed: "SQLite copy failed.",
  sqlite_copy_unavailable: "SQLite database could not be copied safely.",
  agent_interrupted: "Agent stopped before the work finished.",
  snapshot_publish_failed: "Snapshot could not be written to storage.",
  snapshot_not_found: "Snapshot no longer exists.",
  snapshot_invalid: "Snapshot is damaged.",
  image_platform_unsupported: "No image in the snapshot for this architecture.",
  image_archive_missing: "Images missing from the snapshot.",
  image_archive_invalid: "Images in the snapshot are damaged.",
  image_load_failed: "docker load failed.",
  compose_unavailable: "Docker Compose not available on the device.",
  definition_unsupported: "App definition type not supported for restore.",
  app_conflict: "An app with this name exists on the device.",
  port_conflict: "A required port is in use.",
  host_port_unavailable: "A required host port is unavailable.",
  host_requirements_unverified: "Host requirements could not be checked.",
  device_unavailable: "A required device file is missing.",
  mount_unavailable: "A required mount path is missing.",
  network_conflict: "A Docker network with this name exists.",
  source_path_unavailable: "A restore path is unavailable.",
  file_restore_failed: "File restore failed.",
  volume_restore_failed: "Volume restore failed.",
  database_restore_failed: "Database load failed.",
  safety_backup_failed: "Safety snapshot failed. The app was not replaced.",
  app_changed_during_restore: "App changed during the restore.",
  compose_start_failed: "Restored app did not start.",
  integrity_check_failed: "Check found damaged data.",
  drill_container_stopped: "A test container exited.",
  drill_isolation_failed: "Test could not be isolated from the network.",
  drill_check_failed: "Test copy failed its checks.",
  maintenance_not_owner: "Another device runs cleanup for this storage.",
  maintenance_failed: "Cleanup of expired snapshots failed.",
  backup_overdue: "Scheduled backup did not run on time.",
};

export function failureText(code: string | undefined): string {
  if (!code) return "";
  return failures[code] ?? "Failed.";
}

// Storage ---------------------------------------------------------------

export const kindNames: Record<Destination["location"]["kind"], string> = { device: "Device folder", local: "Local disk", sftp: "SFTP", s3: "S3" };

/** where names the place of a storage as a value: a host and path, or a bucket. */
export function where(destination: Destination, nodes: Node[]): { host: string; path: string } {
  const location = destination.location;
  switch (location.kind) {
    case "device":
      return { host: nodeName(nodes, location.device?.node_id ?? destination.node_id), path: location.path ?? "" };
    case "local":
      return { host: nodeName(nodes, destination.node_id), path: location.path ?? "" };
    case "sftp":
      return { host: `${location.sftp?.user}@${location.sftp?.host}:${location.sftp?.port}`, path: location.path ?? "" };
    case "s3":
      return { host: location.s3?.endpoint ?? "", path: [location.s3?.bucket, location.s3?.prefix].filter(Boolean).join("/") };
    default:
      return { host: "", path: "" };
  }
}

export const storageTypes: { kind: Destination["location"]["kind"]; title: string; text: string; offsite: boolean }[] = [
  { kind: "device", title: "Another device", text: "A folder on another Synod device. Survives the loss of the app host.", offsite: true },
  { kind: "local", title: "Local disk", text: "A folder or USB disk on the app host. Lost with that host.", offsite: false },
  { kind: "sftp", title: "SFTP", text: "A path on any SSH server, such as a NAS.", offsite: true },
  { kind: "s3", title: "S3", text: "Backblaze B2, Wasabi, MinIO, AWS, or another S3 API.", offsite: true },
];

/** storageNewest is the newest good snapshot of any app in one storage. */
export function storageNewest(plan: ProtectionPlan | null, id: string): string | undefined {
  let found: string | undefined;
  for (const item of plan?.apps ?? []) {
    for (const target of item.targets ?? []) {
      const at = target.last_good?.completed_at;
      if (target.destination_id === id && valid(at) && (!found || Date.parse(at) > Date.parse(found))) found = at;
    }
  }
  return found;
}

/** used is the share of the filesystem in use, from 0 to 100, when it is known. */
export function used(listing: DestinationList | null, id: string): number | undefined {
  const space = listing?.space.find((s) => s.destination_id === id);
  if (!space?.known || space.total_bytes <= 0) return undefined;
  return Math.round(((space.total_bytes - space.available_bytes) / space.total_bytes) * 100);
}

// Overview --------------------------------------------------------------

export interface Step {
  key: "devices" | "storage" | "schedule";
  title: string;
  text: string;
  done: boolean;
}

/** setup is the first-run list. It is complete when every step is done. */
export function setup(nodes: Node[], destinations: Destination[], plan: ProtectionPlan | null): { steps: Step[]; done: number; next: Step["key"] | null; complete: boolean } {
  const steps: Step[] = [
    { key: "devices", title: "Add a device", text: "Run the agent on a Docker host, then approve it.", done: nodes.some((n) => n.approved_at) },
    { key: "storage", title: "Add storage", text: "A folder on another device, an SFTP path, or an S3 bucket.", done: destinations.length > 0 },
    { key: "schedule", title: "Schedule backups", text: "Pick the apps, a daily time, and retention.", done: (plan?.apps ?? []).some((a) => a.enabled) },
  ];
  const done = steps.filter((s) => s.done).length;
  return { steps, done, next: steps.find((s) => !s.done)?.key ?? null, complete: done === steps.length };
}

export interface Problem {
  key: string;
  tone: Tone;
  /** The app, device, or storage that the problem is about. */
  subject: string;
  /** The condition: a short phrase or a stable code. */
  condition: string;
  code: boolean;
  detail: string;
  link?: { label: string; screen: ScreenName; id: string };
}

const mark: Record<string, Mark> = {
  fail: { glyph: "✕", shape: "diamond", tone: "fail", label: "Failed" },
  attn: { glyph: "!", shape: "triangle", tone: "attn", label: "Needs action" },
  neutral: { glyph: "–", shape: "circle-open", tone: "neutral", label: "Notice" },
};

export function problemMark(tone: Tone): Mark {
  return mark[tone] ?? mark["neutral"]!;
}

/** problems lists what needs action, the most serious first. */
export function problems(plan: ProtectionPlan | null, nodes: Node[], listing: DestinationList | null, now: number): Problem[] {
  const list: Problem[] = [];
  const apps = plan?.apps ?? [];
  const name = (id?: string) => listing?.destinations.find((d) => d.id === id)?.name ?? "storage";
  for (const item of apps) {
    if (!item.enabled || !item.app.present) continue;
    const label = item.app.definition.name;
    const open = { label: "Open", screen: "apps" as ScreenName, id: item.app.id };
    const failed = item.targets.find((t) => t.selected && t.failure_code);
    switch (item.state) {
      case "failed":
        list.push({ key: `app-${item.app.id}`, tone: "fail", subject: label, condition: failed?.failure_code ?? "backup failed", code: !!failed?.failure_code, detail: failed ? `${name(failed.destination_id)}: ${failureText(failed.failure_code)}` : stateWords("failed").text, link: open });
        break;
      case "blocked":
        list.push({ key: `app-${item.app.id}`, tone: "fail", subject: label, condition: item.blocked_code ?? "blocked", code: !!item.blocked_code, detail: blocked[item.blocked_code ?? ""] ?? stateWords("blocked").text, link: open });
        break;
      case "overdue": {
        const last = newest(item);
        list.push({ key: `app-${item.app.id}`, tone: "attn", subject: label, condition: "overdue", code: false, detail: last ? `Newest snapshot ${when(last, now)}.` : "No snapshot yet.", link: open });
        break;
      }
      case "partial":
      case "warning": {
        const codes = (item.limit_codes ?? []).filter((c) => c !== "restore_unproven");
        if (codes.length === 0) break;
        list.push({ key: `app-${item.app.id}`, tone: "attn", subject: label, condition: codes.join(", "), code: true, detail: codes.map((c) => limits[c]?.short ?? "").filter(Boolean).join(". ") + ".", link: open });
        break;
      }
      default:
        break;
    }
    if (item.last_drill?.state === "failed") {
      list.push({ key: `drill-${item.app.id}`, tone: "fail", subject: label, condition: item.last_drill.result.failure_code ?? "restore test failed", code: !!item.last_drill.result.failure_code, detail: `Restore test failed. ${failureText(item.last_drill.result.failure_code)}`.trim(), link: { label: "Open test", screen: "operation", id: item.last_drill.id } });
    }
    if (item.only_on_source) {
      list.push({ key: `source-${item.app.id}`, tone: "attn", subject: label, condition: "single device", code: false, detail: `Every snapshot is on ${item.app.node_name}, the host of the app.`, link: { label: "Storage", screen: "storage", id: "" } });
    }
  }
  for (const node of nodes) {
    if (!node.approved_at) {
      list.push({ key: `node-${node.id}`, tone: "attn", subject: node.name || node.id, condition: "not approved", code: false, detail: node.architecture ? `${node.architecture}, Docker ${node.runtime_version || "unknown"}.` : "Waiting for the first report.", link: { label: "Review", screen: "devices", id: node.id } });
      continue;
    }
    if (online(node, now) || !valid(node.last_seen_at)) continue;
    const mine = apps.filter((a) => a.app.node_id === node.id && a.enabled && a.app.present);
    const copies = mine.map((a) => ({ name: a.app.definition.name, at: newest(a) }));
    const oldest = copies.filter((c) => c.at).sort((a, b) => Date.parse(a.at!) - Date.parse(b.at!))[0];
    const last = oldest ? short(oldest.at, now) : "none";
    const detail = mine.length === 0 ? "No scheduled apps." : mine.length === 1 ? `${copies[0]!.name} last snapshot ${last}.` : `${mine.length} apps, oldest last snapshot ${last}.`;
    list.push({ key: `node-${node.id}`, tone: "neutral", subject: node.name, condition: `offline ${span(now - Date.parse(node.last_seen_at!))}`, code: false, detail, link: { label: "Open", screen: "devices", id: node.id } });
  }
  for (const check of listing?.checks ?? []) {
    if (check.state !== "failed") continue;
    list.push({ key: `check-${check.id}`, tone: "fail", subject: name(check.destination_id), condition: check.result.failure_code ?? "check failed", code: !!check.result.failure_code, detail: failureText(check.result.failure_code) || "Check failed.", link: { label: "Open check", screen: "operation", id: check.id } });
  }
  for (const upkeep of listing?.maintenance ?? []) {
    if (upkeep.last?.state !== "failed") continue;
    list.push({ key: `upkeep-${upkeep.destination_id}`, tone: "attn", subject: name(upkeep.destination_id), condition: upkeep.last.result.failure_code ?? "cleanup failed", code: !!upkeep.last.result.failure_code, detail: failureText(upkeep.last.result.failure_code) || "Cleanup failed.", link: { label: "Open cleanup", screen: "operation", id: upkeep.last.id } });
  }
  for (const destination of listing?.destinations ?? []) {
    const share = used(listing, destination.id);
    const space = listing?.space.find((s) => s.destination_id === destination.id);
    if (share !== undefined && share >= 90) {
      list.push({ key: `full-${destination.id}`, tone: "attn", subject: destination.name, condition: `${share}% full`, code: false, detail: `${bytes(space?.available_bytes)} free.`, link: { label: "Storage", screen: "storage", id: "" } });
    }
  }
  const unscheduled = apps.filter((a) => !a.enabled && a.app.present && nodes.some((n) => n.id === a.app.node_id && n.approved_at));
  if (unscheduled.length && listing?.destinations.length) {
    list.push({ key: "unscheduled", tone: "neutral", subject: unscheduled.length === 1 ? "1 app" : `${unscheduled.length} apps`, condition: "not scheduled", code: false, detail: unscheduled.slice(0, 5).map((a) => a.app.definition.name).join(", ") + (unscheduled.length > 5 ? ", …" : ""), link: { label: "Apps", screen: "apps", id: "" } });
  }
  const order: Record<string, number> = { fail: 0, attn: 1, unc: 2, neutral: 3 };
  return list.sort((a, b) => (order[a.tone] ?? 4) - (order[b.tone] ?? 4));
}

/** summary is the header line of the overview. */
export function summary(plan: ProtectionPlan | null, now: number): { restorable: number; total: number; newest?: string; next?: string } {
  const apps = (plan?.apps ?? []).filter((a) => a.app.present);
  const restorable = apps.filter((a) => newest(a)).length;
  const latest = apps.map(newest).filter((v): v is string => !!v).sort((a, b) => Date.parse(a) - Date.parse(b)).at(-1);
  const next = apps.filter((a) => a.enabled).map(nextRun).filter((v): v is string => !!v && Date.parse(v) > now).sort((a, b) => Date.parse(a) - Date.parse(b))[0];
  return { restorable, total: apps.length, ...(latest ? { newest: latest } : {}), ...(next ? { next } : {}) };
}

// Recovery matrix --------------------------------------------------------

export interface Cell {
  mark: Mark | null;
  value: string;
  title: string;
}

/** cell is one app in one storage: the target state and the newest snapshot there. */
export function cell(item: AppPlan, destinationID: string, now: number): Cell {
  const target = item.targets.find((t) => t.destination_id === destinationID);
  if (!target) return { mark: null, value: "", title: "Not scheduled to this storage" };
  const at = target.last_good?.completed_at;
  const label = protectionMark(target.state).label;
  return { mark: protectionMark(target.state), value: valid(at) ? short(at, now) : "none", title: `${label}${valid(at) ? `, newest snapshot ${when(at, now)}` : ", no snapshot"}${target.selected ? "" : ", kept only"}` };
}

/** tested is the restore-test cell of one app. */
export function tested(item: AppPlan, now: number): Cell {
  if (item.last_drill?.state === "failed") return { mark: { glyph: "✕", shape: "diamond", tone: "fail", label: "Restore test failed" }, value: valid(item.restore_tested_at) ? short(item.restore_tested_at, now) : "failed", title: "Latest restore test failed" };
  if (item.last_drill && (item.last_drill.state === "running" || item.last_drill.state === "pending")) return { mark: { glyph: "⇢", shape: "circle-half", tone: "active", label: "Testing" }, value: "running", title: "Restore test running" };
  if (valid(item.restore_tested_at)) return { mark: { glyph: "✓", shape: "circle", tone: "ok", label: "Tested" }, value: short(item.restore_tested_at, now), title: `Restore tested ${when(item.restore_tested_at, now)}` };
  return { mark: { glyph: "?", shape: "circle-dashed", tone: "unc", label: "Untested" }, value: "untested", title: "No restore test yet" };
}

// Activity --------------------------------------------------------------

export interface Names {
  app: (id: string) => string;
  node: (id: string) => string;
  storage: (id?: string) => string;
}

export const kinds: Record<string, string> = { backup: "backup", restore: "restore", restore_drill: "restore test", integrity_check: "check", repository_maintenance: "cleanup" };

/** activity reads one operation as table columns. */
export function activity(op: Operation, names: Names): { kind: string; subject: string; target: string; result: string; tone: Tone; took?: number } {
  const app = op.app_id && op.app_id !== nilID ? names.app(op.app_id) : "";
  const storage = names.storage(op.result?.destination_id);
  const done = op.status === "succeeded";
  const proof = op.kind === "restore_drill" || op.kind === "integrity_check";
  const result = ({ pending: "queued", running: "running", waiting: "inspect", cancelling: "stopping", succeeded: proof ? "passed" : "ok", failed: "failed", cancelled: "cancelled" } as const)[op.status] ?? op.status;
  const tone: Tone = done ? "ok" : op.status === "failed" ? "fail" : op.status === "waiting" ? "unc" : op.status === "cancelled" ? "neutral" : "active";
  const target = op.kind === "backup" || op.kind === "repository_maintenance" || op.kind === "integrity_check" ? storage : names.node(op.node_id);
  const took = op.finished_at && valid(op.created_at) ? Date.parse(op.finished_at) - Date.parse(op.created_at) : undefined;
  return { kind: kinds[op.kind] ?? op.kind.replaceAll("_", " "), subject: app || storage || "repository", target: app ? target : names.node(op.node_id), result, tone, ...(took !== undefined && took >= 0 ? { took } : {}) };
}

/** duration reads milliseconds as 41s, 3m 12s, or 1h 4m. */
export function took(ms: number): string {
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

// Restore ---------------------------------------------------------------

export interface Target {
  node: Node;
  disabled: boolean;
  note: string;
  same: boolean;
  destinationID: string;
}

/** targets lists every device for a restore of one snapshot, and says why a device cannot take it. */
export function targets(item: AppPlan, copy: Snapshot, nodes: Node[], destinations: Destination[], now: number): Target[] {
  return nodes.map((node) => {
    const readable = copy.destination_ids.find((id) => {
      const d = destinations.find((x) => x.id === id);
      return d && (d.location.kind !== "local" || d.node_id === node.id);
    }) ?? "";
    const same = node.id === item.app.node_id && item.app.present;
    const base = { node, same, destinationID: readable };
    if (!node.approved_at) return { ...base, disabled: true, note: "Not approved" };
    if (!online(node, now)) return { ...base, disabled: true, note: valid(node.last_seen_at) ? `Offline ${span(now - Date.parse(node.last_seen_at!))}` : "Never connected" };
    const arches = copy.restore_architectures ?? [];
    if (arches.length && node.architecture && !arches.includes(node.architecture)) return { ...base, disabled: true, note: `No ${node.architecture} image in this snapshot` };
    if (!readable) return { ...base, disabled: true, note: "Cannot read the storage of this snapshot" };
    if (same) return { ...base, disabled: false, note: `Runs ${item.app.definition.name} now. Replaced after a safety snapshot.` };
    return { ...base, disabled: false, note: "Online" };
  });
}

// Paths -----------------------------------------------------------------

const engines: Record<string, string> = { postgres: "PostgreSQL", mysql: "MySQL", mariadb: "MariaDB", redis: "Redis", sqlite: "SQLite" };

export interface PathRow {
  label: string;
  status: "included" | "dumped" | "excluded" | "never";
  size?: number;
  sizeKnown: boolean;
  chosen: boolean;
  reason: string;
  optional: boolean;
}

/** paths reads the discovered paths of an app as rows, with database dumps named by their engine. */
export function paths(app: App, included: string[] = []): PathRow[] {
  const rows: PathRow[] = [];
  const dumps = (app.definition.containers ?? []).filter((c) => c.database);
  for (const item of app.definition.paths ?? []) {
    const label = item.source || (item.volume ? item.volume : item.target || item.kind);
    const chosen = (item.status === "Large" || item.status === "Remote") && !!item.source && included.includes(item.source);
    const size = item.size_bytes !== undefined ? { size: item.size_bytes } : {};
    if (item.status === "Dumped") {
      if (dumps.length) continue;
      rows.push({ label, status: "dumped", ...size, sizeKnown: false, chosen: false, reason: item.reason, optional: false });
    } else if (item.status === "Included" || chosen) {
      rows.push({ label, status: "included", ...size, sizeKnown: !!item.size_known, chosen, reason: chosen ? "Included by you" : "", optional: false });
    } else {
      rows.push({ label, status: item.status === "Never" ? "never" : "excluded", ...size, sizeKnown: !!item.size_known, chosen: false, reason: item.status === "Large" ? "Large" : item.status === "Remote" ? "Network share" : item.reason, optional: item.status === "Large" || item.status === "Remote" });
    }
  }
  for (const container of dumps) {
    rows.push({ label: container.name, status: "dumped", sizeKnown: false, chosen: false, reason: `${engines[container.database!] ?? container.database} dump`, optional: false });
  }
  const order = { included: 0, dumped: 1, excluded: 2, never: 3 };
  return rows.sort((a, b) => order[a.status] - order[b.status]);
}
