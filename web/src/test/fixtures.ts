import type { AppPlan, ProtectionPlan, Destination, DestinationList, Node, Snapshot, NotificationStatus } from "../lib/api";

export const node: Node = { id: "n1", name: "odin", architecture: "arm64", runtime_version: "29.0", agent_version: "dev", enrolled_at: "2026-10-06T00:00:00Z", approved_at: "2026-10-06T00:00:00Z", connected_at: "2026-10-07T08:00:00Z", last_seen_at: "2026-10-07T08:00:00Z" };
export const destination: Destination = { id: "d1", name: "Backup disk", node_id: "n2", location: { kind: "device", path: "/srv/backups", device: { node_id: "n2", destination_id: "d1" } } };
export const destinations: DestinationList = { revision: 3, destinations: [destination], delivery: [], space: [], maintenance: [] };
export const app: AppPlan = {
  app: { id: "a1", node_id: "n1", node_name: "odin", observed_at: "2026-10-07T08:00:00Z", present: true,
    definition: { name: "notes", kind: "compose", containers: [], paths: [
      { source: "/srv/notes", kind: "bind", status: "Included", reason: "app data", size_known: true, size_bytes: 1024 },
      { source: "/srv/media", kind: "bind", status: "Large", reason: "media excluded" },
      { source: "/var/run/docker.sock", kind: "bind", status: "Never", reason: "host socket" },
    ] } },
  enabled: true, settings_current: true, settings: { destinations: ["d1"], method: "auto", at: "03:00", time_zone: "UTC", retention: { daily: 7, weekly: 4, monthly: 6 } },
  state: "partial", limit_codes: ["app_consistency_unverified"], only_on_source: false, data_loss_window_seconds: 3600, targets: [],
  restore_tested_at: "2026-10-01T03:00:00Z", last_drill: { id: "drill-failed", node_id: "n1", state: "failed", result: { failure_code: "drill_container_stopped" } },
};
export const plan: ProtectionPlan = { revision: 3, auto_protect: true, defaults: app.settings, apps: [app] };
export const snapshot: Snapshot = { id: "s1", destination_ids: ["d1"], source_node_id: "n1", source_node_name: "odin", architecture: "arm64", method: "stop", started_at: "2026-10-07T03:00:00Z", completed_at: "2026-10-07T03:01:00Z", file_bytes: 1024, paths: ["/srv/notes"], exclusions: [{ source: "/srv/media", reason: "Large" }] };
export const notifications: NotificationStatus = { revision: 2, settings: { configured: true, service: "ntfy", overdue_grace_hours: 24, missing_after_hours: 168, full_percent: 90 }, alerts: [] };
