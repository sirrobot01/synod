// Placeholder data for the regions that the API does not serve yet.
//
// Specification 17 permits a mock and forbids inventing a field. Every mock here
// names the recorded gap in tmp/tasks/artifacts/TASK-024/api-gaps.md, and every
// screen that draws one states it in the interface. Nothing in this file reaches
// a real request, and nothing here is ever merged with a real response.
//
// The values follow the approved design, so a screen reads as the design draws it.

import type { Condition, Mark, OperationStatus, Tone } from "./state";

/** The recorded gaps, so a screen cites one rather than writing a sentence. */
export const gaps = {
  collections: "fleet, operation, and account lists are not connected",
  conditions: "no conditions in any response",
  steps: "no step progress on an operation",
  actions: "no cancel or retry route",
  nodes: "no node read route",
  backups: "no backup surface",
  health: "no installation health",
  observedAt: "no observation time or reason on a cluster",
  instances: "no instance read",
  plans: "no upgrade, scale, or restore plan route",
  topology: "no topology read",
  credentials: "no installation-wide credential metadata route",
  versions: "no qualified engine version catalogue",
} as const;

/** mark builds one mark, so a fixture reads as the design writes it. */
function mark(shape: Mark["shape"], tone: Tone, glyph: string, label: string): Mark {
  return { shape, tone, glyph, label };
}

export const ok = (label: string) => mark("circle", "ok", "✓", label);
export const bad = (label: string) => mark("diamond", "fail", "✕", label);
export const unknown = (label: string) => mark("circle-dashed", "neutral", "?", label);
export const attention = (label: string) => mark("triangle", "attn", "▲", label);
export const moving = (label: string) => mark("circle-half", "active", "⇢", label);
export const held = (label: string) => mark("diamond-hatched", "unc", "?", label);
export const idle = (label: string) => mark("square-open", "dim", "—", label);
export const stopped = (label: string) => mark("square", "neutral", "‖", label);

/* Dashboard ------------------------------------------------------------- */

export interface MockHealthRow {
  mark: Mark;
  label: string;
  meta: string;
  stale?: boolean;
}

export interface MockHealthPlane {
  title: string;
  source: string;
  what: string;
  rows: MockHealthRow[];
}

/** Three planes, never combined. They fail separately, so they are reported separately. */
export const healthPlanes: MockHealthPlane[] = [
  {
    title: "Management plane",
    source: "agents + control plane",
    what: "Can Synod observe and change things? Independent of whether databases serve traffic.",
    rows: [
      { mark: ok("3 agents fresh"), label: "3 agents fresh", meta: "≤ 6s" },
      { mark: unknown("1 agent expired"), label: "1 agent expired", meta: "4m 12s", stale: true },
      { mark: attention("etcd quorum 2/3"), label: "etcd quorum 2/3", meta: "degraded" },
    ],
  },
  {
    title: "Database plane",
    source: "engine reports",
    what: "Are the databases actually serving? Unaffected by a control-plane outage.",
    rows: [
      { mark: ok("3 clusters serving"), label: "3 clusters serving", meta: "fresh" },
      { mark: bad("1 replication broken"), label: "1 replication broken", meta: "pg-core" },
      { mark: bad("1 stopped, want running"), label: "1 stopped, want running", meta: "pg-billing" },
    ],
  },
  {
    title: "Backup plane",
    source: "artifact manifests",
    what: "Could we actually recover? Reported from manifests, never from the schedule.",
    rows: [
      { mark: ok("2 current"), label: "2 current", meta: "≤ 12h" },
      { mark: unknown("1 unknown"), label: "1 unknown", meta: "target down", stale: true },
      { mark: attention("1 never verified"), label: "1 never verified", meta: "bk_44e1" },
    ],
  },
];

export interface MockDecision {
  mark: Mark;
  title: string;
  detail: string;
  action: string;
  /** The screen to open, so a decision leads somewhere. */
  go: { screen: "operation" | "nodes" | "settings" | "cluster"; id: string };
}

/** Ordered by what is unrecoverable if ignored, not by severity colour. */
export const decisions: MockDecision[] = [
  {
    mark: held("waiting"),
    title: "op_c4f8ab12 waiting — outcome unknown",
    detail:
      "redis-cache upgrade step 4 dispatched, never acknowledged. The mutation slot stays held until inspected.",
    action: "Inspect →",
    go: { screen: "operation", id: "op_c4f8ab12" },
  },
  {
    mark: bad("contradicted"),
    title: "pg-billing observed stopped, desired running",
    detail: "Fresh observation contradicts intent: exit 137 on node-01. op_9a03 failed with 3 residual objects.",
    action: "Open operation →",
    go: { screen: "operation", id: "op_9a03be71" },
  },
  {
    mark: unknown("unknown"),
    title: "pg-analytics unknown for 4m 12s",
    detail:
      "node-03 has not reported since 14:02:11Z. Last seen running at rev 19 — the database may still be serving.",
    action: "Inspect node →",
    go: { screen: "nodes", id: "node-03" },
  },
  {
    mark: bad("unreachable"),
    title: "Backup target s3-lab unreachable",
    detail: "BackupCurrent went unknown on 2 clusters 3h 04m ago. No backup has failed — none can be confirmed.",
    action: "Settings →",
    go: { screen: "settings", id: "" },
  },
];

export interface MockActiveOperation {
  id: string;
  what: string;
  step: string;
  mark: Mark;
}

export const activeOperations: MockActiveOperation[] = [
  { id: "op_7c14f2a9", what: "redis-sess · clusters.create.v1", step: "3 / 6", mark: moving("running") },
  { id: "op_c4f8ab12", what: "redis-cache · clusters.upgrade.v1", step: "waiting", mark: held("waiting") },
  { id: "op_2d90ff31", what: "node-04 · nodes.drain.v1", step: "cancelling", mark: mark("square-half", "attn", "⊘", "cancelling") },
];

/** Expired observations are counted, not hidden. An expired report is not a failure. */
export const freshnessCounts = [
  { label: "fresh", count: 13, share: 72, tone: "ok" as Tone, dashed: false },
  { label: "ageing", count: 2, share: 11, tone: "attn" as Tone, dashed: false },
  { label: "expired", count: 3, share: 17, tone: "neutral" as Tone, dashed: true },
];

/* Clusters --------------------------------------------------------------- */

export interface MockFleetCell {
  mark: Mark;
  text: string;
}

export interface MockCluster {
  id: string;
  name: string;
  engine: string;
  topology: string;
  size: string;
  desired: string;
  desiredRevision: number;
  observed: string;
  observedRevision: number;
  /** Seconds since the observation, so the real freshness rules apply to it. */
  observedAgeSeconds: number;
  owningOperation?: string;
  management: MockFleetCell;
  database: MockFleetCell;
  backup: MockFleetCell;
  operation: MockFleetCell;
  conditions: Condition[];
}

/**
 * clusters builds the fleet against a clock, so every age on a screen is computed
 * by the real state system rather than written down as a finished string.
 */
export function clusters(now: Date): MockCluster[] {
  const at = (secondsAgo: number) => new Date(now.getTime() - secondsAgo * 1000).toISOString();
  const condition = (name: Condition["name"], value: Condition["value"], secondsAgo: number, revision: number, reason: string): Condition => ({
    name,
    value,
    observedAt: at(secondsAgo),
    observedRevision: revision,
    reason,
  });
  return [
    {
      id: "cl_31d0aa47",
      name: "pg-core",
      engine: "postgres 16.4 · ha",
      topology: "patroni ha · haproxy · pgbouncer",
      size: "3 inst · 100 GiB",
      desired: "running",
      desiredRevision: 14,
      observed: "running",
      observedRevision: 14,
      observedAgeSeconds: 4,
      management: { mark: ok("fresh"), text: "agent 4s" },
      database: { mark: bad("replica lag"), text: "replica lag 92s" },
      backup: { mark: unknown("unknown"), text: "unknown 3h" },
      operation: { mark: idle("none"), text: "none" },
      conditions: [
        condition("Ready", "true", 4, 14, "postconditions_met"),
        condition("Reachable", "true", 4, 14, "tcp_probe_ok"),
        condition("Replicated", "false", 11, 14, "replica_lag_92s"),
        condition("BackupCurrent", "unknown", 11064, 11, "target_unreachable"),
        condition("EndpointReady", "true", 6, 14, "haproxy_backend_up"),
      ],
    },
    {
      id: "cl_88be0c12",
      name: "pg-analytics",
      engine: "postgres 16.4 · standalone",
      topology: "standalone",
      size: "1 inst · 500 GiB",
      desired: "running",
      desiredRevision: 21,
      observed: "running",
      observedRevision: 19,
      observedAgeSeconds: 252,
      management: { mark: unknown("expired"), text: "agent 4m" },
      database: { mark: unknown("unknown"), text: "unknown" },
      backup: { mark: ok("current"), text: "38m ago" },
      operation: { mark: idle("none"), text: "none" },
      conditions: [
        condition("Ready", "true", 252, 19, "report_expired"),
        condition("Reachable", "true", 252, 19, "report_expired"),
        condition("BackupCurrent", "true", 2280, 19, "manifest_current"),
        condition("EndpointReady", "true", 252, 19, "report_expired"),
      ],
    },
    {
      id: "cl_4a7712de",
      name: "redis-sess",
      engine: "redis 7.2 · sentinel",
      topology: "sentinel · 3 quorum",
      size: "3 inst · 8 GiB",
      desired: "running",
      desiredRevision: 3,
      observed: "provisioning",
      observedRevision: 2,
      observedAgeSeconds: 12,
      owningOperation: "op_7c14f2a9",
      management: { mark: ok("fresh"), text: "agent 2s" },
      database: { mark: moving("provisioning"), text: "2/3 up" },
      backup: { mark: idle("no policy"), text: "no policy" },
      operation: { mark: moving("running"), text: "3/6 create" },
      // A cluster whose operation has not finished has not had these evaluated.
      // Unknown is the honest answer; false would paint normal progress as failure.
      conditions: [
        condition("Ready", "unknown", 12, 2, "not_yet_evaluated"),
        condition("Reachable", "true", 12, 2, "tcp_probe_ok"),
        condition("Replicated", "unknown", 12, 2, "replica_not_started"),
        condition("EndpointReady", "unknown", 12, 2, "sentinel_quorum_forming"),
      ],
    },
    {
      id: "cl_5fd10a93",
      name: "pg-billing",
      engine: "postgres 15.8 · ha",
      topology: "patroni ha · haproxy",
      size: "2 inst · 200 GiB",
      desired: "running",
      desiredRevision: 8,
      observed: "stopped",
      observedRevision: 8,
      observedAgeSeconds: 9,
      management: { mark: ok("fresh"), text: "agent 3s" },
      database: { mark: bad("exit 137"), text: "exit 137" },
      backup: { mark: ok("current"), text: "12m ago" },
      operation: { mark: bad("failed"), text: "failed · residual" },
      conditions: [
        condition("Ready", "false", 9, 8, "process_exited_137"),
        condition("Reachable", "false", 9, 8, "connection_refused"),
        condition("Replicated", "false", 9, 8, "no_primary"),
        condition("BackupCurrent", "true", 720, 8, "manifest_current"),
        condition("EndpointReady", "false", 9, 8, "haproxy_no_backend"),
      ],
    },
    {
      id: "cl_20cc7741",
      name: "redis-cache",
      engine: "redis 7.2 · cluster 3 shards",
      topology: "redis cluster · 3 shards",
      size: "3 inst · 24 GiB",
      desired: "running",
      desiredRevision: 5,
      observed: "running",
      observedRevision: 5,
      observedAgeSeconds: 6,
      management: { mark: ok("fresh"), text: "agent 1s" },
      database: { mark: ok("serving"), text: "3/3 shards" },
      backup: { mark: idle("no policy"), text: "no policy" },
      operation: { mark: held("waiting"), text: "upgrade waiting" },
      conditions: [
        condition("Ready", "true", 6, 5, "postconditions_met"),
        condition("Reachable", "true", 6, 5, "tcp_probe_ok"),
        condition("Replicated", "true", 6, 5, "all_shards_replicated"),
        condition("EndpointReady", "true", 6, 5, "cluster_slots_covered"),
      ],
    },
  ];
}

/** conditions for one cluster, including an expired one, so freshness is visible. */
export function conditions(now: Date): Condition[] {
  return clusters(now)[0]?.conditions ?? [];
}

/**
 * conditionsFor answers for the cluster a screen is showing. A name the fixture
 * does not carry falls back to the first cluster, because a screen that drew
 * nothing would read as healthy.
 */
export function conditionsFor(name: string, now: Date): Condition[] {
  const all = clusters(now);
  return (all.find((cluster) => cluster.name === name || cluster.id === name) ?? all[0])?.conditions ?? [];
}

/* Nodes ------------------------------------------------------------------ */

export interface MockCapacity {
  name: string;
  value: string;
  note: string;
  share: number;
  tone: Tone;
}

export interface MockPlacement {
  name: string;
  role: string;
  mark: Mark;
}

export interface MockNode {
  id: string;
  name: string;
  address: string;
  agent: string;
  /** Seconds since the last report, so the real freshness rules apply. */
  seenAgeSeconds: number;
  state: string;
  stateTone: Tone;
  labels: string[];
  capacity: MockCapacity[];
  placements: MockPlacement[];
  cordonLabel: string;
  actionNote: string;
}

export const nodes: MockNode[] = [
  {
    id: "nd_7f21",
    name: "node-01",
    address: "10.0.4.11:8443",
    agent: "v0.1.0-rc3 · docker 26.1",
    seenAgeSeconds: 3,
    state: "schedulable",
    stateTone: "ok",
    labels: ["zone=rack-a", "disk=nvme", "role=db"],
    capacity: [
      { name: "cpu", value: "6 / 16 cores", note: "38%", share: 38, tone: "ok" },
      { name: "memory", value: "24 / 64 GiB", note: "38%", share: 38, tone: "ok" },
      { name: "volumes", value: "340 / 900 GiB", note: "38%", share: 38, tone: "ok" },
    ],
    placements: [
      { name: "pg-core/i1", role: "primary", mark: ok("running") },
      { name: "pg-billing/i1", role: "stopped", mark: stopped("stopped") },
      { name: "redis-cache/s1", role: "master", mark: ok("running") },
    ],
    cordonLabel: "Cordon",
    actionNote: "Draining moves each placement through its own operation — no bulk eviction.",
  },
  {
    id: "nd_9b04",
    name: "node-02",
    address: "10.0.4.12:8443",
    agent: "v0.1.0-rc3 · docker 26.1",
    seenAgeSeconds: 5,
    state: "schedulable",
    stateTone: "ok",
    labels: ["zone=rack-a", "disk=ssd"],
    capacity: [
      { name: "cpu", value: "4 / 16 cores", note: "25%", share: 25, tone: "ok" },
      { name: "memory", value: "16 / 64 GiB", note: "25%", share: 25, tone: "ok" },
      { name: "volumes", value: "220 / 900 GiB", note: "24%", share: 24, tone: "ok" },
    ],
    placements: [
      { name: "pg-core/i2", role: "replica", mark: ok("running") },
      { name: "redis-cache/s2", role: "master", mark: ok("running") },
    ],
    cordonLabel: "Cordon",
    actionNote:
      "Reservations are durable: freeing capacity requires releasing the reservation, not just stopping a process.",
  },
  {
    id: "nd_3ce8",
    name: "node-03",
    address: "10.0.4.13:8443",
    agent: "v0.1.0-rc3 · docker 26.1",
    seenAgeSeconds: 252,
    state: "unknown",
    stateTone: "neutral",
    labels: ["zone=rack-b", "disk=nvme"],
    capacity: [
      { name: "cpu", value: "? last 5 / 16", note: "rev 19", share: 30, tone: "neutral" },
      { name: "memory", value: "? last 20 / 64", note: "rev 19", share: 31, tone: "neutral" },
      { name: "volumes", value: "? last 410 / 900", note: "rev 19", share: 45, tone: "neutral" },
    ],
    placements: [
      { name: "pg-analytics/i1", role: "unknown", mark: unknown("unknown") },
      { name: "pg-core/i3", role: "unknown", mark: unknown("unknown") },
      { name: "redis-cache/s3", role: "unknown", mark: unknown("unknown") },
    ],
    cordonLabel: "Cordon",
    actionNote:
      "Capacity shown is the last reported reading at rev 19, not a current one. Placement is blocked while unknown; existing placements are not evicted.",
  },
  {
    id: "nd_1a77",
    name: "node-04",
    address: "10.0.4.14:8443",
    agent: "v0.1.0-rc3 · docker 26.1",
    seenAgeSeconds: 2,
    state: "cordoned · draining",
    stateTone: "attn",
    labels: ["zone=rack-b", "disk=hdd", "maintenance=firmware"],
    capacity: [
      { name: "cpu", value: "2 / 8 cores", note: "25%", share: 25, tone: "attn" },
      { name: "memory", value: "8 / 32 GiB", note: "25%", share: 25, tone: "attn" },
      { name: "volumes", value: "80 / 400 GiB", note: "20%", share: 20, tone: "attn" },
    ],
    placements: [{ name: "redis-sess/i1", role: "moving", mark: moving("moving") }],
    cordonLabel: "Uncordon",
    actionNote:
      "op_2d90ff31 is draining this node · 1 of 1 placements remaining. Cancelling leaves moved placements where they are.",
  },
];

/* Cluster detail --------------------------------------------------------- */

export interface MockInstance {
  name: string;
  mark: Mark;
  role: string;
  roleTone: Tone;
  node: string;
  observedRole: string;
  volume: string;
  image: string;
  ports: string;
  usage: string;
  stale?: boolean;
}

export const instances: MockInstance[] = [
  {
    name: "pg-core-i1",
    mark: ok("running"),
    role: "primary",
    roleTone: "ok",
    node: "node-01",
    observedRole: "patroni: leader · 4s ago",
    volume: "vol_4410 · 100 GiB · 38% used",
    image: "sha256:9f21c0…e4b7",
    ports: "5432 → 25432",
    usage: "0.9 / 2 cores · 5.1 / 8 GiB",
  },
  {
    name: "pg-core-i2",
    mark: ok("running"),
    role: "replica",
    roleTone: "active",
    node: "node-02",
    observedRole: "patroni: replica · lag 92s · 11s ago",
    volume: "vol_4411 · 100 GiB · 37% used",
    image: "sha256:9f21c0…e4b7",
    ports: "5432 → 25433",
    usage: "0.4 / 2 cores · 4.4 / 8 GiB",
  },
  {
    name: "pg-core-i3",
    mark: unknown("unknown"),
    role: "unknown",
    roleTone: "neutral",
    node: "node-03",
    observedRole: "? unknown · last replica at rev 19",
    volume: "vol_4412 · 100 GiB · last 41%",
    image: "sha256:9f21c0…e4b7",
    ports: "5432 → 25434",
    usage: "? no reading since 14:02:11Z",
    stale: true,
  },
];

export interface MockEndpoint {
  purpose: string;
  mark: Mark;
  mode: string;
  address: string;
  profile: string;
}

/** Client mode is stated, because the three modes are not interchangeable. */
export const endpoints: MockEndpoint[] = [
  {
    purpose: "read-write",
    mark: ok("available"),
    mode: "single-host",
    address: "pg-core-rw.lab-01.internal:5432",
    profile: "haproxy → current leader · TLS synod-ca/pg-core · failover switches backend, client reconnects",
  },
  {
    purpose: "read-only",
    mark: bad("no backend"),
    mode: "single-host",
    address: "pg-core-ro.lab-01.internal:5432",
    profile: "0 of 2 replicas eligible — lag 92s over threshold, i3 unknown · endpoint returns no backend",
  },
  {
    purpose: "pooled read-write",
    mark: ok("available"),
    mode: "single-host",
    address: "pg-core-pool.lab-01.internal:6432",
    profile: "pgbouncer transaction mode · session features unavailable · TLS synod-ca/pg-core",
  },
];

export interface MockLogical {
  kind: string;
  name: string;
  meta: string;
}

export const logicals: MockLogical[] = [
  { kind: "database", name: "orders", meta: "42 GiB · owner app_orders" },
  { kind: "database", name: "billing_archive", meta: "9 GiB · owner app_billing" },
  { kind: "role", name: "app_orders", meta: "login · no superuser · cred v3" },
  { kind: "role", name: "readonly_bi", meta: "login · pending op_1188" },
];

/* Topology --------------------------------------------------------------- */

export interface MockTopologyNode {
  name: string;
  mark: Mark;
  role: string;
  roleTone: Tone;
  meta: string[];
  link: string;
  linkTone: Tone;
  stale?: boolean;
}

export const topologyNodes: MockTopologyNode[] = [
  {
    name: "pg-core-i1",
    mark: ok("running"),
    role: "primary · node-01",
    roleTone: "ok",
    meta: ["wal 0/3F2A1C08", "connections 24", "✓ observed 4s ago"],
    link: "→ streams to i2, i3",
    linkTone: "ok",
  },
  {
    name: "pg-core-i2",
    mark: ok("running"),
    role: "replica · node-02",
    roleTone: "active",
    meta: ["wal 0/3F1D40A0", "replay lag 92s", "✓ observed 11s ago"],
    link: "✕ lag over 30s threshold — removed from read-only endpoint",
    linkTone: "fail",
  },
  {
    name: "pg-core-i3",
    mark: unknown("unknown"),
    role: "unknown · node-03",
    roleTone: "neutral",
    meta: ["wal ? last 0/3F2A19C4", "lag ? at rev 19", "? no report for 4m 12s"],
    link: "? replication state unknown — not assumed broken, not assumed healthy",
    linkTone: "neutral",
    stale: true,
  },
];

export interface MockVoter {
  label: string;
  mark: Mark;
  stale?: boolean;
}

export const voters: MockVoter[] = [
  { label: "node-01 voter · leader", mark: mark("diamond-open", "ok", "◇", "voting") },
  { label: "node-02 voter", mark: mark("diamond-open", "ok", "◇", "voting") },
  { label: "node-03 voter · unknown 4m", mark: mark("diamond-open", "neutral", "?", "unknown"), stale: true },
];

/** Losing quorum stops Synod changing anything. It does not stop running databases. */
export const quorum = {
  mark: attention("degraded"),
  headline: "2 / 3 — one more loss ends management writes",
  detail: "Losing quorum stops Synod from changing anything. It does not stop the databases already running.",
};

export interface MockDegraded {
  mark: Mark;
  title: string;
  detail: string;
}

export const degraded: MockDegraded[] = [
  {
    mark: bad("false"),
    title: "Replicated false",
    detail:
      "i2 replay lag 92s exceeds the 30s threshold. Read-only endpoint has no eligible backend. Primary is unaffected.",
  },
  {
    mark: attention("degraded"),
    title: "Voter plane degraded 2/3",
    detail:
      "node-03 voter unknown for 4m 12s. Quorum holds. One more loss stops management writes; running databases keep serving.",
  },
];

export interface MockShard {
  name: string;
  mark: Mark;
  meta: string[];
  stale?: boolean;
}

export const shards: MockShard[] = [
  { name: "shard 1", mark: ok("covered"), meta: ["slots 0–5460", "master node-01 · ✓ 1s", "1 replica ✓"] },
  { name: "shard 2", mark: ok("covered"), meta: ["slots 5461–10922", "master node-02 · ✓ 2s", "1 replica ✓"] },
  {
    name: "shard 3",
    mark: unknown("unknown"),
    meta: ["slots 10923–16383", "master node-03 · ? 4m", "1 replica ? unknown"],
    stale: true,
  },
];

/* Backups ---------------------------------------------------------------- */

export const backupPolicy = [
  { key: "schedule", value: "full 02:00Z daily · incr 6h" },
  { key: "target", value: "s3-lab · pgbackrest" },
  { key: "retention", value: "14 full · 30d wal" },
  { key: "last_scheduled", value: "02:00:07Z · 12h ago" },
  { key: "scope", value: "whole cluster" },
];

/** The range is drawn from artifact manifests, not from the schedule. */
export const recoveryRange = {
  from: "Sep 04 02:00Z",
  to: "Sep 18 08:14Z",
  /** Spans in order. A gap is drawn, because recovery is not offered inside one. */
  spans: [
    { share: 3, gap: false },
    { share: 1, gap: true },
    { share: 5, gap: false },
  ],
  gapNote: "1 gap: WAL missing Sep 09 04:11Z → 06:38Z",
  detail: "Point-in-time recovery is not offered inside a gap. The range is drawn from artifact manifests, not from the schedule.",
};

export interface MockBackup {
  id: string;
  source: string;
  kind: string;
  scope: string;
  size: string;
  mark: Mark;
  verification: string;
  taken: string;
}

export const backups: MockBackup[] = [
  {
    id: "bk_9f02",
    source: "pg-core · cl_31d0aa47",
    kind: "full",
    scope: "whole cluster",
    size: "38.4 GiB",
    mark: ok("verified"),
    verification: "restore-verified 6h ago",
    taken: "12h ago",
  },
  {
    id: "bk_9e77",
    source: "pg-core · cl_31d0aa47",
    kind: "incremental",
    scope: "whole cluster",
    size: "1.2 GiB",
    mark: attention("never verified"),
    verification: "never verified",
    taken: "6h ago",
  },
  {
    id: "bk_44e1",
    source: "pg-analytics · cl_88be0c12",
    kind: "full",
    scope: "whole cluster",
    size: "210 GiB",
    mark: attention("never verified"),
    verification: "never verified",
    taken: "38m ago",
  },
  {
    id: "bk_2c18",
    source: "pg-core · orders",
    kind: "pg_dump",
    scope: "one logical database",
    size: "9.8 GiB",
    mark: ok("verified"),
    verification: "checksum-verified 2d ago",
    taken: "2d ago",
  },
  {
    id: "bk_1a05",
    source: "pg-billing · cl_5fd10a93",
    kind: "full",
    scope: "whole cluster",
    size: "? last 44 GiB",
    mark: unknown("unknown"),
    verification: "unknown — target unreachable",
    taken: "12m ago",
  },
];

/* Operations -------------------------------------------------------------- */

export interface MockOperation {
  id: string;
  cluster: string;
  kind: string;
  status: OperationStatus;
  planVersion: number;
  attempts: number;
  what: string;
  age: string;
}

export const operations: MockOperation[] = [
  { id: "op_9a03be71", cluster: "pg-billing", kind: "clusters.stop.v1", status: "failed", planVersion: 2, attempts: 3, what: "clusters.stop.v1 · pg-billing", age: "6m ago" },
  { id: "op_c4f8ab12", cluster: "redis-cache", kind: "clusters.upgrade.v1", status: "waiting", planVersion: 2, attempts: 1, what: "clusters.upgrade.v1 · redis-cache", age: "4m ago" },
  { id: "op_7c14f2a9", cluster: "redis-sess", kind: "clusters.create.v1", status: "running", planVersion: 1, attempts: 1, what: "clusters.create.v1 · redis-sess", age: "12s ago" },
  { id: "op_2d90ff31", cluster: "node-04", kind: "nodes.drain.v1", status: "cancelling", planVersion: 1, attempts: 1, what: "nodes.drain.v1 · node-04", age: "2m ago" },
  { id: "op_6b21c445", cluster: "bk_9f02", kind: "backups.verify.v1", status: "succeeded", planVersion: 1, attempts: 1, what: "backups.verify.v1 · bk_9f02", age: "6h ago" },
  { id: "op_3f10dd82", cluster: "pg-core", kind: "clusters.scale.v1", status: "cancelled", planVersion: 3, attempts: 1, what: "clusters.scale.v1 · pg-core", age: "1d ago" },
];

export interface MockStep {
  ordinal: number;
  name: string;
  glyph: string;
  tone: Tone;
  meta: string;
  note?: string;
}

export const steps: MockStep[] = [
  { ordinal: 1, name: "validate_revision", glyph: "✓", tone: "ok", meta: "0.2s · attempt 1" },
  { ordinal: 2, name: "quiesce_connections", glyph: "✓", tone: "ok", meta: "1.4s · attempt 1" },
  { ordinal: 3, name: "stop_replica_i2", glyph: "✓", tone: "ok", meta: "3.1s · attempt 1" },
  { ordinal: 4, name: "stop_primary_i1", glyph: "✓", tone: "ok", meta: "8.7s · attempt 1" },
  {
    ordinal: 5,
    name: "release_reservation",
    glyph: "✕",
    tone: "fail",
    meta: "3 attempts · 42s",
    note:
      "dependency_unavailable: metadata store unreachable at 14:11:48Z. Reservation still held; the volume and container were left in place deliberately.",
  },
  { ordinal: 6, name: "remove_endpoint_backend", glyph: "·", tone: "dim", meta: "not attempted" },
  { ordinal: 7, name: "verify_postconditions", glyph: "·", tone: "dim", meta: "not attempted" },
];

export interface MockResidual {
  kind: string;
  id: string;
  where: string;
  disposition: string;
}

/** Nothing here is deleted automatically. A nonempty volume is never generic cleanup. */
export const residual: MockResidual[] = [
  { kind: "volume", id: "vol_8812", where: "node-01", disposition: "retained · 40 GiB · data intact" },
  { kind: "container", id: "syn-cl5fd1-i1", where: "node-01", disposition: "stopped · not removed" },
  { kind: "reservation", id: "rsv_0c41", where: "node-01", disposition: "held · capacity not returned" },
];

/* Settings ---------------------------------------------------------------- */

export interface MockComponent {
  name: string;
  label: string;
  mark: Mark;
  meta: string;
  note: string;
  stale?: boolean;
}

export const components: MockComponent[] = [
  {
    name: "controller",
    label: "controller",
    mark: ok("healthy"),
    meta: "v0.1.0-rc3 · uptime 4d 02h",
    note: "Leader on node-01. A controller outage does not stop running databases.",
  },
  {
    name: "backup-target",
    label: "backup target s3-lab",
    mark: bad("unreachable"),
    meta: "last ok 3h 04m ago · 6 attempts",
    note: "BackupCurrent is unknown on 2 clusters. No backup has failed; none can be confirmed.",
  },
  {
    name: "metadata",
    label: "metadata (postgres)",
    mark: ok("healthy"),
    meta: "16.4 · 41 ms p99 · 2.1 GiB",
    note: "Durable control-plane state. Reachable and writable.",
  },
  {
    name: "coordination",
    label: "coordination (etcd)",
    mark: attention("degraded"),
    meta: "3.6 · quorum 2/3 · leader node-01",
    note: "node-03 member unreachable 4m. Quorum holds; one more loss blocks management writes.",
  },
  {
    name: "agents",
    label: "agents",
    mark: unknown("expired"),
    meta: "3 fresh · 1 expired · limit 60s",
    note: "node-03 has not reported since 14:02:11Z. Its placements are not evicted from an unknown report.",
    stale: true,
  },
];

export interface MockTarget {
  name: string;
  kind: string;
  uri: string;
  checked: string;
  mark: Mark;
  note: string;
}

export const targets: MockTarget[] = [
  {
    name: "s3-lab",
    kind: "pgbackrest · s3",
    uri: "s3://lab-backups/synod/lab-01",
    checked: "checked 40s ago",
    mark: bad("unreachable"),
    note: "unreachable — connection refused at 11:10:44Z. Credential metadata only; the access key is never rendered.",
  },
  {
    name: "nfs-cold",
    kind: "pgbackrest · posix",
    uri: "/mnt/cold/synod/lab-01",
    checked: "checked 12s ago",
    mark: ok("writable"),
    note: "writable · 2.1 TiB free · retention applied 02:14Z",
  },
];

export interface MockCredential {
  purpose: string;
  owner: string;
  mark: Mark;
  version: string;
  rotation: string;
  rotationTone: Tone;
  rotated: string;
}

/** Metadata only. No secret value is rendered, stored, or placed in a URL. */
export const credentials: MockCredential[] = [
  {
    purpose: "pg-core · superuser",
    owner: "cl_31d0aa47",
    mark: ok("active"),
    version: "v4 · active",
    rotation: "idle",
    rotationTone: "ok",
    rotated: "9d ago · 14:02Z",
  },
  {
    purpose: "pg-core · app_orders",
    owner: "role app_orders",
    mark: moving("rotating"),
    version: "v3 → v4 · rotating",
    rotation: "op_1188 step 2/3 — old version still accepted",
    rotationTone: "active",
    rotated: "41d ago",
  },
  {
    purpose: "agent mTLS · node-03",
    owner: "nd_3ce8",
    mark: unknown("unconfirmed"),
    version: "v2 · active",
    rotation: "renewal unconfirmed — node unreachable",
    rotationTone: "neutral",
    rotated: "88d ago · expires in 2d",
  },
  {
    purpose: "s3-lab · access key",
    owner: "target s3-lab",
    mark: ok("active"),
    version: "v2 · active",
    rotation: "idle",
    rotationTone: "ok",
    rotated: "120d ago",
  },
];

/* Destructive plans -------------------------------------------------------- */

export interface MockPlan {
  title: string;
  facts: { key: string; value: string; tone?: Tone }[];
  steps: { ordinal: number; text: string; meta: string }[];
  irreversibleTitle: string;
  irreversibleBody: string;
  irreversibleFacts: string[];
  confirmLabel: string;
  token: string;
}

/** The confirmation is bound to a validated plan, and the plan states the facts. */
export const deletePlan: MockPlan = {
  title: "Delete cluster — review validated plan",
  facts: [
    { key: "action", value: "clusters.delete.v1" },
    { key: "target", value: "pg-core · cl_31d0aa47" },
    { key: "expected_revision", value: "14" },
    { key: "members", value: "3 instances · node-01, node-02, node-03" },
    { key: "expected_downtime", value: "permanent — cluster stops serving" },
    { key: "data_disposition", value: "volumes RETAINED as retained storage · 300 GiB across 3 nodes", tone: "attn" },
    { key: "parameters", value: "retain_data=true · tombstone created" },
  ],
  steps: [
    { ordinal: 1, text: "Stop accepting new connections on all three endpoints.", meta: "~2s" },
    { ordinal: 2, text: "Stop replicas i2 and i3, then the primary i1.", meta: "~15s" },
    { ordinal: 3, text: "Remove containers and endpoint backends. Volumes are kept.", meta: "~8s" },
    { ordinal: 4, text: "Write a tombstone and hold it until all three agents acknowledge.", meta: "blocks on node-03 · unknown 4m" },
    { ordinal: 5, text: "Release reservations. Retained volumes stay listed under retained storage.", meta: "~1s" },
  ],
  irreversibleTitle: "Data disposition",
  irreversibleBody:
    "Volumes are retained, not deleted. The cluster, its endpoints, and its credentials are gone and their IDs are never reused. Restoring service means creating a new cluster and restoring into it.",
  irreversibleFacts: [
    "retained: vol_4410, vol_4411, vol_4412 · 300 GiB · listed under retained storage",
    "node-03 is unknown for 4m 12s — the tombstone will stay open until it acknowledges",
  ],
  confirmLabel: "Delete pg-core — returns 202",
  token: "plt_a41c",
};

export const upgradePlan: MockPlan = {
  title: "Major version upgrade — review validated plan",
  facts: [
    { key: "action", value: "clusters.upgrade.v1" },
    { key: "target", value: "pg-core · cl_31d0aa47" },
    { key: "expected_revision", value: "14" },
    { key: "members", value: "3 instances · node-01, node-02, node-03" },
    { key: "expected_downtime", value: "~45s read-write pause at promotion" },
    { key: "data_disposition", value: "in-place catalog rewrite on the primary volume", tone: "attn" },
    { key: "parameters", value: "postgres 15.8 → 16.4 · pg_upgrade --link" },
  ],
  steps: [
    { ordinal: 1, text: "Verify version transition against the qualified matrix and take a pre-upgrade backup.", meta: "~9m" },
    { ordinal: 2, text: "Stop replicas, then the primary. Endpoints return no backend during this window.", meta: "~20s" },
    { ordinal: 3, text: "Run pg_upgrade --link on the primary volume. This writes to the target data directory.", meta: "~45s · point of no return" },
    { ordinal: 4, text: "Start the upgraded primary, rebuild replicas from it, reattach endpoints.", meta: "~6m" },
    { ordinal: 5, text: "Verify postconditions and record the new observed revision.", meta: "~10s" },
  ],
  irreversibleTitle: "No rollback after step 3",
  irreversibleBody:
    "Once pg_upgrade writes to the target data directory, this cluster cannot be returned to 15.8. Recovery from that point is a restore from the pre-upgrade backup into a new cluster — with the data loss implied by its timestamp.",
  irreversibleFacts: [
    "pre-upgrade backup: bk_9f02, 12h old, restore-verified 6h ago",
    "replication is currently broken (Replicated=false) — replicas will be rebuilt from scratch",
  ],
  confirmLabel: "Upgrade pg-core — returns 202",
  token: "plt_a41c",
};

/** The refreshed plan after a revision conflict. The old token is rejected, not approved. */
export const refreshedDeletePlan: MockPlan = {
  ...deletePlan,
  facts: [
    { key: "action", value: "clusters.delete.v1" },
    { key: "target", value: "pg-core · cl_31d0aa47" },
    { key: "expected_revision", value: "15 (was 14 — refreshed)", tone: "attn" },
    { key: "members", value: "2 instances (was 3)", tone: "attn" },
    { key: "expected_downtime", value: "permanent — cluster stops serving" },
    {
      key: "data_disposition",
      value: "volumes RETAINED as retained storage · 200 GiB across 2 nodes (was 300 GiB across 3)",
      tone: "attn",
    },
    { key: "parameters", value: "retain_data=true · tombstone created" },
  ],
  steps: [
    { ordinal: 1, text: "Stop accepting new connections on both remaining endpoints.", meta: "~2s" },
    { ordinal: 2, text: "Stop replica i2, then the primary i1. Instance i3 is already gone.", meta: "~11s" },
    { ordinal: 3, text: "Remove 2 containers and endpoint backends. Volumes are kept.", meta: "~6s" },
    { ordinal: 4, text: "Write a tombstone and hold it until both remaining agents acknowledge.", meta: "node-01, node-02 · both fresh" },
    {
      ordinal: 5,
      text: "Release reservations on node-01 and node-02. Retained volumes stay listed under retained storage.",
      meta: "~1s",
    },
  ],
  irreversibleFacts: [
    "retained: vol_4410, vol_4411 · 200 GiB · listed under retained storage",
    "vol_4412 left this plan with instance i3 — it is already retained storage under the earlier scale operation",
  ],
  confirmLabel: "Confirm refreshed plan — returns 202",
  token: "plt_b9f1 (refreshed)",
};

export const conflictChanged =
  "changed: members 3 → 2 · steps 5 (i3 already removed by op_3f10dd82) · retained volumes 3 → 2";

/* Accounts ---------------------------------------------------------------- */

export interface MockAccount {
  username: string;
  role: string;
  disabled: boolean;
  createdAt: string;
}

export const accounts: MockAccount[] = [
  { username: "root", role: "admin", disabled: false, createdAt: "2026-08-02T09:14:00Z" },
  { username: "ops-day", role: "admin", disabled: false, createdAt: "2026-08-19T16:40:12Z" },
  { username: "ops-night", role: "admin", disabled: true, createdAt: "2026-08-19T16:41:03Z" },
];

/* Create cluster ---------------------------------------------------------- */

export interface MockTopologyOption {
  key: string;
  label: string;
  nodes: string;
  detail: string;
  client: string;
}

/** Only versions in the qualified matrix are offered, so unsupported_spec is unreachable here. */
export const engineOptions = [
  { key: "postgres" as const, label: "PostgreSQL", versions: ["16.4", "15.8"] },
  { key: "redis" as const, label: "Redis", versions: ["7.2"] },
];

export const topologyOptions: Record<"postgres" | "redis", MockTopologyOption[]> = {
  postgres: [
    {
      key: "standalone",
      label: "Standalone",
      nodes: "1 node",
      detail: "One instance, one data directory. No failover, no replica endpoint.",
      client: "client mode: single-host",
    },
    {
      key: "ha3",
      label: "Patroni HA",
      nodes: "3 nodes",
      detail: "1 primary + 2 replicas, HAProxy role routing, PgBouncer pooling. Native failover is Patroni’s, not Synod’s.",
      client: "client mode: single-host via role endpoints",
    },
    {
      key: "ha2",
      label: "Patroni HA, 2 members",
      nodes: "2 nodes",
      detail: "Allowed, but no replica remains if the primary is lost during maintenance.",
      client: "client mode: single-host via role endpoints",
    },
  ],
  redis: [
    {
      key: "standalone",
      label: "Standalone",
      nodes: "1 node",
      detail: "One Redis process. No failover.",
      client: "client mode: single-host",
    },
    {
      key: "sentinel",
      label: "Sentinel",
      nodes: "3 nodes",
      detail: "1 master + 1 replica + 3 Sentinels for quorum. Clients must be Sentinel-aware.",
      client: "client mode: sentinel-aware — plain clients will not follow failover",
    },
    {
      key: "cluster",
      label: "Redis Cluster",
      nodes: "3 nodes",
      detail: "3 shards, 16384 slots split evenly, 1 replica each. Multi-key commands are slot-constrained.",
      client: "client mode: cluster-aware required",
    },
  ],
};
