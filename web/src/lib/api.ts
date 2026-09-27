// A typed client for the Synod public API.
//
// The session cookie is HttpOnly, so no script can read it. The browser attaches it
// because every request is same-origin. Explicit database disclosures stay in the
// calling component's memory. The same
// origin also makes the browser send Sec-Fetch-Site, which the server accepts as the
// origin proof that an unsafe method needs.

import type { APIError, OperationStatus } from "./state";

/** Failure carries the server's stable code so a caller can branch on it. */
export class Failure extends Error {
  readonly status: number;
  readonly detail: APIError;

  constructor(status: number, detail: APIError) {
    super(detail.code);
    this.name = "Failure";
    this.status = status;
    this.detail = detail;
  }
}

export interface Session {
  actor_id: string;
  role: string;
  user_id: string;
}

export interface User {
  id: string;
  username: string;
  role: string;
  disabled: boolean;
}

export interface APIToken {
  id: string;
  actor_id: string;
  name: string;
  role: string;
  created_at: string;
  last_used_at: string | null;
  expires_at: string | null;
  revoked_at: string | null;
}

/** A newly created token. The plaintext appears once and is never stored. */
export interface MintedToken extends APIToken {
  token: string;
}

export type ClusterSpec = { schema_version: 1; name: string } & (
  | { engine: "postgres"; postgresql: { version: string; topology: "standalone" }; redis: null }
  | { engine: "redis"; postgresql: null; redis: {
      version: string; topology: "standalone"; persistence: "none" | "rdb" | "aof" | "rdb+aof";
      eviction_policy?: "noeviction" | "allkeys-lru" | "allkeys-lfu" | "allkeys-random" | "volatile-lru" | "volatile-lfu" | "volatile-random" | "volatile-ttl";
    } }
);

/** Cluster projects stored fields used by the web UI. It omits the optional Redis int64 byte limit. */
export interface Cluster {
  id: string;
  installation_id: string;
  spec: ClusterSpec;
  desired_revision: number;
  desired_lifecycle: "running" | "stopped" | "deleted";
  desired_replicas: number;
  observed_revision: number;
  observed_lifecycle: "provisioning" | "running" | "stopped" | "deleting" | "deleted" | "unknown";
  execution_epoch: number;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
}

export interface ClusterPage {
  items: Cluster[];
  next_cursor: string;
}

export interface Operation {
  id: string;
  cluster_id: string;
  kind: string;
  status: OperationStatus;
  plan_version: number;
  desired_revision: number;
  attempts: number;
  failure_code: string | null;
  cancellation_requested_at: string | null;
  finished_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface Admission {
  cluster_id: string;
  operation_id: string;
  desired_revision: number;
  accepted_at: string;
  replayed: boolean;
}

export type ClusterAction = "start" | "stop" | "delete" | "recover-setup";
export type CredentialPurpose = "application" | "superuser" | "replication";

export interface ActionPlan {
  plan_token: string;
  cluster_id: string;
  cluster_name: string;
  action: "delete";
  expected_revision: number;
  data_disposition: "retain";
  retained_volumes: string[];
  downtime: string;
  expires_at: string;
}

export interface ActionConfirmation {
  plan_token: string;
  confirm: true;
  data_disposition: "retain";
}

export interface ScaleDownPlan extends Omit<ActionPlan, "action"> {
  action: "scale_down";
  member_id: string;
}

export interface SetupRecoveryPlan extends Omit<ActionPlan, "action"> {
  action: "recover_setup";
  member_id: string;
  resource_id: string;
}

/** ClusterMember reports stored membership, not a current native role observation. */
export interface ClusterMember {
  id: string;
  role: "primary" | "replica";
  state: "pending" | "active" | "residual";
}

export interface Endpoint {
  id: string;
  node_id: string;
  host: string;
  port: number;
  transport: "tcp";
  database: string;
  client_mode: "direct";
  availability_profile: "single_node";
  tls_mode: "disabled";
}

export interface EndpointInspection {
  cluster_id: string;
  engine: "postgres" | "redis";
  desired_revision: number;
  observed_revision: number;
  desired_lifecycle: "running" | "stopped";
  observed_lifecycle: "provisioning" | "running" | "stopped" | "deleting" | "deleted" | "unknown";
  endpoints: Endpoint[];
  unavailable_reason?: string;
}

export interface CredentialMetadata {
  credential_id: string;
  cluster_id: string;
  version: number;
  purpose: CredentialPurpose;
  username: string;
}

export interface CredentialInspection {
  cluster_id: string;
  credentials: CredentialMetadata[];
}

export interface RevealedCredential {
  credential: CredentialMetadata;
  password: string;
}

type SensitiveFlow = "metadata" | "mutation" | "reveal";

async function call<T>(method: string, path: string, body?: unknown, key?: string,
  options?: { flow: SensitiveFlow; status: number; revision?: number }): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json" };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (key !== undefined) headers["Idempotency-Key"] = key;
  if (options?.revision !== undefined) headers["If-Match"] = `"${options.revision}"`;
  const response = await fetch(path, {
    method,
    headers,
    // Same-origin credentials attach the session cookie without exposing it.
    credentials: "same-origin",
    ...(options ? { cache: "no-store" as const, redirect: "error" as const } : {}),
    body: body === undefined ? null : JSON.stringify(body),
  }).catch((error: unknown) => {
    if (options) throw safeFailure(0, "transport_error", options.flow);
    throw error;
  });
  if (response.status === 204 && !options) return undefined as T;
  const text = await response.text().catch((error: unknown) => {
    if (options) throw safeFailure(response.status, "transport_error", options.flow);
    throw error;
  });
  let payload: unknown = undefined;
  if (text.length > 0) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = undefined;
    }
  }
  if (!response.ok) {
    if (options) {
      const record = object(payload) ? payload : {};
      const code = typeof record.code === "string" && Object.hasOwn(safeMessages, record.code) ? record.code : "api_error";
      const failure = safeFailure(response.status, code, options.flow);
      const requestID = record.request_id ?? response.headers.get("X-Request-ID");
      if (uuid(requestID)) failure.detail.requestID = requestID;
      if (uuid(record.operation_id)) failure.detail.operationID = record.operation_id;
      throw failure;
    }
    const detail = asError(payload, response);
    throw new Failure(response.status, detail);
  }
  if (options && response.status !== options.status) throw safeFailure(response.status, "invalid_response", options.flow);
  return payload as T;
}

const safeMessages: Record<string, string> = {
  unauthenticated: "Sign in again.", forbidden: "This account may not perform this action.",
  account_disabled: "This account is disabled.", cross_origin: "Reload this page before trying again.",
  invalid_request: "The request was rejected.", unsupported_spec: "The requested configuration is unsupported.",
  idempotency_conflict: "The retry key belongs to a different request.", name_conflict: "That cluster name is in use.",
  operation_conflict: "The cluster cannot accept this action in its current state.",
  revision_conflict: "The cluster revision changed. Refresh its state before a new action.",
  confirmation_required: "Fetch a fresh action preview and confirm it again.",
  not_found: "The requested resource is unavailable.", outcome_unknown: "The request outcome is uncertain.",
  dependency_unavailable: "A required service is unavailable.", internal_error: "The API could not complete the request.",
  request_too_large: "The request is too large.", unsupported_media_type: "The request format is unsupported.",
  method_not_allowed: "The requested action is unavailable.", rate_limited: "Too many requests. Wait before trying again.",
  api_error: "The API rejected the request.", transport_error: "The API response could not be received.",
  invalid_response: "The API returned an invalid response.",
};

function safeFailure(status: number, code: string, flow: SensitiveFlow): Failure {
  let message = safeMessages[code] ?? safeMessages.api_error!;
  if (["transport_error", "outcome_unknown", "invalid_response"].includes(code)) {
    if (flow === "mutation") message += " Keep the exact target, key, revision, and body for an explicit retry.";
    if (flow === "reveal") message = "Credential access authorization is uncertain. No password was returned.";
  }
  return new Failure(status, { code, message, requestID: "" });
}

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function uuid(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value) && value !== "00000000-0000-0000-0000-000000000000";
}

function positive(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function timestamp(value: unknown): value is string {
  if (typeof value !== "string" || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?(?:Z|[+-]\d\d:\d\d)$/.test(value) ||
    !Number.isFinite(Date.parse(value)) || value.startsWith("0001-")) return false;
  // Date.parse normalizes impossible calendar dates such as February 31.
  return new Date(`${value.slice(0, 19)}Z`).toISOString().slice(0, 19) === value.slice(0, 19);
}

function purpose(value: unknown): value is CredentialPurpose {
  return value === "application" || value === "superuser" || value === "replication";
}

function credential(value: unknown, id: string, flow: SensitiveFlow): CredentialMetadata {
  if (!object(value) || value.cluster_id !== id || !uuid(value.credential_id) || !positive(value.version) ||
    !purpose(value.purpose) || typeof value.username !== "string" || !/^[a-z][a-z0-9_]{0,62}$/.test(value.username)) {
    throw safeFailure(200, "invalid_response", flow);
  }
  return { cluster_id: id, credential_id: value.credential_id, version: value.version, purpose: value.purpose, username: value.username };
}

function clientAddress(value: unknown): value is string {
  if (typeof value !== "string") return false;
  try {
    if (value.includes(":")) {
      const address = new URL(`http://[${value}]/`).hostname;
      const first = Number.parseInt(value.split(":")[0] || "0", 16);
      return address === `[${value}]` && value !== "::" && value !== "::1" && !/^::ffff:[0-9a-f]+:[0-9a-f]+$/.test(value) &&
        (first & 0xff00) !== 0xff00 && (first & 0xffc0) !== 0xfe80;
    }
    if (!/^\d+\.\d+\.\d+\.\d+$/.test(value) || new URL(`http://${value}/`).hostname !== value) return false;
    const [first = 0, second = 0] = value.split(".").map(Number);
    return value !== "0.0.0.0" && value !== "255.255.255.255" && first !== 127 &&
      !(first === 169 && second === 254) && !(first >= 224 && first <= 239);
  } catch { return false; }
}

function asError(payload: unknown, response: Response): APIError {
  const record = (payload ?? {}) as Record<string, unknown>;
  const detail: APIError = {
    code: typeof record["code"] === "string" ? record["code"] : "transport_error",
    message: typeof record["message"] === "string" ? record["message"] : `The API answered ${response.status}.`,
    requestID: typeof record["request_id"] === "string" ? record["request_id"] : (response.headers.get("X-Request-ID") ?? ""),
  };
  if (typeof record["operation_id"] === "string" && record["operation_id"] !== "") detail.operationID = record["operation_id"];
  return detail;
}

// Project only stored fields. Extra reply fields must not enter list state.
function listedCluster(value: unknown): Cluster | null {
  if (!object(value) || !uuid(value.id) || !uuid(value.installation_id) ||
    typeof value.desired_replicas !== "number" || !Number.isSafeInteger(value.desired_replicas) || value.desired_replicas < 0 || value.desired_replicas > 1 ||
    !positive(value.desired_revision) || !Number.isSafeInteger(value.observed_revision) ||
    typeof value.observed_revision !== "number" || value.observed_revision < 0 ||
    !Number.isSafeInteger(value.execution_epoch) || typeof value.execution_epoch !== "number" || value.execution_epoch < 0 ||
    typeof value.desired_lifecycle !== "string" || !["running", "stopped", "deleted"].includes(value.desired_lifecycle) ||
    typeof value.observed_lifecycle !== "string" || !["provisioning", "running", "stopped", "deleting", "deleted", "unknown"].includes(value.observed_lifecycle) ||
    !timestamp(value.created_at) || !timestamp(value.updated_at) ||
    (value.deleted_at !== null && !timestamp(value.deleted_at)) || !object(value.spec)) return null;
  const spec = value.spec;
  if (spec.schema_version !== 1 || typeof spec.name !== "string" || !/^[a-z][a-z0-9-]{0,62}$/.test(spec.name)) return null;
  const engineSpec = spec.engine === "postgres" ? spec.postgresql : spec.redis;
  if (!object(engineSpec) || engineSpec.topology !== "standalone" || typeof engineSpec.version !== "string" ||
    engineSpec.version.length > 64 || !/^[0-9]+\.[0-9]+(?:\.[0-9]+)?$/.test(engineSpec.version)) return null;
  let projected: ClusterSpec;
  if (spec.engine === "postgres" && spec.redis === null) {
    projected = { schema_version: 1, name: spec.name, engine: "postgres", redis: null,
      postgresql: { version: engineSpec.version, topology: "standalone" } };
  } else if (spec.engine === "redis" && spec.postgresql === null) {
    if (typeof engineSpec.persistence !== "string" || !["none", "rdb", "aof", "rdb+aof"].includes(engineSpec.persistence) ||
      (engineSpec.eviction_policy !== undefined && (typeof engineSpec.eviction_policy !== "string" || !["noeviction", "allkeys-lru", "allkeys-lfu", "allkeys-random",
        "volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl"].includes(engineSpec.eviction_policy)))) return null;
    projected = { schema_version: 1, name: spec.name, engine: "redis", postgresql: null,
      redis: { version: engineSpec.version, topology: "standalone", persistence: engineSpec.persistence as NonNullable<ClusterSpec["redis"]>["persistence"],
        ...(engineSpec.eviction_policy === undefined ? {} : { eviction_policy: engineSpec.eviction_policy as NonNullable<NonNullable<ClusterSpec["redis"]>["eviction_policy"]> }) } };
  } else return null;
  return { id: value.id, installation_id: value.installation_id, spec: projected,
    desired_replicas: value.desired_replicas,
    desired_revision: value.desired_revision, desired_lifecycle: value.desired_lifecycle as Cluster["desired_lifecycle"],
    observed_revision: value.observed_revision, observed_lifecycle: value.observed_lifecycle as Cluster["observed_lifecycle"],
    execution_epoch: value.execution_epoch, created_at: value.created_at, updated_at: value.updated_at, deleted_at: value.deleted_at };
}

export const api = {
  signIn: (username: string, password: string) => call<User>("POST", "/v1/sessions", { username, password }),
  currentSession: () => call<Session>("GET", "/v1/sessions/current"),
  signOut: () => call<void>("DELETE", "/v1/sessions/current"),
  async clusters(cursor = "", includeDeleted = false): Promise<ClusterPage> {
    if (typeof cursor !== "string" || new TextEncoder().encode(cursor).length > 1024 || typeof includeDeleted !== "boolean") {
      throw safeFailure(0, "invalid_request", "metadata");
    }
    const query = new URLSearchParams({ limit: "50", include_deleted: String(includeDeleted) });
    if (cursor !== "") query.set("cursor", cursor);
    const value = await call<unknown>("GET", `/v1/clusters?${query}`, undefined, undefined, { flow: "metadata", status: 200 });
    if (!object(value) || !Array.isArray(value.items) || value.items.length > 50 ||
      typeof value.next_cursor !== "string" || new TextEncoder().encode(value.next_cursor).length > 1024 ||
      (value.next_cursor !== "" && (value.next_cursor === cursor || value.items.length !== 50))) {
      throw safeFailure(200, "invalid_response", "metadata");
    }
    const items: Cluster[] = [];
    const ids = new Set<string>();
    for (const item of value.items) {
      const cluster = listedCluster(item);
      if (!cluster || (!includeDeleted && cluster.deleted_at !== null) || ids.has(cluster.id) ||
        (items.length > 0 && items[0]!.installation_id !== cluster.installation_id)) throw safeFailure(200, "invalid_response", "metadata");
      ids.add(cluster.id);
      items.push(cluster);
    }
    return { items, next_cursor: value.next_cursor };
  },
  async cluster(id: string): Promise<Cluster> {
    if (!uuid(id)) throw safeFailure(0, "invalid_request", "metadata");
    const value = await call<unknown>("GET", `/v1/clusters/${id}`, undefined, undefined, { flow: "metadata", status: 200 });
    const found = listedCluster(value);
    if (!found || found.id !== id) throw safeFailure(200, "invalid_response", "metadata");
    return found;
  },
  operation: (id: string) => call<Operation>("GET", `/v1/operations/${encodeURIComponent(id)}`),
  createCluster: (spec: unknown, key: string) => call<Admission>("POST", "/v1/clusters", spec, key),
  async clusterMembers(id: string): Promise<ClusterMember[]> {
    if (!uuid(id)) throw safeFailure(0, "invalid_request", "metadata");
    const value = await call<unknown>("GET", `/v1/clusters/${id}/members`, undefined, undefined, { flow: "metadata", status: 200 });
    if (!object(value) || !Array.isArray(value.members) || value.members.length > 3) throw safeFailure(200, "invalid_response", "metadata");
    const members: ClusterMember[] = [];
    for (const member of value.members) {
      if (!object(member) || !uuid(member.id) || (member.role !== "primary" && member.role !== "replica") ||
        (member.state !== "pending" && member.state !== "active" && member.state !== "residual") ||
        members.some((saved) => saved.id === member.id || (saved.role === "primary" && member.role === "primary")) ||
        (member.role === "replica" && members.filter((saved) => saved.role === "replica").length >= 2)) throw safeFailure(200, "invalid_response", "metadata");
      members.push({ id: member.id, role: member.role, state: member.state });
    }
    return members;
  },
  async planScaleDown(id: string): Promise<ScaleDownPlan> {
    if (!uuid(id)) throw safeFailure(0, "invalid_request", "metadata");
    const value = await call<unknown>("POST", `/v1/clusters/${id}/action-plans`, { action: "scale_down" }, undefined, { flow: "metadata", status: 200 });
    if (!object(value) || value.cluster_id !== id || !uuid(value.plan_token) || !uuid(value.member_id) ||
      typeof value.cluster_name !== "string" || !/^[a-z][a-z0-9-]{0,62}$/.test(value.cluster_name) ||
      value.action !== "scale_down" || value.data_disposition !== "retain" || !positive(value.expected_revision) ||
      !Array.isArray(value.retained_volumes) || !value.retained_volumes.every(uuid) || new Set(value.retained_volumes).size !== value.retained_volumes.length ||
      typeof value.downtime !== "string" || value.downtime.length === 0 || !timestamp(value.expires_at)) throw safeFailure(200, "invalid_response", "metadata");
    return { plan_token: value.plan_token, cluster_id: id, cluster_name: value.cluster_name, action: "scale_down", member_id: value.member_id,
      expected_revision: value.expected_revision, data_disposition: "retain", retained_volumes: [...value.retained_volumes],
      downtime: value.downtime, expires_at: value.expires_at };
  },
  async planSetupRecovery(id: string): Promise<SetupRecoveryPlan> {
    if (!uuid(id)) throw safeFailure(0, "invalid_request", "metadata");
    const action = "recover_setup";
    const value = await call<unknown>("POST", `/v1/clusters/${id}/action-plans`, { action }, undefined, { flow: "metadata", status: 200 });
    if (!object(value) || value.cluster_id !== id || !uuid(value.plan_token) || !uuid(value.member_id) || !uuid(value.resource_id) ||
      typeof value.cluster_name !== "string" || !/^[a-z][a-z0-9-]{0,62}$/.test(value.cluster_name) ||
      value.action !== action || value.data_disposition !== "retain" || !positive(value.expected_revision) ||
      !Array.isArray(value.retained_volumes) || value.retained_volumes.length !== 1 || !value.retained_volumes.every(uuid) ||
      typeof value.downtime !== "string" || value.downtime.length === 0 || !timestamp(value.expires_at)) throw safeFailure(200, "invalid_response", "metadata");
    return { plan_token: value.plan_token, cluster_id: id, cluster_name: value.cluster_name, action, member_id: value.member_id, resource_id: value.resource_id,
      expected_revision: value.expected_revision, data_disposition: "retain", retained_volumes: [...value.retained_volumes],
      downtime: value.downtime, expires_at: value.expires_at };
  },
  async scaleCluster(id: string, replicas: 0 | 1, expectedRevision: number, key: string, confirmation?: ActionConfirmation): Promise<Admission> {
    if (!uuid(id) || (replicas !== 0 && replicas !== 1) || !positive(expectedRevision) || !/^[!-~]{1,128}$/.test(key) ||
      (replicas === 0 ? !confirmation || !uuid(confirmation.plan_token) || confirmation.confirm !== true || confirmation.data_disposition !== "retain" : confirmation !== undefined)) {
      throw safeFailure(0, "invalid_request", "mutation");
    }
    const body = confirmation ? { replicas, plan_token: confirmation.plan_token, confirm: true, data_disposition: "retain" } : { replicas };
    const value = await call<unknown>("POST", `/v1/clusters/${id}/actions/scale`, body, key, { flow: "mutation", status: 202, revision: expectedRevision });
    if (!object(value) || value.cluster_id !== id || !uuid(value.operation_id) || !positive(value.desired_revision) ||
      !timestamp(value.accepted_at) || typeof value.replayed !== "boolean") throw safeFailure(202, "invalid_response", "mutation");
    return { cluster_id: id, operation_id: value.operation_id, desired_revision: value.desired_revision,
      accepted_at: value.accepted_at, replayed: value.replayed };
  },
  async planDelete(id: string): Promise<ActionPlan> {
    if (!uuid(id)) throw safeFailure(0, "invalid_request", "metadata");
    const value = await call<unknown>("POST", `/v1/clusters/${id}/action-plans`, { action: "delete" }, undefined, { flow: "metadata", status: 200 });
    if (!object(value) || value.cluster_id !== id || !uuid(value.plan_token) ||
      typeof value.cluster_name !== "string" || !/^[a-z][a-z0-9-]{0,62}$/.test(value.cluster_name) ||
      value.action !== "delete" || value.data_disposition !== "retain" || !positive(value.expected_revision) ||
      !Array.isArray(value.retained_volumes) || !value.retained_volumes.every(uuid) ||
      new Set(value.retained_volumes).size !== value.retained_volumes.length ||
      typeof value.downtime !== "string" || value.downtime.length === 0 || !timestamp(value.expires_at)) {
      throw safeFailure(200, "invalid_response", "metadata");
    }
    return { plan_token: value.plan_token, cluster_id: id, cluster_name: value.cluster_name, action: "delete",
      expected_revision: value.expected_revision, data_disposition: "retain", retained_volumes: [...value.retained_volumes],
      downtime: value.downtime, expires_at: value.expires_at };
  },
  async actOnCluster(id: string, action: ClusterAction, expectedRevision: number, key: string, confirmation?: ActionConfirmation): Promise<Admission> {
    if (!uuid(id) || !["start", "stop", "delete", "recover-setup"].includes(action) || !positive(expectedRevision) || !/^[!-~]{1,128}$/.test(key) ||
      (["delete", "recover-setup"].includes(action) ? !confirmation || !uuid(confirmation.plan_token) || confirmation.confirm !== true || confirmation.data_disposition !== "retain" : confirmation !== undefined)) {
      throw safeFailure(0, "invalid_request", "mutation");
    }
    const body = confirmation ? { plan_token: confirmation.plan_token, confirm: true, data_disposition: "retain" } : {};
    const value = await call<unknown>(action === "delete" ? "DELETE" : "POST",
      action === "delete" ? `/v1/clusters/${id}` : `/v1/clusters/${id}/actions/${action}`, body, key,
      { flow: "mutation", status: 202, revision: expectedRevision });
    if (!object(value) || value.cluster_id !== id || !uuid(value.operation_id) || !positive(value.desired_revision) ||
      !timestamp(value.accepted_at) || typeof value.replayed !== "boolean") throw safeFailure(202, "invalid_response", "mutation");
    return { cluster_id: id, operation_id: value.operation_id, desired_revision: value.desired_revision,
      accepted_at: value.accepted_at, replayed: value.replayed };
  },
  async clusterEndpoints(id: string): Promise<EndpointInspection> {
    if (!uuid(id)) throw safeFailure(0, "invalid_request", "metadata");
    const value = await call<unknown>("GET", `/v1/clusters/${id}/endpoints`, undefined, undefined, { flow: "metadata", status: 200 });
    if (!object(value) || value.cluster_id !== id || (value.engine !== "postgres" && value.engine !== "redis") ||
      !positive(value.desired_revision) || typeof value.observed_revision !== "number" || !Number.isSafeInteger(value.observed_revision) || value.observed_revision < 0 ||
      (value.desired_lifecycle !== "running" && value.desired_lifecycle !== "stopped") ||
      typeof value.observed_lifecycle !== "string" || !["provisioning", "running", "stopped", "deleting", "deleted", "unknown"].includes(value.observed_lifecycle) ||
      !Array.isArray(value.endpoints) || value.endpoints.length > 1) throw safeFailure(200, "invalid_response", "metadata");
    const result: EndpointInspection = { cluster_id: id, engine: value.engine, desired_revision: value.desired_revision,
      observed_revision: value.observed_revision, desired_lifecycle: value.desired_lifecycle,
      observed_lifecycle: value.observed_lifecycle as EndpointInspection["observed_lifecycle"], endpoints: [] };
    if (value.endpoints.length === 0) {
      if (typeof value.unavailable_reason !== "string" || !["not_allocated", "client_address_required", "address_not_bound", "ambiguous_allocation"].includes(value.unavailable_reason)) {
        throw safeFailure(200, "invalid_response", "metadata");
      }
      result.unavailable_reason = value.unavailable_reason;
      return result;
    }
    const endpoint: unknown = value.endpoints[0];
    if ((value.unavailable_reason !== undefined && value.unavailable_reason !== "") || !object(endpoint) || !uuid(endpoint.id) || !uuid(endpoint.node_id) ||
      !clientAddress(endpoint.host) || !positive(endpoint.port) || endpoint.port > 65535 || endpoint.transport !== "tcp" ||
      endpoint.database !== (value.engine === "postgres" ? "app" : "0") || endpoint.client_mode !== "direct" ||
      endpoint.availability_profile !== "single_node" || endpoint.tls_mode !== "disabled") throw safeFailure(200, "invalid_response", "metadata");
    result.endpoints.push({ id: endpoint.id, node_id: endpoint.node_id, host: endpoint.host, port: endpoint.port, transport: "tcp",
      database: value.engine === "postgres" ? "app" : "0", client_mode: "direct", availability_profile: "single_node", tls_mode: "disabled" });
    return result;
  },
  async clusterCredentials(id: string): Promise<CredentialInspection> {
    if (!uuid(id)) throw safeFailure(0, "invalid_request", "metadata");
    const value = await call<unknown>("GET", `/v1/clusters/${id}/credentials`, undefined, undefined, { flow: "metadata", status: 200 });
    if (!object(value) || value.cluster_id !== id || !Array.isArray(value.credentials) || value.credentials.length > 3) throw safeFailure(200, "invalid_response", "metadata");
    const credentials = value.credentials.map((entry: unknown) => credential(entry, id, "metadata"));
    if (credentials.some((entry, index) => index > 0 && entry.purpose <= credentials[index - 1]!.purpose)) throw safeFailure(200, "invalid_response", "metadata");
    return { cluster_id: id, credentials };
  },
  async revealClusterCredential(id: string, requestedPurpose: CredentialPurpose): Promise<RevealedCredential> {
    if (!uuid(id) || !purpose(requestedPurpose)) throw safeFailure(0, "invalid_request", "reveal");
    const value = await call<unknown>("POST", `/v1/clusters/${id}/credentials/${requestedPurpose}/reveal`, {}, undefined, { flow: "reveal", status: 200 });
    if (!object(value) || typeof value.password !== "string" || value.password.length === 0) throw safeFailure(200, "invalid_response", "reveal");
    const metadata = credential(value.credential, id, "reveal");
    if (metadata.purpose !== requestedPurpose) throw safeFailure(200, "invalid_response", "reveal");
    return { credential: metadata, password: value.password };
  },
  tokens: () => call<APIToken[]>("GET", "/v1/tokens"),
  createToken: (name: string, actorID: string, expiresAt: string | null) =>
    call<MintedToken>("POST", "/v1/tokens", { name, actor_id: actorID, expires_at: expiresAt }),
  revokeToken: (id: string) => call<void>("POST", `/v1/tokens/${encodeURIComponent(id)}/actions/revoke`),
  createUser: (username: string, password: string) => call<User>("POST", "/v1/users", { username, password }),
  disableUser: (id: string) => call<void>("POST", `/v1/users/${encodeURIComponent(id)}/actions/disable`),
  setPassword: (id: string, currentPassword: string, password: string) =>
    call<void>("POST", `/v1/users/${encodeURIComponent(id)}/actions/set-password`, { current_password: currentPassword, password }),
};

/**
 * detail reads any thrown value as an API failure, so a screen never has to
 * decide what a transport error looks like. A failure that never reached the
 * server carries no request identity, and saying so is honest.
 */
export function detail(error: unknown): APIError {
  if (error instanceof Failure) return error.detail;
  return { code: "transport_error", message: "The API could not be reached.", requestID: "" };
}
