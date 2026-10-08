// A typed client for the Synod public API.
//
// The session cookie is HttpOnly, so no script can read it. The browser attaches it
// because every request is same-origin. The same origin also makes the browser send
// Sec-Fetch-Site, which the server accepts as the origin proof that an unsafe method needs.

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

export interface Operation {
  id: string;
  app_id: string;
  kind: string;
  status: OperationStatus;
  node_id: string;
  actor_id: string;
  failure_code: string;
  finished_at: string | null;
  created_at: string;
  updated_at: string;
  result?: WorkResult;
}

export interface Node {
  id: string;
  name: string;
  architecture: string;
  runtime_version: string;
  agent_version: string;
  enrolled_at: string;
  approved_at: string | null;
  connected_at: string | null;
  last_seen_at: string | null;
}

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json" };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const response = await fetch(path, {
    method,
    headers,
    signal: AbortSignal.timeout(30000),
    // Same-origin credentials attach the session cookie without exposing it.
    credentials: "same-origin",
    body: body === undefined ? null : JSON.stringify(body),
  });
  if (response.status === 204) return undefined as T;
  const text = await response.text();
  let payload: unknown = undefined;
  if (text.length > 0) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = undefined;
    }
  }
  if (!response.ok) throw new Failure(response.status, asError(payload, response));
  return payload as T;
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


export const api = {
  protection: async () => {
    const plan = await call<ProtectionPlan>("GET", "/v1/protection");
    // The initial plan has no destinations. Go represents that empty slice as null.
    plan.defaults.destinations ??= [];
    for (const item of plan.apps) item.settings.destinations ??= [];
    return plan;
  },
  protect: (body: ProtectionUpdate) => call<{ id: string; revision: number }>("PUT", "/v1/protection", body),
  destinations: () => call<DestinationList>("GET", "/v1/destinations"),
  addDestination: (body: DestinationEntry) => call<Destination>("POST", "/v1/destinations", body),
  removeDestination: (id: string) => call<void>("DELETE", `/v1/destinations/${encodeURIComponent(id)}`),
  submit: (kind: "backups" | "restores" | "proofs", body: WorkSubmission) => call<Operation>("POST", `/v1/${kind}`, body),
  snapshots: (id: string, selection: FileSelection = {}) => call<FileHeader>("GET", fileURL(id, "snapshots", selection)),
  files: (id: string, selection: FileSelection) => call<FileHeader>("GET", fileURL(id, "files", selection)),
  notifications: () => call<NotificationStatus>("GET", "/v1/notifications"),
  configureNotifications: (body: NotificationUpdate) => call<{ id: string; revision: number }>("PUT", "/v1/notifications", body),
  testNotification: (body: { id: string; expected_revision: number }) => call<{ id: string; revision: number }>("POST", "/v1/notifications/test", body),
  nodes: () => call<Node[]>("GET", "/v1/nodes"),
  approveNode: (id: string) => call<Node>("POST", `/v1/nodes/${encodeURIComponent(id)}/actions/approve`, {}),
  operations: () => call<Operation[]>("GET", "/v1/operations"),
  signIn: (username: string, password: string) => call<User>("POST", "/v1/sessions", { username, password }),
  currentSession: () => call<Session>("GET", "/v1/sessions/current"),
  signOut: () => call<void>("DELETE", "/v1/sessions/current"),
  operation: (id: string) => call<Operation>("GET", `/v1/operations/${encodeURIComponent(id)}`),
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

export type Method = "auto" | "stop" | "live" | "dump";
export interface CopySettings {
  destinations: string[];
  method: Method;
  at: string;
  time_zone: string;
  retention: { daily: number; weekly: number; monthly: number };
  include_large?: string[];
}
export interface SavedPath {
  source?: string; target?: string; volume?: string; kind: string;
  status: "Included" | "Large" | "Dumped" | "Never";
  reason: string; size_bytes?: number; size_known?: boolean;
}
export interface App {
  id: string; node_id: string; node_name: string; observed_at: string; present: boolean;
  definition: {
    name: string; kind: string; project_directory?: string; paths?: SavedPath[]; issues?: string[];
    containers?: { id: string; name: string; image: string; platform?: string; state?: string; database?: string }[];
  };
}
export interface WorkResult {
  snapshot_id?: string; destination_id?: string; started_at?: string; completed_at?: string;
  file_bytes?: number; downtime_ms?: number; failure_code?: string; resource?: string;
  safety_snapshot_id?: string; target_app_id?: string; project_directory?: string;
  recovery_pending?: string[]; restart_pending?: string[]; warning_codes?: string[];
  consistency?: "unverified" | "filesystem_snapshot";
  contents?: number; read?: number; bytes_read?: number; containers?: number;
  maintenance?: "completed" | "claimed" | "handing_over";
}
export interface ProofStatus { id: string; node_id: string; destination_id?: string; state: OperationStatus; result: WorkResult }
export interface AppPlan {
  app: App; enabled: boolean; settings_current: boolean; settings: CopySettings;
  state: string; blocked_code?: string; only_on_source: boolean; data_loss_window_seconds?: number;
  restore_tested_at?: string; last_drill?: ProofStatus;
  limit_codes?: string[];
  targets: { destination_id: string; selected: boolean; state: string; due_at?: string; last_operation_id?: string;
    failure_code?: string; last_good?: WorkResult; data_loss_window_seconds?: number; limit_codes?: string[] }[];
}
export interface ProtectionPlan { revision: number; auto_protect: boolean; defaults: CopySettings; apps: AppPlan[] }
export interface ProtectionUpdate { id: string; expected_revision: number; all: boolean; app_ids?: string[]; enabled: boolean; settings: CopySettings }
export interface Destination {
  id: string; name: string; node_id?: string; created_at?: string;
  location: { kind: "device" | "local" | "sftp" | "s3"; path?: string;
    device?: { node_id: string; destination_id: string };
    sftp?: { host: string; port: number; user: string; known_hosts: string };
    s3?: { endpoint: string; bucket: string; prefix?: string; region?: string } };
}
export interface DestinationEntry {
  destination: Destination;
  credentials: { password: string; ssh_key?: string; access_key_id?: string; secret_access_key?: string; session_token?: string };
}
export interface DestinationList {
  revision: number; destinations: Destination[];
  delivery: { node_id: string; node_name: string; current: boolean; acknowledged_at?: string }[];
  checks?: ProofStatus[];
  maintenance: { destination_id: string; node_id?: string; last?: ProofStatus }[];
  space: { destination_id: string; known: boolean; observed_at?: string; total_bytes: number; available_bytes: number }[];
}
export interface Snapshot {
  id: string; destination_ids: string[]; source_node_id: string; source_node_name: string; architecture: string;
  method: Method; started_at: string; completed_at: string; file_bytes: number; paths: string[];
  databases?: string[]; exclusions?: { source: string; reason: string; status?: string }[];
}
export interface FileHeader {
  path_type?: string;
  snapshot_id?: string; destination_id?: string; snapshots?: Snapshot[];
  entries?: { name: string; type: string; size: number; target?: string; mod_time?: string }[];
  truncated?: boolean; warning_codes?: string[];
}
export interface FileSelection { snapshot?: string; destination?: string; via?: string; path?: string }
export interface WorkSubmission {
  id: string; app_id?: string; node_id?: string; destination_id?: string; snapshot_id?: string;
  method?: Method; replace?: boolean; kind?: "integrity_check" | "restore_drill";
}
export interface NotificationSettings { url?: string; overdue_grace_hours: number; missing_after_hours: number; full_percent: number }
export interface NotificationUpdate { id: string; expected_revision: number; settings: NotificationSettings }
export interface NotificationStatus {
  revision: number; settings: Omit<NotificationSettings, "url"> & { configured: boolean; service?: string };
  alerts: { id: string; kind: string; operation_id?: string; message: string; state: string; attempts: number;
    created_at: string; delivered_at?: string; failure_code?: string }[];
}

// A lost response can follow a committed write. Keep the same body and ID for its retry.
export function uncertain(error: unknown): boolean {
  return !(error instanceof Failure) || error.status >= 500 || error.detail.code === "outcome_unknown";
}

export function fileURL(appID: string, action: "snapshots" | "files" | "files/content", selection: FileSelection = {}): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(selection)) if (value) query.set(key, value);
  return `/v1/apps/${encodeURIComponent(appID)}/${action}?${query}`;
}

export async function download(appID: string, selection: FileSelection, signal: AbortSignal): Promise<{ blob: Blob; archive: boolean }> {
  const response = await fetch(fileURL(appID, "files/content", selection) + "&verified=1", { credentials: "same-origin", signal });
  if (!response.ok) throw new Failure(response.status, asError(await response.json().catch(() => undefined), response));
  if (response.headers.get("X-Synod-Transfer") !== "complete") {
    throw new Failure(502, { code: "file_transfer_incomplete", message: "The download was not verified. No file was saved. Retry the download.", requestID: response.headers.get("X-Request-ID") ?? "" });
  }
  const blob = await response.blob();
  const length = response.headers.get("Content-Length");
  if (length === null || blob.size !== Number(length)) {
    throw new Failure(502, { code: "file_transfer_incomplete", message: "The download was interrupted. No file was saved. Retry the download.", requestID: response.headers.get("X-Request-ID") ?? "" });
  }
  return { blob, archive: response.headers.get("X-Synod-Format") === "tar" };
}
