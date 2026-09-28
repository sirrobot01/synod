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
  plan_version: number;
  desired_revision: number;
  attempts: number;
  failure_code: string | null;
  cancellation_requested_at: string | null;
  finished_at: string | null;
  created_at: string;
  updated_at: string;
}

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json" };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const response = await fetch(path, {
    method,
    headers,
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
