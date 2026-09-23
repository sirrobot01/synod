import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api, detail, Failure, type ActionConfirmation, type ActionPlan, type CredentialMetadata, type Cluster, type EndpointInspection } from "./api";

const id = "11111111-1111-4111-8111-111111111111";
const other = "22222222-2222-4222-8222-222222222222";
const token = "33333333-3333-4333-8333-333333333333";
const zero = "00000000-0000-0000-0000-000000000000";
const date = "2099-09-21T10:00:00Z";
const plan: ActionPlan = { plan_token: token, cluster_id: id, cluster_name: "example", action: "delete", expected_revision: 4,
  data_disposition: "retain", retained_volumes: [other], downtime: "Database access stops. Data volumes remain retained.", expires_at: date };
const confirmation: ActionConfirmation = { plan_token: token, confirm: true, data_disposition: "retain" };
const admission = { cluster_id: id, operation_id: other, desired_revision: 5, accepted_at: date, replayed: false };
const metadata: CredentialMetadata = { cluster_id: id, credential_id: other, version: 2, purpose: "application", username: "synod_app" };
const endpoints: EndpointInspection = { cluster_id: id, engine: "postgres", desired_revision: 4, observed_revision: 3,
  desired_lifecycle: "running", observed_lifecycle: "unknown", endpoints: [{ id: other, node_id: token, host: "192.0.2.8", port: 15432,
    transport: "tcp", database: "app", client_mode: "direct", availability_profile: "single_node", tls_mode: "disabled" }] };
const fetchMock = vi.fn<typeof fetch>();

beforeEach(() => { fetchMock.mockReset(); vi.stubGlobal("fetch", fetchMock); });
afterEach(() => vi.unstubAllGlobals());

function respond(value: unknown, status = 200): void {
  fetchMock.mockResolvedValueOnce(new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } }));
}

describe("cluster lifecycle requests", () => {
  it("requests the exact retained-data preview without a mutation key", async () => {
    respond(plan);
    expect(await api.planDelete(id)).toEqual(plan);
    expect(fetchMock).toHaveBeenCalledExactlyOnceWith(`/v1/clusters/${id}/action-plans`, {
      method: "POST", headers: { Accept: "application/json", "Content-Type": "application/json" },
      credentials: "same-origin", cache: "no-store", redirect: "error", body: '{"action":"delete"}',
    });
  });

  it.each(["start", "stop"] as const)("sends %s with a fixed quoted revision and key", async (action) => {
    respond(admission, 202);
    expect(await api.actOnCluster(id, action, 4, "original-key")).toEqual(admission);
    expect(fetchMock).toHaveBeenCalledExactlyOnceWith(`/v1/clusters/${id}/actions/${action}`, expect.objectContaining({
      method: "POST", body: "{}", headers: { Accept: "application/json", "Content-Type": "application/json", "Idempotency-Key": "original-key", "If-Match": '"4"' },
    }));
  });

  it("keeps an explicit deletion retry identical without fetching a new preview", async () => {
    fetchMock.mockRejectedValueOnce(new Error("connection lost"));
    await expect(api.actOnCluster(id, "delete", 4, "original-key", confirmation)).rejects.toMatchObject({ detail: { code: "transport_error" } });
    respond({ ...admission, replayed: true }, 202);
    expect((await api.actOnCluster(id, "delete", 4, "original-key", confirmation)).replayed).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0]).toEqual(fetchMock.mock.calls[1]);
    expect(fetchMock.mock.calls[0]).toEqual([`/v1/clusters/${id}`, expect.objectContaining({ method: "DELETE", body: JSON.stringify(confirmation) })]);
  });

  it.each([
    { cluster_id: other }, { plan_token: zero }, { expected_revision: 0 }, { expected_revision: 1.5 },
    { action: "start" }, { data_disposition: "purge" }, { retained_volumes: null }, { retained_volumes: undefined },
    { retained_volumes: [zero] }, { retained_volumes: [other, other] }, { expires_at: "not-a-time" },
    { expires_at: "2099-02-31T10:00:00Z" }, { cluster_name: "" }, { downtime: "" },
  ])("rejects invalid preview bindings %j", async (change) => {
    respond({ ...plan, ...change });
    await expect(api.planDelete(id)).rejects.toMatchObject({ detail: { code: "invalid_response" } });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it.each([null, [], {}, { ...admission, cluster_id: other }, { ...admission, operation_id: zero },
    { ...admission, desired_revision: 0 }, { ...admission, accepted_at: "invalid" }, { ...admission, replayed: "true" }])("retains uncertainty for invalid admission %j", async (value) => {
    respond(value, 202);
    await expect(api.actOnCluster(id, "stop", 4, "key")).rejects.toMatchObject({ detail: { code: "invalid_response", message: expect.stringContaining("exact target, key, revision, and body") } });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("refuses an unexpected success status and malformed JSON", async () => {
    respond(admission, 200);
    await expect(api.actOnCluster(id, "start", 4, "key")).rejects.toMatchObject({ detail: { code: "invalid_response" } });
    fetchMock.mockResolvedValueOnce(new Response("not JSON", { status: 202 }));
    await expect(api.actOnCluster(id, "start", 4, "key")).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });

  it("rejects invalid mutation inputs before any request", async () => {
    for (const revision of [0, -1, 1.1, Number.MAX_SAFE_INTEGER + 1]) {
      await expect(api.actOnCluster(id, "start", revision, "key")).rejects.toBeInstanceOf(Failure);
    }
    await expect(api.actOnCluster(id, "delete", 4, "key")).rejects.toBeInstanceOf(Failure);
    await expect(api.actOnCluster(id, "start", 4, "key", confirmation)).rejects.toBeInstanceOf(Failure);
    await expect(api.actOnCluster(id, "stop", 4, "bad key")).rejects.toBeInstanceOf(Failure);
    await expect(api.planDelete("../other?password=secret")).rejects.toBeInstanceOf(Failure);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it.each([412, 503])("preserves stable safe error identity for HTTP %i without retry", async (status) => {
    const code = status === 412 ? "revision_conflict" : "outcome_unknown";
    respond({ code, message: "password=must-not-escape", request_id: token, operation_id: other }, status);
    try { await api.actOnCluster(id, "start", 4, "key"); expect.fail("request unexpectedly succeeded"); }
    catch (error) {
      expect(detail(error)).toMatchObject({ code, requestID: token, operationID: other });
      expect(JSON.stringify(error)).not.toContain("must-not-escape");
    }
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});

describe("Redis replica requests", () => {
  const removal = { ...plan, action: "scale_down", member_id: other };

  it("reads declared member identity without inventing native observations", async () => {
    respond({ members: [{ id: other, role: "primary", state: "active" }, { id: token, role: "replica", state: "residual" }] });
    expect(await api.clusterMembers(id)).toEqual([{ id: other, role: "primary", state: "active" }, { id: token, role: "replica", state: "residual" }]);
    expect(fetchMock.mock.calls[0]?.[0]).toBe(`/v1/clusters/${id}/members`);
  });

  it.each([null, {}, { members: null }, { members: [{ id: zero, role: "primary", state: "active" }] },
    { members: [{ id: other, role: "master", state: "active" }] }, { members: [{ id: other, role: "primary", state: "healthy" }] },
    { members: [{ id: other, role: "primary", state: "active" }, { id: other, role: "replica", state: "active" }] }])("rejects malformed members %j", async (value) => {
    respond(value);
    await expect(api.clusterMembers(id)).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });

  it("binds a removal preview to the exact member and retained data", async () => {
    respond(removal);
    expect(await api.planScaleDown(id)).toEqual(removal);
    expect(fetchMock.mock.calls[0]).toEqual([`/v1/clusters/${id}/action-plans`, expect.objectContaining({ method: "POST", body: '{"action":"scale_down"}' })]);
  });

  it.each([{ member_id: undefined }, { member_id: zero }, { cluster_id: other }, { action: "delete" },
    { data_disposition: "purge" }, { expected_revision: 0 }, { retained_volumes: [zero] }, { retained_volumes: [other, other] },
    { expires_at: "2099-02-31T10:00:00Z" }])("rejects malformed removal preview %j", async (change) => {
    respond({ ...removal, ...change });
    await expect(api.planScaleDown(id)).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });

  it.each([0, 1] as const)("sends the exact replica count %d and preserves uncertain retry", async (replicas) => {
    const confirmed = replicas === 0 ? confirmation : undefined;
    fetchMock.mockRejectedValueOnce(new Error("lost reply"));
    await expect(api.scaleCluster(id, replicas, 4, "scale-key", confirmed)).rejects.toMatchObject({ detail: { code: "transport_error" } });
    respond(admission, 202);
    expect(await api.scaleCluster(id, replicas, 4, "scale-key", confirmed)).toEqual(admission);
    expect(fetchMock.mock.calls[1]).toEqual(fetchMock.mock.calls[0]);
    expect(fetchMock.mock.calls[0]).toEqual([`/v1/clusters/${id}/actions/scale`, expect.objectContaining({
      method: "POST", body: JSON.stringify(confirmed ? { replicas, ...confirmed } : { replicas }),
      headers: expect.objectContaining({ "If-Match": '"4"', "Idempotency-Key": "scale-key" }),
    })]);
  });

  it("rejects scale input and unexpected admission before presenting acceptance", async () => {
    await expect(api.scaleCluster(id, 0, 4, "key")).rejects.toMatchObject({ detail: { code: "invalid_request" } });
    await expect(api.scaleCluster(id, 1, 4, "key", confirmation)).rejects.toMatchObject({ detail: { code: "invalid_request" } });
    await expect(api.scaleCluster(id, 1, 0, "key")).rejects.toMatchObject({ detail: { code: "invalid_request" } });
    expect(fetchMock).not.toHaveBeenCalled();
    respond({ ...admission, cluster_id: other }, 202);
    await expect(api.scaleCluster(id, 1, 4, "key")).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });
});

describe("declared endpoints", () => {
  it("accepts the shipped Redis database zero endpoint", async () => {
    const redis = { ...endpoints, engine: "redis", endpoints: [{ ...endpoints.endpoints[0], database: "0", port: 16379 }] };
    respond(redis);
    expect(await api.clusterEndpoints(id)).toEqual(redis);
  });

  it("returns metadata without claiming health or retaining unknown secrets", async () => {
    respond({ ...endpoints, password: "private", endpoints: [{ ...endpoints.endpoints[0], password: "private", healthy: true }] });
    expect(await api.clusterEndpoints(id)).toEqual(endpoints);
    expect(fetchMock).toHaveBeenCalledExactlyOnceWith(`/v1/clusters/${id}/endpoints`, expect.objectContaining({ method: "GET", body: null, cache: "no-store" }));
  });

  it.each(["not_allocated", "client_address_required", "address_not_bound", "ambiguous_allocation"])("preserves %s without an invented address", async (reason) => {
    const unavailable = { ...endpoints, endpoints: [], unavailable_reason: reason };
    respond(unavailable);
    expect(await api.clusterEndpoints(id)).toEqual(unavailable);
  });

  it.each(["127.0.0.1", "0.0.0.0", "169.254.1.1", "224.0.0.1", "host.example", "user:password@host", "::1", "fe80::1", "ff02::1", "[2001:db8::1]"])("rejects invalid client address %s", async (host) => {
    respond({ ...endpoints, endpoints: [{ ...endpoints.endpoints[0], host }] });
    await expect(api.clusterEndpoints(id)).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });

  it.each(["192.0.2.8", "10.1.2.3", "2001:db8::1", "fd00::1"])("accepts canonical unicast address %s", async (host) => {
    respond({ ...endpoints, endpoints: [{ ...endpoints.endpoints[0], host }] });
    expect((await api.clusterEndpoints(id)).endpoints[0]?.host).toBe(host);
  });

  it.each([{ cluster_id: other }, { endpoints: null }, { endpoints: [] }, { observed_revision: -1 },
    { desired_lifecycle: "deleted" }, { observed_lifecycle: "healthy" }, { engine: "unknown" }])("rejects invalid endpoint inspection %j", async (change) => {
    respond({ ...endpoints, ...change });
    await expect(api.clusterEndpoints(id)).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });
});

describe("credential metadata and audited disclosure", () => {
  it("accepts replication metadata and its explicit audited disclosure", async () => {
    const replication = { ...metadata, purpose: "replication", username: "synod_replication" };
    respond({ cluster_id: id, credentials: [replication] });
    expect(await api.clusterCredentials(id)).toEqual({ cluster_id: id, credentials: [replication] });
    respond({ credential: replication, password: "replication-password" });
    expect(await api.revealClusterCredential(id, "replication")).toEqual({ credential: replication, password: "replication-password" });
    expect(fetchMock.mock.calls[1]?.[0]).toBe(`/v1/clusters/${id}/credentials/replication/reveal`);
  });

  it("projects credential metadata and never retains a stray password", async () => {
    respond({ cluster_id: id, password: "private", credentials: [{ ...metadata, password: "private" }] });
    expect(await api.clusterCredentials(id)).toEqual({ cluster_id: id, credentials: [metadata] });
    expect(fetchMock).toHaveBeenCalledExactlyOnceWith(`/v1/clusters/${id}/credentials`, expect.objectContaining({ method: "GET", body: null }));
  });

  it.each([{ cluster_id: other }, { credential_id: zero }, { version: 0 }, { purpose: "root" }, { username: "postgres;DROP" }])("refuses malformed credential %j", async (change) => {
    respond({ cluster_id: id, credentials: [{ ...metadata, ...change }] });
    await expect(api.clusterCredentials(id)).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });

  it("rejects missing or duplicate metadata and accepts an explicit empty list", async () => {
    for (const credentials of [undefined, null, [metadata, metadata]]) {
      respond({ cluster_id: id, credentials });
      await expect(api.clusterCredentials(id)).rejects.toMatchObject({ detail: { code: "invalid_response" } });
    }
    respond({ cluster_id: id, credentials: [] });
    expect(await api.clusterCredentials(id)).toEqual({ cluster_id: id, credentials: [] });
  });

  it("sends one explicit reveal POST with no secret or retry key in the request", async () => {
    respond({ credential: metadata, password: "disclosed-once" });
    expect(await api.revealClusterCredential(id, "application")).toEqual({ credential: metadata, password: "disclosed-once" });
    expect(fetchMock).toHaveBeenCalledExactlyOnceWith(`/v1/clusters/${id}/credentials/application/reveal`, {
      method: "POST", headers: { Accept: "application/json", "Content-Type": "application/json" },
      credentials: "same-origin", cache: "no-store", redirect: "error", body: "{}",
    });
  });

  it.each([{ cluster_id: other }, { purpose: "superuser" }, { version: 0 }])("discards secret on mismatched disclosure %j", async (change) => {
    respond({ credential: { ...metadata, ...change }, password: "must-not-escape" });
    try { await api.revealClusterCredential(id, "application"); expect.fail("unexpected disclosure"); }
    catch (error) {
      expect(detail(error).code).toBe("invalid_response");
      expect(JSON.stringify(error)).not.toContain("must-not-escape");
    }
  });

  it.each(["outcome_unknown", "must-not-escape"])("never exposes raw failure or retries reveal: %s", async (code) => {
    respond({ code, message: "must-not-escape", request_id: "must-not-escape", operation_id: "must-not-escape", password: "must-not-escape" }, 503);
    try { await api.revealClusterCredential(id, "application"); expect.fail("unexpected disclosure"); }
    catch (error) {
      expect(JSON.stringify(error)).not.toContain("must-not-escape");
      expect(detail(error).message).not.toMatch(/idempotency|retry/i);
    }
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("sanitizes transport and interrupted response failures without retry", async () => {
    fetchMock.mockRejectedValueOnce(new Error("password=must-not-escape"));
    await expect(api.revealClusterCredential(id, "application")).rejects.toMatchObject({ detail: { code: "transport_error", message: "Credential access authorization is uncertain. No password was returned." } });
    const response = new Response();
    vi.spyOn(response, "text").mockRejectedValueOnce(new Error("password=must-not-escape"));
    fetchMock.mockResolvedValueOnce(response);
    await expect(api.revealClusterCredential(id, "application")).rejects.toMatchObject({ detail: { code: "transport_error" } });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});


const storedCluster: Cluster = { id, installation_id: token,
  spec: { schema_version: 1, name: "orders", engine: "postgres", postgresql: { version: "17.11", topology: "standalone" }, redis: null },
  desired_revision: 2, desired_lifecycle: "running", desired_replicas: 0, observed_revision: 0, observed_lifecycle: "unknown", execution_epoch: 0,
  created_at: date, updated_at: date, deleted_at: null };
const firstPage = Array.from({ length: 50 }, (_, index) => ({ ...storedCluster, id: `11111111-1111-4111-8111-${String(index + 1).padStart(12, "0")}` }));

describe("cluster collection reads", () => {
  it("validates a cluster detail before scale controls consume it", async () => {
    respond(storedCluster);
    expect(await api.cluster(id)).toEqual(storedCluster);
    for (const changed of [{ id: other }, { desired_replicas: undefined }, { spec: null }]) {
      respond({ ...storedCluster, ...changed });
      await expect(api.cluster(id)).rejects.toMatchObject({ detail: { code: "invalid_response" } });
    }
  });

  it("reads one bounded page with same-origin authentication and no caching", async () => {
    respond({ items: firstPage, next_cursor: "opaque-token" });
    expect(await api.clusters()).toEqual({ items: firstPage, next_cursor: "opaque-token" });
    expect(fetchMock).toHaveBeenCalledExactlyOnceWith("/v1/clusters?limit=50&include_deleted=false", {
      method: "GET", headers: { Accept: "application/json" }, credentials: "same-origin", cache: "no-store", redirect: "error", body: null,
    });
  });

  it("encodes an opaque continuation and keeps its deletion filter", async () => {
    respond({ items: [], next_cursor: "" });
    expect(await api.clusters("opaque+/=?&", true)).toEqual({ items: [], next_cursor: "" });
    expect(fetchMock).toHaveBeenCalledExactlyOnceWith("/v1/clusters?limit=50&include_deleted=true&cursor=opaque%2B%2F%3D%3F%26", expect.anything());
  });

  it("projects PostgreSQL and Redis metadata and drops unexpected secrets at every level", async () => {
    const redis: Cluster = { ...storedCluster, id: other, desired_replicas: 1, spec: { schema_version: 1, name: "sessions", engine: "redis", postgresql: null,
      redis: { version: "8.2.7", topology: "standalone", persistence: "rdb+aof", eviction_policy: "allkeys-lfu" } } };
    respond({ items: [{ ...storedCluster, password: "private", spec: { ...storedCluster.spec, password: "private",
      postgresql: { ...storedCluster.spec.postgresql, password: "private" } } },
      { ...redis, spec: { ...redis.spec, redis: { ...redis.spec.redis, password: "private" } } }], next_cursor: "", password: "private" });
    const page = await api.clusters();
    expect(page).toEqual({ items: [storedCluster, redis], next_cursor: "" });
    expect(JSON.stringify(page)).not.toContain("private");
  });

  it("accepts a deletion in progress and includes tombstones only for the explicit filter", async () => {
    const deleting = { ...storedCluster, desired_lifecycle: "deleted", observed_lifecycle: "deleting" };
    respond({ items: [deleting], next_cursor: "" });
    expect((await api.clusters()).items[0]).toEqual(deleting);
    const deleted = { ...deleting, observed_lifecycle: "deleted", deleted_at: date };
    respond({ items: [deleted], next_cursor: "" });
    expect((await api.clusters("", true)).items[0]).toEqual(deleted);
    respond({ items: [deleted], next_cursor: "" });
    await expect(api.clusters()).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });

  it.each([null, {}, { items: null, next_cursor: "" }, { items: [], next_cursor: null }, { items: [], next_cursor: "x" },
    { items: [storedCluster], next_cursor: "x" }, { items: firstPage, next_cursor: "é".repeat(513) },
    { items: [...firstPage, storedCluster], next_cursor: "" }, { items: [storedCluster, storedCluster], next_cursor: "" },
    { items: [storedCluster, { ...storedCluster, id: other, installation_id: other }], next_cursor: "" },
  ])("rejects malformed collection %j", async (value) => {
    respond(value);
    await expect(api.clusters()).rejects.toMatchObject({ detail: { code: "invalid_response" } });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it.each([{ id: zero }, { installation_id: "other" }, { desired_revision: 0 }, { desired_replicas: undefined }, { desired_replicas: -1 }, { desired_replicas: 1.5 }, { observed_revision: -1 },
    { execution_epoch: 1.5 }, { desired_lifecycle: "provisioning" }, { observed_lifecycle: "healthy" },
    { desired_lifecycle: ["running"] }, { observed_lifecycle: ["running"] }, { created_at: "invalid" },
    { updated_at: "2099-02-31T10:00:00Z" }, { deleted_at: undefined }, { spec: { name: "partial", engine: "postgres" } },
    { spec: { ...storedCluster.spec, redis: {} } },
    { spec: { ...storedCluster.spec, postgresql: { version: "17.11", topology: "patroni" } } },
  ])("rejects malformed stored cluster %j", async (change) => {
    respond({ items: [{ ...storedCluster, ...change }], next_cursor: "" });
    await expect(api.clusters()).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });

  it.each([{ persistence: ["rdb"] }, { eviction_policy: ["noeviction"] },
    { persistence: "unsupported" }, { version: "latest" }])("rejects malformed Redis fields %j", async (change) => {
    respond({ items: [{ ...storedCluster, spec: { schema_version: 1, name: "cache", engine: "redis", postgresql: null,
      redis: { version: "8.2.7", topology: "standalone", persistence: "rdb", ...change } } }], next_cursor: "" });
    await expect(api.clusters()).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });

  it("accepts a real Redis int64 byte limit without retaining its rounded browser value", async () => {
    const redisSpec = { schema_version: 1, name: "large-cache", engine: "redis", postgresql: null,
      redis: { version: "8.2.7", topology: "standalone", persistence: "rdb" } };
    const body = JSON.stringify({ items: [{ ...storedCluster, spec: redisSpec }], next_cursor: "" })
      .replace('"persistence":"rdb"', '"persistence":"rdb","maxmemory_bytes":9223372036854775807');
    fetchMock.mockResolvedValueOnce(new Response(body, { status: 200 }));
    const page = await api.clusters();
    expect(page.items[0]?.spec).toEqual(redisSpec);
    expect(JSON.stringify(page)).not.toContain("maxmemory_bytes");
  });

  it("accepts timestamp precision without imposing local sort or clock assumptions", async () => {
    const precise = { ...storedCluster, created_at: "2099-09-21T10:00:00.999999Z", updated_at: "2099-09-21T09:59:00Z" };
    respond({ items: [precise], next_cursor: "" });
    expect((await api.clusters()).items[0]).toEqual(precise);
  });

  it("rejects a non-progressing cursor and overlong input without an automatic retry", async () => {
    respond({ items: firstPage, next_cursor: "same" });
    await expect(api.clusters("same")).rejects.toMatchObject({ detail: { code: "invalid_response" } });
    await expect(api.clusters("é".repeat(513))).rejects.toMatchObject({ detail: { code: "invalid_request" } });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("sanitizes failed list reads and leaves retry to the operator", async () => {
    respond({ code: "dependency_unavailable", message: "password=private", request_id: token }, 503);
    try { await api.clusters(); expect.fail("unexpected success"); }
    catch (error) {
      expect(detail(error)).toMatchObject({ code: "dependency_unavailable", requestID: token });
      expect(JSON.stringify(error)).not.toContain("private");
    }
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});


describe("stored PostgreSQL replication policy", () => {
  it.each(["", "asynchronous", "synchronous"])("preserves %s", async (policy) => {
    const configured = { ...storedCluster, spec: { ...storedCluster.spec,
      postgresql: { ...storedCluster.spec.postgresql, replication_policy: policy } } };
    respond(configured);
    expect(await api.cluster(id)).toEqual(configured);
  });
  it.each(["automatic", true, 1, null])("rejects invalid policy %s", async (policy) => {
    respond({ ...storedCluster, spec: { ...storedCluster.spec,
      postgresql: { ...storedCluster.spec.postgresql, replication_policy: policy } } });
    await expect(api.cluster(id)).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });
});

describe("PostgreSQL recovery requests", () => {
  it.each(["scale_down_failed", "recover_setup", "rebuild_replica"] as const)("binds the %s preview to a member, resource, and volume", async (action) => {
    const recovery = { ...plan, action, member_id: other, resource_id: token };
    respond(recovery);
    expect(await api.planPostgresRecovery(id, action)).toEqual(recovery);
    expect(fetchMock.mock.calls[0]).toEqual([`/v1/clusters/${id}/action-plans`, expect.objectContaining({ method: "POST", body: JSON.stringify({ action }) })]);
  });

  it.each([{ member_id: zero }, { resource_id: undefined }, { resource_id: zero }, { cluster_id: other }, { action: "recover_setup" },
    { retained_volumes: [] }, { retained_volumes: [other, token] }, { retained_volumes: [zero] }, { expected_revision: 0 }, { cluster_name: "" },
    { data_disposition: "purge" }, { expires_at: "2099-02-31T10:00:00Z" }])("rejects malformed rebuild review %j", async (change) => {
    respond({ ...plan, action: "rebuild_replica", member_id: other, resource_id: token, ...change });
    await expect(api.planPostgresRecovery(id, "rebuild_replica")).rejects.toMatchObject({ detail: { code: "invalid_response" } });
  });

  it.each(["recover-setup", "rebuild-replica"] as const)("requires confirmation and preserves a lost %s reply", async (action) => {
    await expect(api.actOnCluster(id, action, 4, "recovery-key")).rejects.toMatchObject({ detail: { code: "invalid_request" } });
    expect(fetchMock).not.toHaveBeenCalled();
    fetchMock.mockRejectedValueOnce(new Error("lost reply"));
    await expect(api.actOnCluster(id, action, 4, "recovery-key", confirmation)).rejects.toMatchObject({ detail: { code: "transport_error" } });
    respond(admission, 202);
    await api.actOnCluster(id, action, 4, "recovery-key", confirmation);
    expect(fetchMock.mock.calls[1]).toEqual(fetchMock.mock.calls[0]);
    expect(fetchMock.mock.calls[0]).toEqual([`/v1/clusters/${id}/actions/${action}`, expect.objectContaining({ method: "POST", body: JSON.stringify(confirmation), headers: expect.objectContaining({ "If-Match": '"4"', "Idempotency-Key": "recovery-key" }) })]);
  });

  it("reads replacement membership without declaring another healthy replica", async () => {
    const members = [{ id, role: "primary", state: "active" }, { id: other, role: "replica", state: "residual" }, { id: token, role: "replica", state: "pending" }];
    respond({ members });
    expect(await api.clusterMembers(id)).toEqual(members);
    for (const invalid of [[...members, { ...members[2], id: crypto.randomUUID() }], members.map((m) => ({ ...m, role: "replica" })), [{ ...members[0] }, { ...members[1], role: "primary" }]]) {
      respond({ members: invalid });
      await expect(api.clusterMembers(id)).rejects.toMatchObject({ detail: { code: "invalid_response" } });
    }
  });
});
