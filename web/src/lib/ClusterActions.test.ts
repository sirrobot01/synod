import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/svelte";
import ClusterActions from "./ClusterActions.svelte";
import { api, Failure, type ActionPlan, type Admission, type Cluster, type ClusterMember, type ScaleDownPlan, type PostgresRecoveryPlan } from "./api";

vi.mock("./api", async () => {
  const original = await vi.importActual<typeof import("./api")>("./api");
  return { ...original, api: { ...original.api, planDelete: vi.fn(), actOnCluster: vi.fn(), planScaleDown: vi.fn(), scaleCluster: vi.fn(), planPostgresRecovery: vi.fn() } };
});
const cluster: Cluster = {
  installation_id: "99999999-9999-4999-8999-999999999999", execution_epoch: 0, created_at: "2026-09-21T09:00:00Z", deleted_at: null,
  id: "11111111-1111-4111-8111-111111111111", spec: { schema_version: 1, name: "pg-core", engine: "postgres", postgresql: { version: "17.11", topology: "standalone" }, redis: null }, desired_revision: 7,
  desired_lifecycle: "running", desired_replicas: 0, observed_revision: 7, observed_lifecycle: "running", updated_at: "2026-09-21T10:00:00Z",
};
const plan: ActionPlan = {
  cluster_id: cluster.id, cluster_name: "pg-core", action: "delete", expected_revision: 7,
  plan_token: "44444444-4444-4444-8444-444444444444", data_disposition: "retain", retained_volumes: ["66666666-6666-4666-8666-666666666666", "77777777-7777-4777-8777-777777777777"],
  downtime: "permanent", expires_at: "2099-09-21T10:05:00Z",
};
const admission: Admission = {
  cluster_id: cluster.id, operation_id: "33333333-3333-4333-8333-333333333333", desired_revision: 8,
  accepted_at: "2026-09-21T10:00:00Z", replayed: false,
};
const props = { id: cluster.id, actorID: "actor-a", cluster, onOpenOperation: vi.fn() };
const storageKey = `synod.lifecycle:${JSON.stringify([props.actorID, cluster.id])}`;
beforeEach(() => {
  sessionStorage.clear();
  vi.mocked(api.planDelete).mockReset().mockResolvedValue(structuredClone(plan));
  vi.mocked(api.actOnCluster).mockReset().mockResolvedValue(admission);
  vi.mocked(api.planScaleDown).mockReset();
  vi.mocked(api.planPostgresRecovery).mockReset();
  vi.mocked(api.scaleCluster).mockReset().mockResolvedValue(admission);
  props.onOpenOperation.mockReset();
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

describe("Redis replica controls", () => {
  const redis: Cluster = { ...cluster, spec: { schema_version: 1, name: "cache", engine: "redis", postgresql: null,
    redis: { version: "8.2.7", topology: "standalone", persistence: "rdb" } } };
  const members: ClusterMember[] = [{ id: "88888888-8888-4888-8888-888888888888", role: "primary", state: "active" },
    { id: "99999999-9999-4999-8999-999999999999", role: "replica", state: "active" }];
  const removal: ScaleDownPlan = { ...plan, cluster_name: "cache", action: "scale_down", member_id: members[1]!.id, retained_volumes: [plan.retained_volumes[0]!] };
  const replicated = { ...props, cluster: { ...redis, desired_replicas: 1 }, members };

  it("adds one replica with a saved identity and an operation link", async () => {
    vi.mocked(api.scaleCluster).mockImplementation(async (id, replicas, revision, key) => {
      expect(JSON.parse(sessionStorage.getItem(storageKey)!)).toMatchObject({ targetID: id, action: "scale_up", expectedRevision: revision, key });
      expect(replicas).toBe(1);
      return admission;
    });
    render(ClusterActions, { ...props, cluster: redis, members: [members[0]!] });
    await fireEvent.click(screen.getByRole("button", { name: "Add replica" }));
    await screen.findByRole("link", { name: /Open operation/ });
    expect(api.scaleCluster).toHaveBeenCalledWith(redis.id, 1, redis.desired_revision, expect.any(String), undefined);
    expect(api.actOnCluster).not.toHaveBeenCalled();
  });

  it("offers an explicit retry of a residual member at the same desired count", async () => {
    render(ClusterActions, { ...replicated, members: [members[0]!, { ...members[1]!, state: "residual" }] });
    await fireEvent.click(screen.getByRole("button", { name: "Retry replica add" }));
    await screen.findByRole("link", { name: /Open operation/ });
    expect(api.scaleCluster).toHaveBeenCalledWith(redis.id, 1, redis.desired_revision, expect.any(String), undefined);
  });

  it.each(["no persistence", "stopped", "members unavailable"])("disables new scale for %s", async (reason) => {
    const found = structuredClone(redis);
    if (reason === "no persistence") found.spec.redis!.persistence = "none";
    if (reason === "stopped") found.desired_lifecycle = "stopped";
    render(ClusterActions, { ...props, cluster: found, members: reason === "members unavailable" ? null : members });
    expect((screen.getByRole("button", { name: "Add replica" }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole("button", { name: "Remove replica…" }) as HTMLButtonElement).disabled).toBe(true);
    expect(api.scaleCluster).not.toHaveBeenCalled();
  });

  it("shows the exact removal member and uses the server preview revision", async () => {
    vi.mocked(api.planScaleDown).mockResolvedValue({ ...removal, expected_revision: 12 });
    render(ClusterActions, replicated);
    await fireEvent.click(screen.getByRole("button", { name: "Remove replica…" }));
    const dialog = await screen.findByRole("dialog");
    await within(dialog).findByText(removal.member_id);
    expect(within(dialog).getByText(removal.retained_volumes[0]!)).toBeTruthy();
    expect(api.scaleCluster).not.toHaveBeenCalled();
    expect((within(dialog).getByRole("button", { name: "Confirm replica removal" }) as HTMLButtonElement).disabled).toBe(true);
    await fireEvent.input(screen.getByLabelText("Cluster name confirmation"), { target: { value: "cache" } });
    await fireEvent.click(screen.getByRole("button", { name: "Confirm replica removal" }));
    await screen.findByRole("link", { name: /Open operation/ });
    expect(api.scaleCluster).toHaveBeenCalledWith(redis.id, 0, 12, expect.any(String), { plan_token: removal.plan_token, confirm: true, data_disposition: "retain" });
  });

  it("holds all new actions and retries an uncertain removal after reload and preview expiry", async () => {
    vi.mocked(api.planScaleDown).mockResolvedValue(removal);
    vi.mocked(api.scaleCluster).mockRejectedValueOnce(new TypeError("lost reply")).mockResolvedValueOnce(admission);
    const view = render(ClusterActions, replicated);
    await fireEvent.click(screen.getByRole("button", { name: "Remove replica…" }));
    await fireEvent.input(await screen.findByLabelText("Cluster name confirmation"), { target: { value: "cache" } });
    await fireEvent.click(screen.getByRole("button", { name: "Confirm replica removal" }));
    await screen.findByRole("button", { name: "Retry same replica removal request" });
    const original = vi.mocked(api.scaleCluster).mock.calls[0];
    await fireEvent.click(screen.getByRole("button", { name: "Close" }));
    expect((screen.getByRole("button", { name: "Start" }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole("button", { name: "Add replica" }) as HTMLButtonElement).disabled).toBe(true);
    view.unmount();
    vi.spyOn(Date, "now").mockReturnValue(Date.parse("2100-01-01T00:00:00Z"));
    render(ClusterActions, { ...props, cluster: null, members: null });
    await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
    await screen.findByRole("link", { name: /Open operation/ });
    expect(vi.mocked(api.scaleCluster).mock.calls[1]).toEqual(original);
    expect(api.planScaleDown).toHaveBeenCalledOnce();
  });

  it("requires a new removal confirmation after a definite revision conflict", async () => {
    vi.mocked(api.planScaleDown).mockResolvedValueOnce(removal).mockResolvedValueOnce({ ...removal, expected_revision: 15 });
    vi.mocked(api.scaleCluster).mockRejectedValueOnce(new Failure(412, { code: "revision_conflict", message: "Changed", requestID: "" }));
    render(ClusterActions, replicated);
    await fireEvent.click(screen.getByRole("button", { name: "Remove replica…" }));
    await fireEvent.input(await screen.findByLabelText("Cluster name confirmation"), { target: { value: "cache" } });
    await fireEvent.click(screen.getByRole("button", { name: "Confirm replica removal" }));
    await screen.findByRole("button", { name: "Fetch new preview" });
    expect(sessionStorage.getItem(storageKey)).toBeNull();
    await fireEvent.click(screen.getByRole("button", { name: "Fetch new preview" }));
    const input = await screen.findByLabelText("Cluster name confirmation");
    expect((input as HTMLInputElement).value).toBe("");
    expect(api.planScaleDown).toHaveBeenCalledTimes(2);
  });

  it("blocks a restored removal whose saved member identity is malformed", async () => {
    vi.mocked(api.planScaleDown).mockResolvedValue(removal);
    vi.mocked(api.scaleCluster).mockRejectedValue(new TypeError("lost reply"));
    const view = render(ClusterActions, replicated);
    await fireEvent.click(screen.getByRole("button", { name: "Remove replica…" }));
    await fireEvent.input(await screen.findByLabelText("Cluster name confirmation"), { target: { value: "cache" } });
    await fireEvent.click(screen.getByRole("button", { name: "Confirm replica removal" }));
    await screen.findByRole("button", { name: "Retry same replica removal request" });
    view.unmount();
    const saved = JSON.parse(sessionStorage.getItem(storageKey)!);
    saved.plan.member_id = "00000000-0000-0000-0000-000000000000";
    sessionStorage.setItem(storageKey, JSON.stringify(saved));
    render(ClusterActions, replicated);
    await screen.findAllByText("lifecycle_storage_unavailable");
    expect(screen.queryByRole("button", { name: "Retry same request" })).toBeNull();
    expect((screen.getByRole("button", { name: "Start" }) as HTMLButtonElement).disabled).toBe(true);
    expect(api.scaleCluster).toHaveBeenCalledOnce();
  });
});

describe("cluster lifecycle actions", () => {
  it.each(["Start", "Stop"])("persists %s before dispatch and keeps acceptance across reload", async (label) => {
    const refresh = vi.fn();
    vi.mocked(api.actOnCluster).mockImplementation(async (id, action, revision, key) => {
      expect(JSON.parse(sessionStorage.getItem(storageKey)!)).toMatchObject({ targetID: id, action, expectedRevision: revision, key });
      return admission;
    });
    const view = render(ClusterActions, { ...props, onRefresh: refresh });
    await fireEvent.click(screen.getByRole("button", { name: label }));
    const link = await screen.findByRole("link", { name: /Open operation 33333333-3333-4333-8333-333333333333/ });
    expect(api.actOnCluster).toHaveBeenCalledWith(cluster.id, label.toLowerCase(), 7, expect.any(String), undefined);
    expect(link.getAttribute("href")).toBe("/operations/33333333-3333-4333-8333-333333333333");
    expect(screen.getByText(/This confirms admission only/)).toBeTruthy();
    await fireEvent.click(link);
    expect(props.onOpenOperation).toHaveBeenCalledWith("33333333-3333-4333-8333-333333333333");
    expect(refresh).toHaveBeenCalledOnce();
    view.unmount();
    render(ClusterActions, { ...props, cluster: null });
    expect(await screen.findByRole("link", { name: /Open operation 33333333-3333-4333-8333-333333333333/ })).toBeTruthy();
    expect(api.actOnCluster).toHaveBeenCalledOnce();
  });

  it("shows the server target and requires an exact name for retained-data deletion", async () => {
    vi.mocked(api.planDelete).mockResolvedValue({ ...plan, cluster_name: "server-name", expected_revision: 9 });
    render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Delete…" }));
    const dialog = await screen.findByRole("dialog");
    await within(dialog).findByText("66666666-6666-4666-8666-666666666666");
    for (const text of ["77777777-7777-4777-8777-777777777777", cluster.id, "9", "permanent"]) expect(within(dialog).getByText(text)).toBeTruthy();
    expect(api.actOnCluster).not.toHaveBeenCalled();
    const confirm = within(dialog).getByRole("button", { name: "Confirm delete" }) as HTMLButtonElement;
    expect(confirm.disabled).toBe(true);
    await fireEvent.input(screen.getByLabelText("Cluster name confirmation"), { target: { value: "server-name " } });
    expect(confirm.disabled).toBe(true);
    await fireEvent.input(screen.getByLabelText("Cluster name confirmation"), { target: { value: "server-name" } });
    await fireEvent.click(confirm);
    await screen.findByRole("link", { name: /Open operation/ });
    expect(api.actOnCluster).toHaveBeenCalledWith(cluster.id, "delete", 9, expect.any(String), { plan_token: plan.plan_token, confirm: true, data_disposition: "retain" });
  });

  it("restores an uncertain delete after close and reload and retries the exact expired request", async () => {
    vi.mocked(api.actOnCluster).mockRejectedValueOnce(new Failure(503, { code: "outcome_unknown", message: "Unknown", requestID: "request-a" })).mockResolvedValueOnce({ ...admission, replayed: true });
    const view = render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Delete…" }));
    await fireEvent.input(await screen.findByLabelText("Cluster name confirmation"), { target: { value: "pg-core" } });
    await fireEvent.click(screen.getByRole("button", { name: "Confirm delete" }));
    await screen.findByText("outcome_unknown");
    const original = vi.mocked(api.actOnCluster).mock.calls[0];
    await fireEvent.click(screen.getByRole("button", { name: "Close" }));
    expect(screen.queryByRole("dialog")).toBeNull();
    await fireEvent.click(screen.getByRole("button", { name: "Delete…" }));
    expect(screen.queryByLabelText("Cluster name confirmation")).toBeNull();
    expect(api.planDelete).toHaveBeenCalledOnce();
    view.unmount();
    vi.spyOn(Date, "now").mockReturnValue(Date.parse("2100-01-01T00:00:00Z"));
    render(ClusterActions, { ...props, cluster: null });
    await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
    await screen.findByRole("link", { name: /Open operation/ });
    expect(vi.mocked(api.actOnCluster).mock.calls[1]).toEqual(original);
    expect(api.planDelete).toHaveBeenCalledOnce();
  });

  it.each(["revision_conflict", "invalid_request"])("requires new preview and name after %s refusal", async (code) => {
    vi.mocked(api.actOnCluster).mockRejectedValueOnce(new Failure(409, { code, message: "Refused", requestID: "request-a" })).mockResolvedValueOnce(admission);
    vi.mocked(api.planDelete).mockResolvedValueOnce(plan).mockResolvedValueOnce({ ...plan, plan_token: "55555555-5555-4555-8555-555555555555", expected_revision: 10 });
    render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Delete…" }));
    await fireEvent.input(await screen.findByLabelText("Cluster name confirmation"), { target: { value: "pg-core" } });
    await fireEvent.click(screen.getByRole("button", { name: "Confirm delete" }));
    await screen.findAllByText(code);
    expect(sessionStorage.getItem(storageKey)).toBeNull();
    expect(screen.queryByRole("button", { name: "Retry same request" })).toBeNull();
    await fireEvent.click(screen.getByRole("button", { name: "Fetch new preview" }));
    const input = await screen.findByLabelText("Cluster name confirmation");
    expect((input as HTMLInputElement).value).toBe("");
    expect((screen.getByRole("button", { name: "Confirm delete" }) as HTMLButtonElement).disabled).toBe(true);
    await fireEvent.input(input, { target: { value: "pg-core" } });
    await fireEvent.click(screen.getByRole("button", { name: "Confirm delete" }));
    await screen.findByRole("link", { name: /Open operation/ });
    const [first, second] = vi.mocked(api.actOnCluster).mock.calls;
    expect(second?.[2]).toBe(10);
    expect(second?.[3]).not.toBe(first?.[3]);
    expect(second?.[4]?.plan_token).toBe("55555555-5555-4555-8555-555555555555");
  });

  it("refuses an expired initial preview without dispatch", async () => {
    vi.mocked(api.planDelete).mockResolvedValue({ ...plan, expires_at: "2000-01-01T00:00:00Z", retained_volumes: [] });
    render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Delete…" }));
    expect(await screen.findByText("No recorded volumes.")).toBeTruthy();
    await fireEvent.input(screen.getByLabelText("Cluster name confirmation"), { target: { value: "pg-core" } });
    await fireEvent.click(screen.getByRole("button", { name: "Confirm delete" }));
    await screen.findAllByText("plan_expired");
    expect(api.actOnCluster).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Fetch new preview" })).toBeTruthy();
  });

  it("keeps a late admission bound to its original actor and target", async () => {
    let finish!: (value: Admission) => void;
    vi.mocked(api.actOnCluster).mockReturnValue(new Promise((resolve) => { finish = resolve; }));
    const refresh = vi.fn();
    const view = render(ClusterActions, { ...props, onRefresh: refresh });
    await fireEvent.click(screen.getByRole("button", { name: "Stop" }));
    expect((screen.getByRole("button", { name: "Start" }) as HTMLButtonElement).disabled).toBe(true);
    await fireEvent.click(screen.getByRole("button", { name: "Stop" }));
    expect(api.actOnCluster).toHaveBeenCalledOnce();
    await view.rerender({ ...props, id: "22222222-2222-4222-8222-222222222222", actorID: "actor-b", cluster: { ...cluster, id: "22222222-2222-4222-8222-222222222222" }, onRefresh: refresh });
    finish(admission);
    await waitFor(() => expect(JSON.parse(sessionStorage.getItem(storageKey)!).admission).toEqual(admission));
    expect(screen.queryByRole("link", { name: /Open operation/ })).toBeNull();
    expect(refresh).not.toHaveBeenCalled();
    await view.rerender(props);
    expect(await screen.findByRole("link", { name: /Open operation/ })).toBeTruthy();
  });

  it("saves a late admission after unmount without refreshing the destroyed view", async () => {
    let finish!: (value: Admission) => void;
    vi.mocked(api.actOnCluster).mockReturnValue(new Promise((resolve) => { finish = resolve; }));
    const refresh = vi.fn();
    const view = render(ClusterActions, { ...props, onRefresh: refresh });
    await fireEvent.click(screen.getByRole("button", { name: "Stop" }));
    view.unmount();
    finish(admission);
    await waitFor(() => expect(JSON.parse(sessionStorage.getItem(storageKey)!).admission).toEqual(admission));
    expect(refresh).not.toHaveBeenCalled();
    render(ClusterActions, props);
    expect(await screen.findByRole("link", { name: /Open operation/ })).toBeTruthy();
  });

  it("ignores a preview after navigation or dialog closure", async () => {
    let finish!: (value: ActionPlan) => void;
    vi.mocked(api.planDelete).mockReturnValue(new Promise((resolve) => { finish = resolve; }));
    const view = render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Delete…" }));
    await fireEvent.keyDown(window, { key: "Escape" });
    await view.rerender({ ...props, id: "22222222-2222-4222-8222-222222222222", cluster: { ...cluster, id: "22222222-2222-4222-8222-222222222222" } });
    finish(plan);
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(screen.queryByText("66666666-6666-4666-8666-666666666666")).toBeNull();
    expect(api.actOnCluster).not.toHaveBeenCalled();
  });

  it("blocks dispatch when persistence fails and refuses malformed saved attempts", async () => {
    vi.spyOn(Object.getPrototypeOf(sessionStorage), "setItem").mockImplementation(() => { throw new DOMException("blocked"); });
    const view = render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Start" }));
    await screen.findAllByText("lifecycle_storage_unavailable");
    expect(api.actOnCluster).not.toHaveBeenCalled();
    view.unmount();
    vi.restoreAllMocks();
    sessionStorage.setItem(storageKey, '{"version":1,"key":"original-but-incomplete"}');
    render(ClusterActions, props);
    await screen.findAllByText("lifecycle_storage_unavailable");
    expect((screen.getByRole("button", { name: "Start" }) as HTMLButtonElement).disabled).toBe(true);
    expect(sessionStorage.getItem(storageKey)).toContain("original-but-incomplete");
  });

  it("retains an uncertain request when its explicit retry is refused by authentication", async () => {
    vi.mocked(api.actOnCluster)
      .mockRejectedValueOnce(new TypeError("network"))
      .mockRejectedValueOnce(new Failure(401, { code: "unauthenticated", message: "Sign in", requestID: "request-b" }))
      .mockResolvedValueOnce(admission);
    render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Stop" }));
    await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
    await screen.findAllByText("unauthenticated");
    expect(sessionStorage.getItem(storageKey)).not.toBeNull();
    expect((screen.getByRole("button", { name: "Start" }) as HTMLButtonElement).disabled).toBe(true);
    await fireEvent.click(screen.getByRole("button", { name: "Retry same request" }));
    await screen.findByRole("link", { name: /Open operation/ });
    const calls = vi.mocked(api.actOnCluster).mock.calls;
    expect(calls[1]).toEqual(calls[0]);
    expect(calls[2]).toEqual(calls[0]);
  });

  it.each(["accepted", "refused"])("cannot replace a newer uncertain request with a late old %s response", async (outcome) => {
    let finish!: (value: Admission) => void;
    let reject!: (error: Failure) => void;
    vi.mocked(api.actOnCluster)
      .mockReturnValueOnce(new Promise((resolve, fail) => { finish = resolve; reject = fail; }))
      .mockResolvedValueOnce(admission)
      .mockRejectedValueOnce(new TypeError("network"));
    const original = render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Start" }));
    original.unmount();
    const remounted = render(ClusterActions, props);
    await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
    await screen.findByRole("link", { name: /Open operation/ });
    await fireEvent.click(screen.getByRole("button", { name: "Stop" }));
    await screen.findByRole("button", { name: "Retry same request" });
    const pending = sessionStorage.getItem(storageKey);
    if (outcome === "accepted") finish(admission);
    else reject(new Failure(409, { code: "operation_conflict", message: "Earlier conflict", requestID: "" }));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(sessionStorage.getItem(storageKey)).toBe(pending);
    remounted.unmount();
    render(ClusterActions, props);
    await screen.findByRole("button", { name: "Retry same request" });
    expect(screen.getByText(/Outcome uncertain · stop/)).toBeTruthy();
  });

  it("does not let a late initial refusal erase a pending remounted retry", async () => {
    let reject!: (error: Failure) => void;
    let finish!: (value: Admission) => void;
    vi.mocked(api.actOnCluster)
      .mockReturnValueOnce(new Promise((_, fail) => { reject = fail; }))
      .mockReturnValueOnce(new Promise((resolve) => { finish = resolve; }));
    const original = render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Start" }));
    original.unmount();
    render(ClusterActions, props);
    await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
    const pending = sessionStorage.getItem(storageKey);
    reject(new Failure(409, { code: "operation_conflict", message: "Earlier conflict", requestID: "" }));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(sessionStorage.getItem(storageKey)).toBe(pending);
    finish(admission);
    await screen.findByRole("link", { name: /Open operation/ });
  });

  it.each(["revision_conflict", "operation_conflict"])("allows a fresh request after retry proves absence with %s", async (code) => {
    vi.mocked(api.actOnCluster).mockRejectedValueOnce(new TypeError("network"))
      .mockRejectedValueOnce(new Failure(409, { code, message: "Refused after replay lookup", requestID: "" }));
    render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Stop" }));
    await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
    await screen.findAllByText(code);
    expect(sessionStorage.getItem(storageKey)).toBeNull();
    expect((screen.getByRole("button", { name: "Start" }) as HTMLButtonElement).disabled).toBe(false);
  });

  it.each(["operation", "targetName", "key"])("blocks malformed saved %s instead of treating it as accepted", async (field) => {
    const view = render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Start" }));
    await screen.findByRole("link", { name: /Open operation/ });
    view.unmount();
    const saved = JSON.parse(sessionStorage.getItem(storageKey)!);
    if (field === "operation") saved.admission.operation_id = "bogus";
    if (field === "targetName") saved.targetName = "invalid name";
    if (field === "key") saved.key = "invalid key";
    sessionStorage.setItem(storageKey, JSON.stringify(saved));
    render(ClusterActions, props);
    await screen.findAllByText("lifecycle_storage_unavailable");
    expect(screen.queryByRole("link", { name: /Open operation/ })).toBeNull();
    expect((screen.getByRole("button", { name: "Start" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it.each(["unauthenticated", "transport"])("shows an earlier known admission when the current retry reports %s", async (outcome) => {
    let finish!: (value: Admission) => void;
    let reject!: (error: Error) => void;
    vi.mocked(api.actOnCluster)
      .mockReturnValueOnce(new Promise((resolve) => { finish = resolve; }))
      .mockReturnValueOnce(new Promise((_, fail) => { reject = fail; }));
    const original = render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Start" }));
    original.unmount();
    render(ClusterActions, props);
    await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
    finish(admission);
    await waitFor(() => expect(JSON.parse(sessionStorage.getItem(storageKey)!).admission).toEqual(admission));
    reject(outcome === "transport" ? new TypeError("network") : new Failure(401, { code: "unauthenticated", message: "Sign in", requestID: "" }));
    await screen.findByRole("link", { name: /Open operation/ });
    expect(screen.queryByRole("button", { name: "Retry same request" })).toBeNull();
  });

  it("does not expose another actor's saved request", async () => {
    vi.mocked(api.actOnCluster).mockRejectedValue(new TypeError("network"));
    const view = render(ClusterActions, props);
    await fireEvent.click(screen.getByRole("button", { name: "Start" }));
    await screen.findByRole("button", { name: "Retry same request" });
    await view.rerender({ ...props, actorID: "actor-b" });
    expect(screen.queryByRole("button", { name: "Retry same request" })).toBeNull();
    expect((screen.getByRole("button", { name: "Start" }) as HTMLButtonElement).disabled).toBe(false);
    expect(sessionStorage.getItem(storageKey)).not.toBeNull();
  });
});


describe("PostgreSQL replica and recovery controls", () => {
  const primary: ClusterMember = { id: "88888888-8888-4888-8888-888888888888", role: "primary", state: "active" };
  const replica: ClusterMember = { id: "99999999-9999-4999-8999-999999999999", role: "replica", state: "residual" };
  const replicated = { ...props, cluster: { ...cluster, desired_replicas: 1 }, members: [primary, replica] };
  const actions = [
    { button: "Remove failed replica…", confirm: "Confirm failed replica removal", action: "scale_down_failed", preview: "scale_down_failed" },
    { button: "Rebuild replica…", confirm: "Confirm replica rebuild", action: "rebuild-replica", preview: "rebuild_replica" },
    { button: "Recover setup…", confirm: "Confirm setup recovery", action: "recover-setup", preview: "recover_setup" },
  ] as const;

  it("adds a PostgreSQL replica and discloses synchronous commit waits", async () => {
    render(ClusterActions, { ...props, cluster: { ...cluster, spec: { ...cluster.spec, engine: "postgres", redis: null, postgresql: { version: "17.11", topology: "standalone", replication_policy: "synchronous" } } }, members: [primary] });
    expect(screen.getByText(/Synchronous commits wait/)).toBeTruthy();
    await fireEvent.click(screen.getByRole("button", { name: "Add replica" }));
    await screen.findByRole("link", { name: /Open operation/ });
    expect(api.scaleCluster).toHaveBeenCalledWith(cluster.id, 1, 7, expect.any(String), undefined);
  });

  it.each(actions)("reviews and restores the exact $action request after a lost reply", async (entry) => {
    const review: PostgresRecoveryPlan = { ...plan, action: entry.preview, member_id: entry.action === "recover-setup" ? primary.id : replica.id,
      resource_id: "55555555-5555-4555-8555-555555555555", retained_volumes: [plan.retained_volumes[0]!], expected_revision: 12,
      downtime: "Verify native failure. Keep the old volume. Commits can wait." };
    vi.mocked(api.planPostgresRecovery).mockResolvedValue(review);
    const mutation = entry.action === "scale_down_failed" ? vi.mocked(api.scaleCluster) : vi.mocked(api.actOnCluster);
    mutation.mockRejectedValueOnce(new TypeError("lost reply")).mockResolvedValue(admission);
    const selected = entry.action === "recover-setup" ? { ...props, members: [primary] } : replicated;
    const view = render(ClusterActions, selected);
    await fireEvent.click(screen.getByRole("button", { name: entry.button }));
    const dialog = await screen.findByRole("dialog");
    await within(dialog).findByText(review.resource_id);
    expect(within(dialog).getByText(review.member_id)).toBeTruthy();
    expect(within(dialog).getByText(review.retained_volumes[0]!)).toBeTruthy();
    expect(within(dialog).getByText(review.downtime)).toBeTruthy();
    expect((within(dialog).getByRole("button", { name: entry.confirm }) as HTMLButtonElement).disabled).toBe(true);
    await fireEvent.input(within(dialog).getByLabelText("Cluster name confirmation"), { target: { value: "pg-core" } });
    await fireEvent.click(within(dialog).getByRole("button", { name: entry.confirm }));
    await waitFor(() => expect(mutation).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(screen.getByText(/Outcome uncertain/)).toBeTruthy());
    const first = mutation.mock.calls[0];
    expect(first).toEqual([cluster.id, entry.action === "scale_down_failed" ? 0 : entry.action, 12, expect.any(String), { plan_token: review.plan_token, confirm: true, data_disposition: "retain" }]);
    const saved = JSON.parse(sessionStorage.getItem(storageKey)!);
    expect(saved.plan).toEqual(review);
    view.unmount();
    // An accepted request can be replayed after its preview expires.
    saved.plan.expires_at = "2020-09-21T10:00:00Z";
    sessionStorage.setItem(storageKey, JSON.stringify(saved));
    render(ClusterActions, selected);
    await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
    await screen.findByRole("link", { name: /Open operation/ });
    expect(mutation.mock.calls[1]).toEqual(first);
    expect(api.planPostgresRecovery).toHaveBeenCalledExactlyOnceWith(cluster.id, entry.preview);
  });

  it("blocks new replica changes during replacement membership", async () => {
    render(ClusterActions, { ...replicated, members: [primary, replica, { ...replica, id: plan.plan_token, state: "pending" }] });
    for (const name of ["Retry replica add", "Remove replica…", "Remove failed replica…", "Rebuild replica…", "Recover setup…"]) {
      expect((screen.getByRole("button", { name }) as HTMLButtonElement).disabled).toBe(true);
    }
    expect(screen.getByText(/Replacement membership is still present/)).toBeTruthy();
  });

  it.each(["stopped", "replicas requested"])("disables setup recovery when %s", (reason) => {
    render(ClusterActions, { ...props, cluster: { ...cluster, desired_lifecycle: reason === "stopped" ? "stopped" : "running", desired_replicas: reason === "replicas requested" ? 1 : 0 }, members: [primary] });
    expect((screen.getByRole("button", { name: "Recover setup…" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("does not offer PostgreSQL recovery for an unsupported version", () => {
    render(ClusterActions, { ...props, cluster: { ...cluster, spec: { ...cluster.spec, engine: "postgres", redis: null, postgresql: { version: "16.13", topology: "standalone" } } }, members: [primary] });
    expect(screen.queryByRole("button", { name: "Recover setup…" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Rebuild replica…" })).toBeNull();
  });

  it.each(["resource", "member", "action", "volumes"])("blocks a saved recovery with invalid %s", async (field) => {
    const review = { ...plan, action: "rebuild_replica", member_id: replica.id, resource_id: primary.id, retained_volumes: [plan.retained_volumes[0]!] };
    if (field === "resource") review.resource_id = "invalid";
    if (field === "member") review.member_id = "00000000-0000-0000-0000-000000000000";
    if (field === "action") review.action = "recover_setup";
    if (field === "volumes") review.retained_volumes = [];
    sessionStorage.setItem(storageKey, JSON.stringify({ version: 1, actorID: props.actorID, targetID: cluster.id, targetName: "pg-core", action: "rebuild-replica", expectedRevision: 7, key: "rebuild-key", dispatchID: primary.id, plan: review, confirmation: { plan_token: plan.plan_token, confirm: true, data_disposition: "retain" } }));
    render(ClusterActions, replicated);
    await screen.findAllByText("lifecycle_storage_unavailable");
    expect(screen.queryByRole("button", { name: "Retry same request" })).toBeNull();
    expect(api.actOnCluster).not.toHaveBeenCalled();
  });
});
