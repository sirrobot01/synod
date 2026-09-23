import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { tick } from "svelte";
import ClusterConnection from "./ClusterConnection.svelte";
import { api, Failure, type CredentialInspection, type CredentialMetadata, type EndpointInspection, type RevealedCredential } from "./api";

vi.mock("./api", async () => {
  const real = await vi.importActual<typeof import("./api")>("./api");
  return { ...real, api: { clusterEndpoints: vi.fn(), clusterCredentials: vi.fn(), revealClusterCredential: vi.fn() } };
});

const clusterID = "11111111-1111-4111-8111-111111111111";
const otherID = "22222222-2222-4222-8222-222222222222";
const credential: CredentialMetadata = {
  credential_id: "33333333-3333-4333-8333-333333333333", cluster_id: clusterID,
  purpose: "application", version: 3, username: "synod_app",
};
const endpointMetadata: EndpointInspection = {
  cluster_id: clusterID, engine: "postgres", desired_revision: 4, observed_revision: 4,
  desired_lifecycle: "running", observed_lifecycle: "running",
  endpoints: [{ id: "44444444-4444-4444-8444-444444444444", node_id: "55555555-5555-4555-8555-555555555555",
    host: "192.0.2.8", port: 5432, transport: "tcp", database: "app", client_mode: "direct",
    availability_profile: "single_node", tls_mode: "disabled" }],
};
const credentialMetadata: CredentialInspection = { cluster_id: clusterID, credentials: [credential] };
const secret = "private-application-password";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

async function mounted() {
  const component = render(ClusterConnection, { id: clusterID });
  await screen.findByRole("button", { name: "Reveal application — audited" });
  return component;
}

beforeEach(() => {
  vi.resetAllMocks();
  vi.mocked(api.clusterEndpoints).mockImplementation(async (id) => ({ ...structuredClone(endpointMetadata), cluster_id: id }));
  vi.mocked(api.clusterCredentials).mockImplementation(async (id) => ({ cluster_id: id, credentials: [{ ...credential, cluster_id: id }] }));
  vi.mocked(api.revealClusterCredential).mockResolvedValue({ credential: { ...credential }, password: secret });
});

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

describe("cluster connection metadata", () => {
  it("reads metadata only and states the client contract and plaintext transport", async () => {
    await mounted();
    expect(screen.getByText("192.0.2.8:5432")).toBeTruthy();
    expect(screen.getByText("direct")).toBeTruthy();
    expect(screen.getByText("single_node")).toBeTruthy();
    expect(screen.getByText("app")).toBeTruthy();
    expect(screen.getByText("TLS is disabled. Database traffic is plaintext.")).toBeTruthy();
    expect(screen.getByText(/do not prove reachability or database health/)).toBeTruthy();
    expect(screen.getByText("version 3")).toBeTruthy();
    expect(screen.getByText("synod_app")).toBeTruthy();
    expect(api.revealClusterCredential).not.toHaveBeenCalled();
    await fireEvent.click(screen.getByRole("button", { name: "Refresh connection metadata" }));
    await screen.findByRole("button", { name: "Reveal application — audited" });
    expect(api.clusterEndpoints).toHaveBeenCalledTimes(2);
    expect(api.clusterCredentials).toHaveBeenCalledTimes(2);
    expect(api.revealClusterCredential).not.toHaveBeenCalled();
  });

  it("shows unavailable reasons without inventing an endpoint", async () => {
    vi.mocked(api.clusterEndpoints).mockResolvedValue({ ...endpointMetadata, endpoints: [], unavailable_reason: "client_address_required" });
    await mounted();
    expect(screen.getByText("client_address_required")).toBeTruthy();
    expect(screen.getByText("No declared endpoint address is available.")).toBeTruthy();
    expect(screen.queryByText("192.0.2.8:5432")).toBe(null);
    expect(screen.queryByText(/127\.0\.0\.1|localhost/)).toBe(null);
  });

  it("keeps independent credential metadata when the endpoint request fails", async () => {
    vi.mocked(api.clusterEndpoints).mockRejectedValue(new Failure(503, { code: "dependency_unavailable", message: "Unavailable", requestID: "request-endpoints" }));
    await mounted();
    expect(screen.getByRole("alert").textContent).toContain("request-endpoints");
    expect(screen.getByText("synod_app")).toBeTruthy();
    expect(api.revealClusterCredential).not.toHaveBeenCalled();
  });

  it("ignores metadata that arrives after navigation", async () => {
    const endpointRead = deferred<EndpointInspection>();
    const credentialRead = deferred<CredentialInspection>();
    vi.mocked(api.clusterEndpoints).mockReturnValueOnce(endpointRead.promise);
    vi.mocked(api.clusterCredentials).mockReturnValueOnce(credentialRead.promise);
    const component = render(ClusterConnection, { id: clusterID });
    await waitFor(() => expect(api.clusterEndpoints).toHaveBeenCalledWith(clusterID));
    await component.rerender({ id: otherID });
    await screen.findByRole("button", { name: "Reveal application — audited" });
    endpointRead.resolve({ ...endpointMetadata, endpoints: [{ ...endpointMetadata.endpoints[0]!, host: "192.0.2.9" }] });
    credentialRead.resolve({ ...credentialMetadata, credentials: [{ ...credential, username: "old_user" }] });
    await tick();
    await tick();
    expect(screen.queryByText("192.0.2.9:5432")).toBe(null);
    expect(screen.queryByText("old_user")).toBe(null);
  });
});

describe("explicit audited credential access", () => {
  it("reveals only on request and clears the value on hide without persistence", async () => {
    const storage = vi.spyOn(Storage.prototype, "setItem");
    const log = vi.spyOn(console, "log");
    const address = window.location.href;
    await mounted();
    expect(screen.queryByDisplayValue(secret)).toBe(null);
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    expect(await screen.findByLabelText("application password · version 3")).toBeTruthy();
    expect(screen.getByDisplayValue(secret)).toBeTruthy();
    expect(api.revealClusterCredential).toHaveBeenCalledExactlyOnceWith(clusterID, "application");
    expect(storage).not.toHaveBeenCalled();
    expect(log).not.toHaveBeenCalled();
    expect(window.location.href).toBe(address);
    await fireEvent.click(screen.getByRole("button", { name: "Hide application credential" }));
    expect(screen.queryByDisplayValue(secret)).toBe(null);
    expect(screen.queryByLabelText(/application password/)).toBe(null);
  });

  it("clears an old value before a failed reveal and never retries it automatically", async () => {
    await mounted();
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    await screen.findByDisplayValue(secret);
    const pending = deferred<RevealedCredential>();
    vi.mocked(api.revealClusterCredential).mockReturnValueOnce(pending.promise);
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    expect(screen.queryByDisplayValue(secret)).toBe(null);
    pending.reject(new Failure(503, { code: "outcome_unknown", message: secret, requestID: "request-reveal" }));
    const failure = await screen.findByRole("alert");
    expect(failure.textContent).toContain("outcome_unknown");
    expect(failure.textContent).toContain("request-reveal");
    expect(failure.textContent).not.toContain(secret);
    expect(failure.textContent).not.toContain("same request identity");
    expect(screen.queryByDisplayValue(secret)).toBe(null);
    expect(api.revealClusterCredential).toHaveBeenCalledTimes(2);
  });

  it("ignores a reveal response after its panel was hidden", async () => {
    const pending = deferred<RevealedCredential>();
    vi.mocked(api.revealClusterCredential).mockReturnValueOnce(pending.promise);
    await mounted();
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    await fireEvent.click(screen.getByRole("button", { name: "Hide application credential" }));
    pending.resolve({ credential, password: secret });
    await tick();
    await tick();
    expect(screen.queryByDisplayValue(secret)).toBe(null);
    expect(screen.queryByRole("button", { name: "Hide application credential" })).toBe(null);
    expect(api.revealClusterCredential).toHaveBeenCalledTimes(1);
  });

  it("ignores an outstanding reveal after navigating away and back to the same cluster", async () => {
    const pending = deferred<RevealedCredential>();
    vi.mocked(api.revealClusterCredential).mockReturnValueOnce(pending.promise);
    const component = await mounted();
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    await component.rerender({ id: otherID });
    await screen.findByRole("button", { name: "Reveal application — audited" });
    await component.rerender({ id: clusterID });
    await screen.findByRole("button", { name: "Reveal application — audited" });
    pending.resolve({ credential, password: secret });
    await tick();
    await tick();
    expect(screen.queryByDisplayValue(secret)).toBe(null);
    expect(api.revealClusterCredential).toHaveBeenCalledTimes(1);
  });

  it("does not let an older request clear a newer explicit reveal", async () => {
    const older = deferred<RevealedCredential>();
    const newer = deferred<RevealedCredential>();
    vi.mocked(api.revealClusterCredential).mockReturnValueOnce(older.promise).mockReturnValueOnce(newer.promise);
    await mounted();
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    await fireEvent.click(screen.getByRole("button", { name: "Hide application credential" }));
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    newer.resolve({ credential, password: "newer-explicit-value" });
    await screen.findByDisplayValue("newer-explicit-value");
    older.reject(new Error("late private failure"));
    await tick();
    await tick();
    expect(screen.getByDisplayValue("newer-explicit-value")).toBeTruthy();
    expect(screen.queryByRole("alert")).toBe(null);
    expect(api.revealClusterCredential).toHaveBeenCalledTimes(2);
  });

  it("does not let an unmounted component publish an outstanding secret", async () => {
    const pending = deferred<RevealedCredential>();
    vi.mocked(api.revealClusterCredential).mockReturnValueOnce(pending.promise);
    const component = await mounted();
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    component.unmount();
    await mounted();
    pending.resolve({ credential, password: secret });
    await tick();
    await tick();
    expect(screen.queryByDisplayValue(secret)).toBe(null);
    expect(api.revealClusterCredential).toHaveBeenCalledTimes(1);
  });

  it("refuses a rotated credential until its metadata is read again", async () => {
    vi.mocked(api.revealClusterCredential).mockResolvedValueOnce({ credential: { ...credential, version: 4 }, password: secret });
    await mounted();
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    expect((await screen.findByRole("alert")).textContent).toContain("invalid_response");
    expect(screen.queryByDisplayValue(secret)).toBe(null);
    expect(api.revealClusterCredential).toHaveBeenCalledTimes(1);
  });

  it("clears a revealed value on metadata refresh and full-page navigation", async () => {
    await mounted();
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    await screen.findByDisplayValue(secret);
    await fireEvent.click(screen.getByRole("button", { name: "Refresh connection metadata" }));
    await screen.findByRole("button", { name: "Reveal application — audited" });
    expect(screen.queryByDisplayValue(secret)).toBe(null);
    expect(api.revealClusterCredential).toHaveBeenCalledTimes(1);
    await fireEvent.click(screen.getByRole("button", { name: "Reveal application — audited" }));
    await screen.findByDisplayValue(secret);
    window.dispatchEvent(new Event("pagehide"));
    await tick();
    expect(screen.queryByDisplayValue(secret)).toBe(null);
  });
});
