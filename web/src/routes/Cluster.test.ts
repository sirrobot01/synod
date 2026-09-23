import { cleanup, fireEvent, render, screen } from "@testing-library/svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import Cluster from "./Cluster.svelte";
import { api, type ClusterMember } from "../lib/api";

vi.mock("../lib/api", async () => {
  const real = await vi.importActual<typeof import("../lib/api")>("../lib/api");
  return {
    ...real,
    api: {
      cluster: () =>
        Promise.resolve({
          id: "11111111-1111-4111-8111-111111111111",
          installation_id: "99999999-9999-4999-8999-999999999999", execution_epoch: 0, created_at: "2026-09-21T09:00:00Z", deleted_at: null,
          spec: { schema_version: 1, name: "pg-core", engine: "postgres", postgresql: { version: "17.11", topology: "standalone" }, redis: null },
          desired_revision: 14,
          desired_lifecycle: "running", desired_replicas: 0,
          observed_revision: 14,
          observed_lifecycle: "running",
          updated_at: new Date().toISOString(),
        }),
      clusterMembers: () => Promise.resolve([]),
      clusterEndpoints: () => Promise.resolve({ cluster_id: "11111111-1111-4111-8111-111111111111", engine: "postgres", desired_revision: 14, observed_revision: 14, desired_lifecycle: "running", observed_lifecycle: "running", endpoints: [], unavailable_reason: "client_address_required" }),
      clusterCredentials: () => Promise.resolve({ cluster_id: "11111111-1111-4111-8111-111111111111", credentials: [] }),
    },
  };
});

function open() {
  render(Cluster, { id: "11111111-1111-4111-8111-111111111111", actorID: "test-admin", onOpenTopology: () => {}, onOpenOperation: () => {} });
}

async function settle() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

/** press clicks a control by name and lets the screen redraw. */
async function press(name: RegExp | string) {
  (screen.getByRole("button", { name }) as HTMLButtonElement).click();
  await settle();
}

beforeEach(() => {
  cleanup();
  sessionStorage.clear();
});

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

describe("the unsupported upgrade preview", () => {
  it("names a point of no return on an upgrade, and promises no rollback past it", async () => {
    open();
    await settle();
    await press("Upgrade…");
    expect(screen.getByText(/point of no return/)).toBeTruthy();
    expect(screen.getByText("No rollback after step 3")).toBeTruthy();
    expect(screen.getByText(/cannot be returned to 15\.8/)).toBeTruthy();
    expect((screen.getByRole("button", { name: "Upgrade unavailable" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("closes without acting, so a dialog is never a commitment", async () => {
    open();
    await settle();
    await press("Upgrade…");
    await press("Cancel");
    expect(screen.queryByRole("dialog")).toBe(null);
  });
});

describe("dialog keyboard navigation", () => {
  it("keeps Tab in the modal and returns focus after Escape", async () => {
    open();
    await settle();
    const opener = screen.getByRole("button", { name: "Upgrade…" });
    opener.focus();
    await press("Upgrade…");
    const dialog = screen.getByRole("dialog");
    expect(document.activeElement).toBe(dialog);
    await fireEvent.keyDown(window, { key: "Tab" });
    expect(document.activeElement).toBe(screen.getByRole("button", { name: "Close" }));
    await fireEvent.keyDown(window, { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(screen.getByRole("button", { name: "Cancel" }));
    await fireEvent.keyDown(window, { key: "Tab" });
    expect(document.activeElement).toBe(screen.getByRole("button", { name: "Close" }));
    await fireEvent.keyDown(window, { key: "Escape" });
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(document.activeElement).toBe(opener);
  });
});

describe("the cluster screen", () => {
  it("shows real stored members with explicit native observation limits", async () => {
    const memberID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    vi.spyOn(api, "clusterMembers").mockResolvedValue([{ id: memberID, role: "replica", state: "residual" }]);
    open();
    expect(await screen.findByText(memberID)).toBeTruthy();
    expect(screen.getByText("declared_role")).toBeTruthy();
    expect(screen.getByText("residual")).toBeTruthy();
    expect(screen.getByText(/These records contain no native observation time/)).toBeTruthy();
    expect(screen.queryByText("pg-core-i2")).toBeNull();
  });

  it("keeps membership unavailable after a failed read", async () => {
    vi.spyOn(api, "clusterMembers").mockRejectedValue(new TypeError("offline"));
    open();
    expect((await screen.findAllByText("transport_error")).length).toBeGreaterThan(0);
    expect(screen.queryByText(/^No stored members/)).toBeNull();
    expect(screen.queryByText("pg-core-i2")).toBeNull();
  });

  it("does not display a delayed member response on a different cluster", async () => {
    let finish!: (value: ClusterMember[]) => void;
    const late = new Promise<ClusterMember[]>((resolve) => { finish = resolve; });
    const first = "11111111-1111-4111-8111-111111111111";
    const second = "22222222-2222-4222-8222-222222222222";
    const old = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const current = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const stored = await api.cluster(first);
    vi.spyOn(api, "cluster").mockResolvedValueOnce(stored).mockResolvedValueOnce({ ...stored, id: second });
    vi.spyOn(api, "clusterMembers").mockReturnValueOnce(late).mockResolvedValueOnce([{ id: current, role: "primary", state: "active" }]);
    const props = { id: first, actorID: "test-admin", onOpenTopology: () => {}, onOpenOperation: () => {} };
    const view = render(Cluster, props);
    await screen.findByText("Reading stored members.");
    await view.rerender({ ...props, id: second });
    await screen.findByText(current);
    finish([{ id: old, role: "primary", state: "active" }]);
    await settle();
    expect(screen.queryByText(old)).toBeNull();
    expect(screen.getByText(current)).toBeTruthy();
  });

  it("does not offer actions on a response for a different cluster", async () => {
    vi.spyOn(api, "cluster").mockResolvedValueOnce({ id: "22222222-2222-4222-8222-222222222222", installation_id: "99999999-9999-4999-8999-999999999999", execution_epoch: 0, created_at: "2026-09-21T09:00:00Z", deleted_at: null, spec: { schema_version: 1, name: "wrong", engine: "postgres", postgresql: { version: "17.11", topology: "standalone" }, redis: null }, desired_revision: 1, desired_replicas: 0, observed_revision: 0, desired_lifecycle: "running", observed_lifecycle: "unknown", updated_at: new Date().toISOString() });
    open();
    await settle();
    expect(screen.getAllByText("invalid_response").length).toBeGreaterThan(0);
    expect((screen.getByRole("button", { name: "Start" }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.queryByRole("heading", { name: "wrong" })).toBeNull();
  });

  it("draws desired and observed apart, each with its own revision", async () => {
    open();
    await settle();
    // A single collapsed badge would have to pick a winner between the two.
    expect(screen.getByText("desired")).toBeTruthy();
    expect(screen.getByText("observed")).toBeTruthy();
    expect(screen.getByText("converged")).toBeTruthy();
  });

  it("says the verdict measures a write time, because the API serves no observation time", async () => {
    open();
    await settle();
    expect(screen.getByText(/no observation time or reason on a cluster/)).toBeTruthy();
  });
});
