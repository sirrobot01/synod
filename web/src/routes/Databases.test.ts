import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api, Failure, type Cluster, type ClusterPage } from "../lib/api";
import Databases from "./Databases.svelte";

// The client is replaced, because this test is about what the screen sends, not
// about the network. Every call is recorded with the key that went with it.
const sent: { spec: Record<string, unknown>; key: string }[] = [];
let answer: () => unknown = () => ({ cluster_id: "c1", operation_id: "o1", desired_revision: 1, accepted_at: "", replayed: false });

vi.mock("../lib/api", async () => {
  const real = await vi.importActual<typeof import("../lib/api")>("../lib/api");
  return {
    ...real,
    api: {
      clusters: vi.fn(),
      createCluster: (spec: Record<string, unknown>, key: string) => {
        sent.push({ spec, key });
        const value = answer();
        return value instanceof Error ? Promise.reject(value) : Promise.resolve(value);
      },
    },
  };
});

/** reopen presses the drawer control, which is where the form lives. */
async function reopen() {
  (screen.getByRole("button", { name: "Create cluster" }) as HTMLButtonElement).click();
  await Promise.resolve();
}

async function open() {
  render(Databases, { onOpen: () => {}, onOpenOperation: () => {} });
  // The design puts creation in a drawer, so the form exists once it is opened.
  await reopen();
}

/** type edits the name, which is an edit to the request body. */
function type(name: string) {
  const field = screen.getByLabelText(/^Name/) as HTMLInputElement;
  field.value = name;
  field.dispatchEvent(new Event("input", { bubbles: true }));
}

/** submit presses Create without touching the body, which is what a retry is. */
async function submit() {
  (screen.getByRole("button", { name: /returns 202/i }) as HTMLButtonElement).click();
  // One turn for the handler, one for the awaited call.
  await Promise.resolve();
  await Promise.resolve();
}

beforeEach(() => {
  vi.mocked(api.clusters).mockReset().mockResolvedValue({ items: [], next_cursor: "" });
  sent.length = 0;
  answer = () => ({ cluster_id: "c1", operation_id: "o1", desired_revision: 1, accepted_at: "", replayed: false });
});

afterEach(cleanup);

describe("the idempotency key", () => {
  it("repeats the same key after a failure, so a retry cannot admit a second database", async () => {
    answer = () => new Failure(503, { code: "outcome_unknown", message: "", requestID: "req_1" });
    await open();
    type("orders-eu");
    await submit();
    await submit();
    expect(sent).toHaveLength(2);
    // A new key here would admit a second database for a request the server may
    // already have accepted. That is the whole reason the key is kept.
    expect(sent[1]?.key).toBe(sent[0]?.key);
  });

  it("takes a new key once the body changes", async () => {
    answer = () => new Failure(503, { code: "outcome_unknown", message: "", requestID: "req_1" });
    await open();
    type("orders-eu");
    await submit();
    type("ledger");
    await submit();
    expect(sent[1]?.key).not.toBe(sent[0]?.key);
  });

  it("takes a new key after the work is accepted", async () => {
    await open();
    type("orders-eu");
    await submit();
    // Acceptance replaces the form, so a second create starts from the drawer
    // again. The body is unchanged; only the spent key must not come back.
    (screen.getByRole("button", { name: "Back to clusters" }) as HTMLButtonElement).click();
    await reopen();
    await submit();
    expect(sent).toHaveLength(2);
    // The first body is spent. Reusing its key would replay the first admission
    // and report an accepted database that this request never created.
    expect(sent[1]?.key).not.toBe(sent[0]?.key);
  });

  it("sends the engine specification that the API expects", async () => {
    await open();
    type("orders-eu");
    await submit();
    expect(sent[0]?.spec).toMatchObject({ schema_version: 1, name: "orders-eu", engine: "postgres", redis: null });
    expect(sent[0]?.spec["postgresql"]).toMatchObject({ topology: "standalone" });
  });
});

describe("the engine choice", () => {
  /** pick presses an engine button, which rewrites the body and its version. */
  async function pick(engine: string) {
    (screen.getByRole("button", { name: engine }) as HTMLButtonElement).click();
    await Promise.resolve();
  }

  it("sends the Redis specification, with the PostgreSQL branch left null", async () => {
    await open();
    await pick("Redis");
    type("sessions");
    await submit();
    // The two engine branches are exclusive. Sending both would let the server
    // read a specification the operator never asked for.
    expect(sent[0]?.spec).toMatchObject({ schema_version: 1, name: "sessions", engine: "redis", postgresql: null });
    expect(sent[0]?.spec["redis"]).toMatchObject({ topology: "standalone", persistence: "rdb" });
  });

  it("offers a version this server qualifies for each engine", async () => {
    await open();
    const version = () => (screen.getByLabelText("Version") as HTMLInputElement).value;
    // A default the server refuses makes the first create fail for no reason.
    expect(version()).toBe("17.11");
    await pick("Redis");
    expect(version()).toBe("8.2.7");
  });

  it("names every topology the engine has, and admits only the one this server takes", async () => {
    await open();
    // The shapes are drawn so the client mode each one forces is visible, and
    // refused so no request can carry a topology the server would reject.
    expect(screen.getByText("Patroni HA")).toBeTruthy();
    expect(screen.getAllByText("not admitted by this server yet")).toHaveLength(2);
    await pick("Redis");
    expect(screen.getByText("Sentinel")).toBeTruthy();
    expect(screen.getByText("Redis Cluster")).toBeTruthy();
    expect(screen.getAllByText("not admitted by this server yet")).toHaveLength(2);
  });

  it("takes a new key when the engine changes, because that is a different body", async () => {
    answer = () => new Failure(503, { code: "outcome_unknown", message: "", requestID: "req_1" });
    await open();
    type("sessions");
    await submit();
    await pick("Redis");
    await submit();
    expect(sent[1]?.key).not.toBe(sent[0]?.key);
  });
});


const cluster: Cluster = { id: "11111111-1111-4111-8111-111111111111", installation_id: "22222222-2222-4222-8222-222222222222",
  spec: { schema_version: 1, name: "orders-live", engine: "postgres", postgresql: { version: "17.11", topology: "standalone" }, redis: null },
  desired_revision: 2, desired_lifecycle: "running", desired_replicas: 0, observed_revision: 1, observed_lifecycle: "unknown", execution_epoch: 1,
  created_at: "2026-09-20T10:00:00Z", updated_at: "2026-09-21T10:00:00Z", deleted_at: null };
const redis: Cluster = { ...cluster, id: "33333333-3333-4333-8333-333333333333", spec: {
  schema_version: 1, name: "cache-live", engine: "redis", postgresql: null, redis: { version: "8.2.7", topology: "standalone", persistence: "aof" } } };

function pendingPage() {
  let resolve!: (page: ClusterPage) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<ClusterPage>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

describe("the live cluster collection", () => {
  it("shows loading, then an empty result without sample rows or a continuation", async () => {
    const pending = pendingPage();
    vi.mocked(api.clusters).mockReturnValueOnce(pending.promise);
    render(Databases, { onOpen: vi.fn(), onOpenOperation: vi.fn() });
    expect(screen.getByRole("status").textContent).toContain("Loading clusters");
    expect(api.clusters).toHaveBeenCalledExactlyOnceWith("", false);
    expect(screen.queryByText("pg-core")).toBeNull();
    pending.resolve({ items: [], next_cursor: "" });
    expect(await screen.findByText("No clusters found with completed deletions excluded.")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Load more" })).toBeNull();
  });

  it("shows real configuration and stored revisions without inventing health or observation age", async () => {
    const onOpen = vi.fn();
    vi.mocked(api.clusters).mockResolvedValueOnce({ items: [cluster, redis], next_cursor: "" });
    const { container } = render(Databases, { onOpen, onOpenOperation: vi.fn() });
    const link = await screen.findByRole("link", { name: "orders-live" });
    expect(link.getAttribute("href")).toContain(cluster.id);
    expect(screen.getByText("PostgreSQL 17.11")).toBeTruthy();
    expect(screen.getByText("Redis 8.2.7")).toBeTruthy();
    expect(screen.getByText("Persistence: aof")).toBeTruthy();
    expect(screen.getAllByText("Desired: running · rev 2")).toHaveLength(2);
    expect(screen.getAllByText("Stored: unknown · rev 1")).toHaveLength(2);
    expect(screen.getAllByText("Health and observation time unavailable")).toHaveLength(2);
    expect(screen.getByText("2 loaded")).toBeTruthy();
    expect(screen.getByText("Row updated")).toBeTruthy();
    expect(container.querySelectorAll("time")[0]?.getAttribute("datetime")).toBe(cluster.updated_at);
    expect(screen.queryByText("Conditions")).toBeNull();
    expect(screen.queryByText("Size")).toBeNull();
    // Let the component handle the click before suppressing jsdom navigation.
    window.addEventListener("click", (event) => {
      expect(event.defaultPrevented).toBe(false);
      event.preventDefault();
    }, { once: true });
    await fireEvent.click(link, { ctrlKey: true });
    expect(onOpen).not.toHaveBeenCalled();
    await fireEvent.click(link);
    expect(onOpen).toHaveBeenCalledExactlyOnceWith(cluster.id);
  });

  it("loads one continuation, deduplicates IDs, and prevents simultaneous load-more reads", async () => {
    const pending = pendingPage();
    vi.mocked(api.clusters).mockResolvedValueOnce({ items: [cluster], next_cursor: "page-2" }).mockReturnValueOnce(pending.promise);
    render(Databases, { onOpen: vi.fn(), onOpenOperation: vi.fn() });
    await screen.findByRole("link", { name: "orders-live" });
    const more = screen.getByRole("button", { name: "Load more" });
    await fireEvent.click(more);
    expect((more as HTMLButtonElement).disabled).toBe(true);
    expect(api.clusters).toHaveBeenLastCalledWith("page-2", false);
    pending.resolve({ items: [cluster, redis], next_cursor: "" });
    await screen.findByRole("link", { name: "cache-live" });
    expect(screen.getAllByRole("link", { name: "orders-live" })).toHaveLength(1);
    expect(screen.getByText("2 loaded")).toBeTruthy();
    expect(api.clusters).toHaveBeenCalledTimes(2);
    expect(screen.queryByRole("button", { name: "Load more" })).toBeNull();
  });

  it("preserves loaded rows after a later-page failure and explicitly retries the same cursor", async () => {
    vi.mocked(api.clusters).mockResolvedValueOnce({ items: [cluster], next_cursor: "page-2" })
      .mockRejectedValueOnce(new Failure(503, { code: "dependency_unavailable", message: "A required service is unavailable.", requestID: "" }))
      .mockResolvedValueOnce({ items: [redis], next_cursor: "" });
    render(Databases, { onOpen: vi.fn(), onOpenOperation: vi.fn() });
    await screen.findByRole("link", { name: "orders-live" });
    await fireEvent.click(screen.getByRole("button", { name: "Load more" }));
    expect((await screen.findByRole("alert")).textContent).toContain("dependency_unavailable");
    expect(screen.getByRole("link", { name: "orders-live" })).toBeTruthy();
    expect(api.clusters).toHaveBeenCalledTimes(2);
    await fireEvent.click(screen.getByRole("button", { name: "Retry list read" }));
    await screen.findByRole("link", { name: "cache-live" });
    expect(api.clusters).toHaveBeenLastCalledWith("page-2", false);
    expect(api.clusters).toHaveBeenCalledTimes(3);
  });

  it("offers an explicit first-page retry without presenting failure as an empty collection", async () => {
    vi.mocked(api.clusters).mockRejectedValueOnce(new Failure(401, { code: "unauthenticated", message: "Sign in again.", requestID: "" }));
    render(Databases, { onOpen: vi.fn(), onOpenOperation: vi.fn() });
    expect((await screen.findByRole("alert")).textContent).toContain("Sign in again.");
    expect(screen.queryByText(/No clusters found/)).toBeNull();
    await fireEvent.click(screen.getByRole("button", { name: "Retry list read" }));
    expect(await screen.findByText(/No clusters found/)).toBeTruthy();
    expect(api.clusters).toHaveBeenLastCalledWith("", false);
  });

  it("resets pages on refresh and ignores an older continuation", async () => {
    const old = pendingPage();
    vi.mocked(api.clusters).mockResolvedValueOnce({ items: [cluster], next_cursor: "page-2" }).mockReturnValueOnce(old.promise)
      .mockResolvedValueOnce({ items: [redis], next_cursor: "" });
    render(Databases, { onOpen: vi.fn(), onOpenOperation: vi.fn() });
    await screen.findByRole("link", { name: "orders-live" });
    await fireEvent.click(screen.getByRole("button", { name: "Load more" }));
    await fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await screen.findByRole("link", { name: "cache-live" });
    expect(api.clusters).toHaveBeenLastCalledWith("", false);
    old.resolve({ items: [cluster], next_cursor: "stale-page-3" });
    await waitFor(() => expect(screen.queryByRole("status")).toBeNull());
    expect(screen.queryByRole("link", { name: "orders-live" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Load more" })).toBeNull();
  });

  it("binds Show deleted to a new first page and ignores stale success and failure after filter changes", async () => {
    const first = pendingPage(), second = pendingPage();
    vi.mocked(api.clusters).mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise)
      .mockResolvedValueOnce({ items: [cluster], next_cursor: "" });
    render(Databases, { onOpen: vi.fn(), onOpenOperation: vi.fn() });
    await fireEvent.click(screen.getByRole("checkbox", { name: "Show deleted" }));
    expect(api.clusters).toHaveBeenLastCalledWith("", true);
    await fireEvent.click(screen.getByRole("checkbox", { name: "Show deleted" }));
    await screen.findByRole("link", { name: "orders-live" });
    expect(api.clusters).toHaveBeenLastCalledWith("", false);
    first.resolve({ items: [redis], next_cursor: "stale" });
    second.reject(new Failure(503, { code: "dependency_unavailable", message: "Old failure", requestID: "" }));
    await waitFor(() => expect(screen.queryByRole("status")).toBeNull());
    expect(screen.queryByRole("link", { name: "cache-live" })).toBeNull();
    expect(screen.queryByRole("alert")).toBeNull();
    expect(screen.queryByRole("button", { name: "Load more" })).toBeNull();
  });

  it("shows a completed deletion only when its page is requested", async () => {
    vi.mocked(api.clusters).mockResolvedValueOnce({ items: [], next_cursor: "" }).mockResolvedValueOnce({ items: [{ ...cluster,
      desired_lifecycle: "deleted", observed_lifecycle: "deleted", deleted_at: cluster.updated_at }], next_cursor: "" });
    render(Databases, { onOpen: vi.fn(), onOpenOperation: vi.fn() });
    await screen.findByText(/No clusters found/);
    await fireEvent.click(screen.getByRole("checkbox", { name: "Show deleted" }));
    expect(await screen.findByText("Deleted record")).toBeTruthy();
    expect(screen.getByText("Stored: deleted · rev 1")).toBeTruthy();
  });

  it("discards a pending read after navigation destroys the screen", async () => {
    const old = pendingPage();
    vi.mocked(api.clusters).mockReturnValueOnce(old.promise).mockResolvedValueOnce({ items: [redis], next_cursor: "" });
    const first = render(Databases, { onOpen: vi.fn(), onOpenOperation: vi.fn() });
    first.unmount();
    render(Databases, { onOpen: vi.fn(), onOpenOperation: vi.fn() });
    await screen.findByRole("link", { name: "cache-live" });
    old.resolve({ items: [cluster], next_cursor: "old" });
    await waitFor(() => expect(screen.queryByRole("status")).toBeNull());
    expect(screen.queryByRole("link", { name: "orders-live" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Load more" })).toBeNull();
  });
});


describe("standalone PostgreSQL", () => {
  it("offers no replication policy, because a standalone cluster has no replicas", async () => {
    answer = () => new Failure(503, { code: "outcome_unknown", message: "", requestID: "req_policy" });
    await open();
    type("orders-eu");
    expect(screen.queryByLabelText("Replication policy")).toBeNull();
    await submit();
    expect(sent[0]?.spec["postgresql"]).toEqual({ version: "17.11", topology: "standalone" });
  });
});
