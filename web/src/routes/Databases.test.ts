import { cleanup, render, screen } from "@testing-library/svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Failure } from "../lib/api";
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
