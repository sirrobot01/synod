import { cleanup, render, screen } from "@testing-library/svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import Operation from "./Operation.svelte";

let reads: string[] = [];
let answers: Record<string, unknown> = {};
let held: (() => void)[] = [];
let hold = false;

vi.mock("../lib/api", async () => {
  const real = await vi.importActual<typeof import("../lib/api")>("../lib/api");
  return {
    ...real,
    api: {
      operation: (id: string) => {
        reads.push(id);
        if (hold) return new Promise((resolve) => held.push(() => resolve(answers[id])));
        return Promise.resolve(answers[id]);
      },
    },
  };
});

function operation(id: string, status: string, kind = "cluster.update", attempts = 1) {
  return {
    id, cluster_id: "c1", kind, status, plan_version: 2, desired_revision: 3,
    attempts, failure_code: null, cancellation_requested_at: null,
    finished_at: status === "succeeded" ? "2026-09-19T13:00:00Z" : null,
    created_at: "2026-09-19T12:00:00Z", updated_at: "2026-09-19T12:30:00Z",
  };
}

function open(id: string) {
  return render(Operation, { id, onOpenCluster: () => {} });
}

async function settle() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

beforeEach(() => {
  reads = [];
  held = [];
  hold = false;
  // The two answers differ in their fields, so the screen shows which one it drew.
  answers = {
    op_running: operation("op_running", "running", "cluster.delete", 9),
    op_done: operation("op_done", "succeeded", "cluster.update", 1),
  };
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  cleanup();
});

describe("reading an operation", () => {
  it("reads again while the operation can still change", async () => {
    open("op_running");
    await settle();
    expect(reads).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(5000);
    expect(reads.length).toBeGreaterThan(1);
  });

  it("stops reading once no owner can change the status", async () => {
    open("op_done");
    await settle();
    const first = reads.length;
    // A finished operation never changes again. Reading it forever would spend
    // the request budget on an answer that cannot differ.
    await vi.advanceTimersByTimeAsync(60000);
    expect(reads).toHaveLength(first);
  });

  it("does not draw an answer that belongs to a previous address", async () => {
    hold = true;
    const view = open("op_running");
    await settle();
    // The address moves on while the first request is still open.
    await view.rerender({ id: "op_done", onOpenCluster: () => {} });
    await settle();
    hold = false;
    // The request for the open address answers first, and the stale one answers
    // after it. Without the guard the stale answer wins, because it lands last.
    held.reverse().forEach((release) => release());
    await settle();
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("op_done");
    expect(screen.getByText("cluster.update")).toBeTruthy();
    expect(screen.queryByText("cluster.delete")).toBe(null);
  });
});

describe("a failed operation", () => {
  beforeEach(() => {
    answers = {
      op_failed: {
        ...operation("op_failed", "failed", "cluster.stop", 3),
        failure_code: "dependency_unavailable",
        finished_at: "2026-09-19T13:00:00Z",
      },
      op_pending: { ...operation("op_pending", "pending", "cluster.create", 0), plan_version: 0 },
      op_waiting: operation("op_waiting", "waiting", "cluster.upgrade", 1),
    };
  });

  it("shows the failure code verbatim, because that is what an operator greps for", async () => {
    open("op_failed");
    await settle();
    expect(screen.getByText("dependency_unavailable")).toBeTruthy();
  });

  it("claims residual resources only where work could have left some", async () => {
    // A pending operation has provisioned nothing. Listing retained objects under
    // it would assert an effect that never happened.
    open("op_pending");
    await settle();
    expect(screen.queryByText(/residual resources/i)).toBe(null);
    cleanup();
    open("op_failed");
    await settle();
    expect(screen.getByText(/residual resources/i)).toBeTruthy();
    expect(screen.getByText("vol_8812")).toBeTruthy();
  });

  it("separates acceptance from completion", async () => {
    open("op_pending");
    await settle();
    // Acceptance is its own state: recorded, planned for nothing, claiming nothing.
    expect(screen.getByText("accepted")).toBeTruthy();
    expect(screen.getByText("0 — no executable plan yet")).toBeTruthy();
  });

  it("offers no action the API cannot serve, whatever the status permits", async () => {
    open("op_failed");
    await settle();
    // A failed operation permits a retry, and the route does not exist. The
    // button says which of the two is missing rather than pretending to work.
    const retry = screen.getByRole("button", { name: /retry/i }) as HTMLButtonElement;
    expect(retry.disabled).toBe(true);
    expect(retry.textContent).toContain("no API route");
    const cancel = screen.getByRole("button", { name: /cancel/i }) as HTMLButtonElement;
    expect(cancel.disabled).toBe(true);
    expect(cancel.textContent).toContain("not permitted");
  });

  it("promises no progress on a waiting operation", async () => {
    open("op_waiting");
    await settle();
    // A spinner would promise progress that an unresolved effect cannot promise.
    expect(screen.getByText(/not a queue and not a spinner/i)).toBeTruthy();
    // Waiting is not a failure, so no retry is offered to resolve it.
    expect((screen.getByRole("button", { name: /retry/i }) as HTMLButtonElement).textContent).toContain("not permitted");
  });
});
