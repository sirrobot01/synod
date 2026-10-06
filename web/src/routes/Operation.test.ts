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

function operation(id: string, status: string, kind = "backup", attempts = 1) {
  return {
    id, app_id: "c1", kind, status, node_id: "n1", actor_id: "admin",
    failure_code: "",
    finished_at: status === "succeeded" ? "2026-09-19T13:00:00Z" : null,
    created_at: "2026-09-19T12:00:00Z", updated_at: "2026-09-19T12:30:00Z",
  };
}

function open(id: string) {
  return render(Operation, { id });
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
    op_running: operation("op_running", "running", "restore", 9),
    op_done: operation("op_done", "succeeded", "backup", 1),
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
    await view.rerender({ id: "op_done" });
    await settle();
    hold = false;
    // The request for the open address answers first, and the stale one answers
    // after it. Without the guard the stale answer wins, because it lands last.
    held.reverse().forEach((release) => release());
    await settle();
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("op_done");
    expect(screen.getByText("backup")).toBeTruthy();
    expect(screen.queryByText("restore")).toBe(null);
  });
});

describe("a failed operation", () => {
  beforeEach(() => {
    answers = {
      op_failed: {
        ...operation("op_failed", "failed", "backup", 3),
        failure_code: "dependency_unavailable",
        finished_at: "2026-09-19T13:00:00Z",
      },
      op_pending: { ...operation("op_pending", "pending", "backup", 0), plan_version: 0 },
      op_waiting: operation("op_waiting", "waiting", "restore", 1),
    };
  });

  it("shows the failure code verbatim, because that is what an operator greps for", async () => {
    open("op_failed");
    await settle();
    expect(screen.getByText("dependency_unavailable")).toBeTruthy();
  });

  it("does not invent resources or offer unsupported actions", async () => {
    open("op_failed");
    await settle();
    expect(screen.queryByText(/residual resources/i)).toBe(null);
    expect(screen.queryByRole("button", {name:/retry/i})).toBe(null);
    expect(screen.queryByRole("button", {name:/cancel/i})).toBe(null);
  });
  it("shows pending work without claiming completion", async () => {
    open("op_pending"); await settle();
    expect(screen.getByText("The request is recorded. Work has not started.")).toBeTruthy();
  });
  it("reports uncertainty when work needs inspection", async () => {
    open("op_waiting"); await settle();
    expect(screen.getByText(/The result is unknown/)).toBeTruthy();
  });
});
