import { describe, expect, it } from "vitest";
import {
  accepted,
  canCancel,
  canRetry,
  conditionOrder,
  errorView,
  failing,
  finished,
  freshness,
  operationMark,
  unresolved,
  verdict,
  viewCondition,
  viewConditions,
  type Condition,
} from "./state";

const now = new Date("2026-09-19T14:00:00Z");

function at(secondsAgo: number): string {
  return new Date(now.getTime() - secondsAgo * 1000).toISOString();
}

function condition(overrides: Partial<Condition> = {}): Condition {
  return { name: "Ready", value: "true", observedAt: at(4), observedRevision: 14, reason: "postconditions_met", ...overrides };
}

describe("freshness", () => {
  it("ages before it expires, so decay is visible", () => {
    expect(freshness(6)).toBe("fresh");
    expect(freshness(48)).toBe("ageing");
    expect(freshness(252)).toBe("expired");
    expect(freshness(60)).toBe("expired");
  });

  it("treats an unreadable observation time as expired", () => {
    const view = viewCondition(condition({ observedAt: "" }), now);
    expect(view.freshness).toBe("expired");
    expect(view.display).toBe("unknown");
  });
});

describe("conditions", () => {
  it("keeps the last known value of an expired observation as history", () => {
    const view = viewCondition(condition({ value: "true", observedAt: at(252) }), now);
    expect(view.display).toBe("unknown");
    expect(view.lastKnown).toBe("true");
    expect(view.observedRevision).toBe(14);
    expect(view.reason).toBe("postconditions_met");
  });

  it("does not invent history for a value that was already unknown", () => {
    const view = viewCondition(condition({ value: "unknown", observedAt: at(252) }), now);
    expect(view.lastKnown).toBeUndefined();
  });

  it("returns every condition in the fixed order, reporting the absent ones as unknown", () => {
    const views = viewConditions([condition({ name: "Replicated", value: "false", reason: "replica_lag_92s" })], now);
    expect(views.map((view) => view.name)).toEqual([...conditionOrder]);
    const replicated = views.find((view) => view.name === "Replicated");
    expect(replicated?.display).toBe("false");
    expect(replicated?.reason).toBe("replica_lag_92s");
    // A condition the server did not report is unknown, never quietly absent.
    expect(views.find((view) => view.name === "BackupCurrent")?.display).toBe("unknown");
    expect(views.find((view) => view.name === "BackupCurrent")?.reason).toBe("not_reported");
  });

  it("never sums conditions into one indicator", () => {
    // The design's own example: reachable primary, broken replication, unknown backup.
    const views = viewConditions(
      [
        condition({ name: "Ready", value: "true" }),
        condition({ name: "Reachable", value: "true", reason: "tcp_probe_ok" }),
        condition({ name: "Replicated", value: "false", observedAt: at(11), reason: "replica_lag_92s" }),
        condition({ name: "BackupCurrent", value: "unknown", observedAt: at(11064), reason: "target_unreachable" }),
        condition({ name: "EndpointReady", value: "true", observedAt: at(6), reason: "haproxy_backend_up" }),
      ],
      now,
    );
    expect(failing(views)).toBe(1);
    expect(unresolved(views)).toBe(1);
    // Three separate facts. No single verdict exists for this cluster.
    expect(views.filter((view) => view.display === "true")).toHaveLength(3);
  });

  it("does not let an expired observation count as a failure", () => {
    const views = viewConditions([condition({ name: "Ready", value: "false", observedAt: at(600) })], now);
    expect(failing(views)).toBe(0);
    expect(unresolved(views)).toBe(5);
  });

  it("gives every mark a shape as well as a glyph", () => {
    const shapes = viewConditions([], now).map((view) => view.mark.shape);
    expect(shapes.every((shape) => shape.length > 0)).toBe(true);
  });
});

describe("divergence", () => {
  const base = { desired: "running", desiredRevision: 15, observed: "stopped", observedRevision: 14, observedAt: at(4) };

  it("reports agreement as converged", () => {
    expect(verdict({ ...base, observed: "running", observedRevision: 15 }, now)).toBe("converged");
  });

  it("treats a difference that an operation owns as progress, not an error", () => {
    expect(verdict({ ...base, owningOperation: "op_7c14f2a9" }, now)).toBe("in_progress");
  });

  it("calls a fresh contradicting observation an error", () => {
    expect(verdict(base, now)).toBe("contradicted");
  });

  it("treats a revision that lags but agrees as progress, not a contradiction", () => {
    // The agent has not seen revision 15 yet. Nothing contradicts anything.
    expect(verdict({ ...base, observed: "running", observedRevision: 14 }, now)).toBe("in_progress");
  });

  it("reports an expired observation as stale rather than contradicting", () => {
    expect(verdict({ ...base, observedAt: at(600) }, now)).toBe("stale");
  });
});

describe("operations", () => {
  it("separates acceptance from completion", () => {
    expect(accepted("pending", 0)).toBe(true);
    expect(accepted("pending", 1)).toBe(false);
    expect(accepted("running", 0)).toBe(false);
    expect(operationMark("pending", 0).label).toBe("accepted");
  });

  it("marks waiting without promising progress", () => {
    const mark = operationMark("waiting", 1);
    // A hatched diamond, never a spinner: motion would promise progress.
    expect(mark.shape).toBe("diamond-hatched");
    expect(mark.label).toBe("waiting");
  });

  it("knows which statuses no owner can change", () => {
    expect(["succeeded", "failed", "cancelled"].every((status) => finished(status as never))).toBe(true);
    expect(["pending", "running", "waiting", "cancelling"].some((status) => finished(status as never))).toBe(false);
  });

  it("offers an action only when the API accepts it", () => {
    expect(canCancel("running", false)).toBe(true);
    expect(canCancel("running", true)).toBe(false);
    expect(canCancel("cancelling", false)).toBe(false);
    expect(canCancel("succeeded", false)).toBe(false);
    expect(canRetry("failed")).toBe(true);
    expect(canRetry("running")).toBe(false);
  });
});

describe("errors", () => {
  it("keeps the stable code verbatim and carries the request identity", () => {
    const view = errorView({ code: "operation_conflict", message: "The request is invalid.", requestID: "req_4b19c0e2", operationID: "op_7c14f2a9" });
    expect(view.code).toBe("operation_conflict");
    expect(view.consequence).toContain("one mutating operation");
    expect(view.requestID).toBe("req_4b19c0e2");
    expect(view.operationID).toBe("op_7c14f2a9");
  });

  it("falls back to the server message for a code it does not know", () => {
    const view = errorView({ code: "some_future_code", message: "A later server said this.", requestID: "req_1" });
    expect(view.code).toBe("some_future_code");
    expect(view.consequence).toBe("A later server said this.");
  });
});
