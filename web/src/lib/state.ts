// The state system that every screen depends on.
//
// It follows the approved design in tmp/design and specification 17. Three rules
// shape all of it. Conditions are never summed into one indicator. An observation
// that ages out changes only what is displayed as observed, never desired state and
// never a failure count. A mark always carries a shape and a glyph, so colour is
// never the only signal.

/** A value that an observation can carry. Unknown is a real answer, not a missing one. */
export type Truth = "true" | "false" | "unknown";

/** How recent an observation is, against its own limit. */
export type Freshness = "fresh" | "ageing" | "expired";

/** The five conditions, in the fixed order that the design requires. */
export const conditionOrder = ["Ready", "Reachable", "Replicated", "BackupCurrent", "EndpointReady"] as const;

export type ConditionName = (typeof conditionOrder)[number];

/** One observed condition. Reason, time, and revision belong in the row, not a tooltip. */
export interface Condition {
  name: ConditionName;
  value: Truth;
  /** Server time of the observation, as an ISO 8601 string. */
  observedAt: string;
  observedRevision: number;
  /** A stable code such as replica_lag_92s. Never paraphrase it away. */
  reason: string;
}

/** A condition as a screen should draw it, after freshness is applied. */
export interface ConditionView extends Condition {
  freshness: Freshness;
  ageSeconds: number;
  /** The value to draw. An expired observation draws unknown and keeps its history. */
  display: Truth;
  /** The last known value of an expired observation. It is demoted, never deleted. */
  lastKnown?: Truth;
  mark: Mark;
}

/**
 * The silhouettes the design draws. An outline is an intention, a solid is an
 * observation, and a dash is uncertainty. A diamond is reserved for known-bad, so
 * failure has one silhouette nothing else uses.
 */
export type Shape =
  | "circle"
  | "circle-open"
  | "circle-dashed"
  | "circle-half"
  | "square"
  | "square-open"
  | "square-half"
  | "square-slash"
  | "square-open-slash"
  | "triangle"
  | "diamond"
  | "diamond-open"
  | "diamond-hatched";

/** The five semantic colour slots, plus the two that carry no judgement. */
export type Tone = "ok" | "attn" | "fail" | "active" | "unc" | "neutral" | "dim";

/** A mark pairs a glyph with a shape so that colour is never the only signal. */
export interface Mark {
  glyph: string;
  shape: Shape;
  tone: Tone;
  label: string;
}

export const marks: Record<Truth, Mark> = {
  true: { glyph: "✓", shape: "circle", tone: "ok", label: "true" },
  // A diamond, because a known failure earns a silhouette of its own.
  false: { glyph: "✕", shape: "diamond", tone: "fail", label: "false" },
  unknown: { glyph: "?", shape: "circle-dashed", tone: "neutral", label: "unknown" },
};

/**
 * desiredMark draws what the operator asked for. It is always an outline, because
 * an intention is not an observation and must never be mistaken for one.
 */
export function desiredMark(lifecycle: string): Mark {
  switch (lifecycle) {
    case "running":
      return { glyph: "→", shape: "circle-open", tone: "ok", label: "running" };
    case "stopped":
      // A stopped cluster is a valid steady state, so it is never styled as a problem.
      return { glyph: "‖", shape: "square-open", tone: "neutral", label: "stopped" };
    case "deleted":
      return { glyph: "✕", shape: "square-open-slash", tone: "fail", label: "deleted" };
    default:
      return { glyph: "→", shape: "circle-open", tone: "dim", label: lifecycle };
  }
}

/**
 * observedMark draws what was last seen. It is always solid, except unknown, which
 * is always dashed. Deleted is only ever set from a positive observation, never
 * from silence, so silence draws unknown instead.
 */
export function observedMark(lifecycle: string): Mark {
  switch (lifecycle) {
    case "provisioning":
      return { glyph: "⇢", shape: "circle-half", tone: "active", label: "provisioning" };
    case "running":
      return { glyph: "✓", shape: "circle", tone: "ok", label: "running" };
    case "stopped":
      return { glyph: "‖", shape: "square", tone: "neutral", label: "stopped" };
    case "deleting":
      return { glyph: "⇣", shape: "square-half", tone: "attn", label: "deleting" };
    case "deleted":
      return { glyph: "✕", shape: "square-slash", tone: "dim", label: "deleted" };
    case "unknown":
      return { glyph: "?", shape: "circle-dashed", tone: "neutral", label: "unknown" };
    default:
      return { glyph: "?", shape: "circle-dashed", tone: "neutral", label: lifecycle };
  }
}

/** The default observation limit. An observation older than this reads as unknown. */
export const defaultLimitSeconds = 60;

/** ageSeconds is the whole seconds between an observation and now, never negative. */
export function ageSeconds(observedAt: string, now: Date): number {
  const observed = Date.parse(observedAt);
  if (Number.isNaN(observed)) return Number.POSITIVE_INFINITY;
  return Math.max(0, Math.floor((now.getTime() - observed) / 1000));
}

/**
 * age reads a whole number of seconds as the design writes it: seconds below a
 * minute, then minutes and seconds, then hours and minutes. An age nobody can
 * measure reads as never, because zero would be a lie.
 */
export function age(seconds: number): string {
  if (!Number.isFinite(seconds)) return "never";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${String(seconds % 60).padStart(2, "0")}s`;
  return `${Math.floor(seconds / 3600)}h ${String(Math.floor((seconds % 3600) / 60)).padStart(2, "0")}m`;
}

/**
 * freshness reports where an observation sits against its limit.
 * It ages from half the limit, so a reader sees decay before the value is dropped.
 */
export function freshness(age: number, limitSeconds = defaultLimitSeconds): Freshness {
  if (!Number.isFinite(age) || age >= limitSeconds) return "expired";
  if (age >= limitSeconds / 2) return "ageing";
  return "fresh";
}

/**
 * viewCondition applies freshness to one condition.
 * An expired observation displays unknown and keeps its last known value as history.
 * It never changes desired state and it never counts as a failure.
 */
export function viewCondition(condition: Condition, now: Date, limitSeconds = defaultLimitSeconds): ConditionView {
  const seconds = ageSeconds(condition.observedAt, now);
  const state = freshness(seconds, limitSeconds);
  const expired = state === "expired";
  const display: Truth = expired ? "unknown" : condition.value;
  const view: ConditionView = {
    ...condition,
    freshness: state,
    ageSeconds: seconds,
    display,
    mark: marks[display],
  };
  if (expired && condition.value !== "unknown") view.lastKnown = condition.value;
  return view;
}

/**
 * viewConditions returns every condition in the fixed order, including the ones
 * the server did not report. A missing condition is unknown, never absent, so a
 * screen cannot quietly drop one and appear healthier than it is.
 */
export function viewConditions(reported: readonly Condition[], now: Date, limitSeconds = defaultLimitSeconds): ConditionView[] {
  return conditionOrder.map((name) => {
    const found = reported.find((condition) => condition.name === name);
    if (found) return viewCondition(found, now, limitSeconds);
    return viewCondition({ name, value: "unknown", observedAt: "", observedRevision: 0, reason: "not_reported" }, now, limitSeconds);
  });
}

/** failing counts conditions that are observed false. Unknown is never a failure. */
export function failing(views: readonly ConditionView[]): number {
  return views.filter((view) => view.display === "false").length;
}

/** unresolved counts conditions that nobody can currently answer. */
export function unresolved(views: readonly ConditionView[]): number {
  return views.filter((view) => view.display === "unknown").length;
}

/** Divergence is desired state against observed state, with the revision of each. */
export interface Divergence {
  desired: string;
  desiredRevision: number;
  observed: string;
  observedRevision: number;
  observedAt: string;
  /** The operation that currently owns the difference, when one does. */
  owningOperation?: string;
}

export type DivergenceVerdict = "converged" | "in_progress" | "stale" | "contradicted";

/**
 * verdict classifies a difference between desired and observed state.
 *
 * Divergence alone is not an error. Divergence with a fresh contradicting
 * observation is. An observation that agrees on state and lags only by revision
 * contradicts nothing, so it reports progress. An expired observation is stale,
 * because nobody can currently say whether the two agree.
 */
export function verdict(value: Divergence, now: Date, limitSeconds = defaultLimitSeconds): DivergenceVerdict {
  const agrees = value.desired === value.observed;
  if (agrees && value.desiredRevision === value.observedRevision) return "converged";
  if (freshness(ageSeconds(value.observedAt, now), limitSeconds) === "expired") return "stale";
  if (agrees || value.owningOperation) return "in_progress";
  return "contradicted";
}

/**
 * verdictView names a verdict and gives it the rule the design draws beside the
 * desired and observed pair. Only converged gets a single summary word, because
 * it is the only case where one word is honest.
 */
export function verdictView(value: DivergenceVerdict): { label: string; tone: Tone; dashed: boolean } {
  switch (value) {
    case "converged":
      return { label: "converged", tone: "ok", dashed: false };
    case "in_progress":
      return { label: "converging", tone: "active", dashed: false };
    case "stale":
      return { label: "divergent · unknown", tone: "neutral", dashed: true };
    default:
      return { label: "divergent · known failure", tone: "fail", dashed: false };
  }
}

/** The durable status of one operation, as the API reports it. */
export type OperationStatus = "pending" | "running" | "waiting" | "cancelling" | "succeeded" | "failed" | "cancelled";

/**
 * accepted reports work that the controller recorded and has not started.
 * Acceptance is its own state. It makes no claim about any database.
 */
export function accepted(status: OperationStatus, planVersion: number): boolean {
  return status === "pending" && planVersion === 0;
}

/** finished reports a status that no owner can change. */
export function finished(status: OperationStatus): boolean {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}

/** operationMark draws a status without a spinner, because progress is not promised. */
export function operationMark(status: OperationStatus, planVersion: number): Mark {
  if (accepted(status, planVersion)) return { glyph: "⋯", shape: "circle-open", tone: "active", label: "accepted" };
  switch (status) {
    case "pending":
      return { glyph: "⋯", shape: "circle-open", tone: "active", label: "pending" };
    case "running":
      return { glyph: "⇢", shape: "circle-half", tone: "active", label: "running" };
    case "waiting":
      // A hatched diamond, never a spinner. A spinner promises progress that
      // an operation with an unresolved effect cannot promise.
      return { glyph: "?", shape: "diamond-hatched", tone: "unc", label: "waiting" };
    case "cancelling":
      return { glyph: "⊘", shape: "square-half", tone: "attn", label: "cancelling" };
    case "succeeded":
      return { glyph: "✓", shape: "circle", tone: "ok", label: "succeeded" };
    case "failed":
      return { glyph: "✕", shape: "diamond", tone: "fail", label: "failed" };
    default:
      return { glyph: "⊘", shape: "square", tone: "neutral", label: "cancelled" };
  }
}

/** canCancel and canRetry offer an action only when the API accepts it. */
export function canCancel(status: OperationStatus, cancellationRequested: boolean): boolean {
  return !finished(status) && status !== "cancelling" && !cancellationRequested;
}

export function canRetry(status: OperationStatus): boolean {
  return status === "failed";
}

/** One API failure, as a screen must present it. */
export interface APIError {
  code: string;
  message: string;
  requestID: string;
  operationID?: string;
}

/**
 * errorView keeps the stable code verbatim, because that is what an operator greps for.
 * It never paraphrases the code away and it never hides the request identity.
 */
export function errorView(failure: APIError): { code: string; consequence: string; requestID: string; operationID?: string; mark: Mark } {
  const view: { code: string; consequence: string; requestID: string; operationID?: string; mark: Mark } = {
    code: failure.code,
    consequence: consequences[failure.code] ?? failure.message,
    requestID: failure.requestID,
    mark: errorMarks[failure.code] ?? { glyph: "✕", shape: "diamond", tone: "fail", label: failure.code },
  };
  if (failure.operationID) view.operationID = failure.operationID;
  return view;
}

/**
 * Each stable code gets one presentation across the whole product. A conflict is
 * an attention mark, not a failure, and an uncertain outcome is neither.
 */
const errorMarks: Record<string, Mark> = {
  idempotency_conflict: { glyph: "!", shape: "square-half", tone: "attn", label: "conflict" },
  name_conflict: { glyph: "!", shape: "square-half", tone: "attn", label: "conflict" },
  operation_conflict: { glyph: "!", shape: "square-half", tone: "attn", label: "conflict" },
  enrollment_conflict: { glyph: "!", shape: "square-half", tone: "attn", label: "conflict" },
  dependency_unavailable: { glyph: "!", shape: "square-half", tone: "attn", label: "transient" },
  // Never a failure mark: nothing is claimed either way.
  outcome_unknown: { glyph: "?", shape: "diamond-hatched", tone: "unc", label: "uncertain" },
  not_found: { glyph: "✕", shape: "square-slash", tone: "neutral", label: "gone or never was" },
};

/** A plain-language consequence for each stable code. The code itself is never replaced. */
const consequences: Record<string, string> = {
  operation_conflict: "Another operation is mutating this cluster. Synod runs one mutating operation for each cluster.",
  outcome_unknown: "The result is uncertain. Retry with the same request identity and the same body.",
  idempotency_conflict: "That idempotency key belongs to a different request body.",
  name_conflict: "That cluster name is already in use.",
  unsupported_spec: "This server does not support the requested engine version.",
  not_found: "The resource does not exist, or this credential cannot see it.",
  unauthenticated: "Sign in again.",
  forbidden: "This credential may not perform that action.",
  account_disabled: "This account cannot sign in.",
  cross_origin: "The request did not prove its origin. Reload the page and try again.",
  rate_limited: "Too many requests. Retry after the indicated delay.",
  invalid_request: "The request was rejected before any work started.",
  dependency_unavailable: "A dependency of the control plane is unavailable.",
};
