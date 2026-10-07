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

/** The durable status of one operation, as the API reports it. */
export type OperationStatus = "pending" | "running" | "waiting" | "cancelling" | "succeeded" | "failed" | "cancelled";

/** finished reports a status that no owner can change. */
export function finished(status: OperationStatus): boolean {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}

/** operationMark draws a status without a spinner, because progress is not promised. */
export function operationMark(status: OperationStatus): Mark {
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
    consequence: failure.message || consequences[failure.code] || "The request could not be completed.",
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
  protect_plan_changed: { glyph: "!", shape: "square-half", tone: "attn", label: "plan changed" },
  notification_settings_changed: { glyph: "!", shape: "square-half", tone: "attn", label: "settings changed" },
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
  operation_conflict: "Active work or a storage change conflicts with this request. Inspect the app and its operations before a new request.",
  outcome_unknown: "The result is uncertain. Retry with the same request identity and the same body.",
  idempotency_conflict: "That idempotency key belongs to a different request body.",
  name_conflict: "That app name is already in use.",
  not_found: "The resource does not exist, or this credential cannot see it.",
  unauthenticated: "Sign in again.",
  forbidden: "This credential may not perform that action.",
  account_disabled: "This account cannot sign in.",
  cross_origin: "The request did not prove its origin. Reload the page and try again.",
  rate_limited: "Too many requests. Retry after the indicated delay.",
  invalid_request: "The request was rejected before any work started.",
  dependency_unavailable: "The service is unavailable. Try again.",
};
