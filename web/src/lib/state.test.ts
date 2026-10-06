import { describe, expect, it } from "vitest";
import { errorView, finished, operationMark } from "./state";

describe("operation status", () => {
  it("keeps uncertainty distinct from failure", () => {
    expect(operationMark("waiting").shape).toBe("diamond-hatched");
    expect(operationMark("failed").shape).toBe("diamond");
    expect(finished("waiting")).toBe(false);
    expect(finished("succeeded")).toBe(true);
    expect(finished("failed")).toBe(true);
    expect(finished("cancelled")).toBe(true);
  });
  it("keeps error identity and unknown server messages", () => {
    expect(errorView({code:"new_code", message:"Try again.", requestID:"r1"})).toMatchObject({code:"new_code", consequence:"Try again.", requestID:"r1"});
  });
});
