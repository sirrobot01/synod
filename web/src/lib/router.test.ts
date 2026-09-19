import { describe, expect, it } from "vitest";
import { follow, match, path, type ScreenName } from "./router.svelte";

describe("routing", () => {
  it("reads every screen from its path", () => {
    expect(match("/")).toEqual({ screen: "fleet", id: "" });
    expect(match("/nodes")).toEqual({ screen: "nodes", id: "" });
    expect(match("/databases")).toEqual({ screen: "databases", id: "" });
    expect(match("/databases/pg-core")).toEqual({ screen: "cluster", id: "pg-core" });
    expect(match("/databases/pg-core/topology")).toEqual({ screen: "topology", id: "pg-core" });
    expect(match("/backups")).toEqual({ screen: "backups", id: "" });
    expect(match("/operations")).toEqual({ screen: "operations", id: "" });
    expect(match("/settings")).toEqual({ screen: "settings", id: "" });
    expect(match("/nowhere")).toEqual({ screen: "unknown", id: "" });
  });

  // Specification 17 requires an operation ID to survive a reload.
  it("keeps an operation identity in the path", () => {
    const id = "0f9c1a2b-4d5e-4f60-8a71-2b3c4d5e6f70";
    const address = path("operation", id);
    expect(address).toBe(`/operations/${id}`);
    expect(match(address)).toEqual({ screen: "operation", id });
  });

  it("round-trips every screen through its own address", () => {
    const cases: [ScreenName, string][] = [
      ["fleet", ""],
      ["nodes", ""],
      ["nodes", "node-1"],
      ["databases", ""],
      ["cluster", "pg-core"],
      ["topology", "pg-core"],
      ["backups", ""],
      ["operations", ""],
      ["operation", "op-1"],
      ["settings", ""],
    ];
    for (const [screen, id] of cases) {
      expect(match(path(screen, id))).toEqual({ screen, id });
    }
  });

  it("survives a trailing slash and an encoded identity", () => {
    expect(match("/databases/pg-core/")).toEqual({ screen: "cluster", id: "pg-core" });
    expect(match(path("cluster", "pg core/1"))).toEqual({ screen: "cluster", id: "pg core/1" });
  });
});

describe("follow", () => {
  function click(overrides: Partial<MouseEvent> = {}): MouseEvent & { prevented: boolean } {
    const event = {
      button: 0,
      defaultPrevented: false,
      metaKey: false,
      ctrlKey: false,
      shiftKey: false,
      altKey: false,
      prevented: false,
      preventDefault() {
        this.prevented = true;
      },
      ...overrides,
    };
    return event as unknown as MouseEvent & { prevented: boolean };
  }

  it("routes a plain primary click in the browser", () => {
    let went = 0;
    const event = click();
    follow(event, () => (went += 1));
    expect(went).toBe(1);
    expect(event.prevented).toBe(true);
  });

  it("leaves a click that opens elsewhere to the browser", () => {
    // Each of these means open in a new tab or window. Routing one of them in
    // place, or calling preventDefault on it, takes that away from the operator.
    for (const overrides of [{ metaKey: true }, { ctrlKey: true }, { shiftKey: true }, { altKey: true }, { button: 1 }]) {
      let went = 0;
      const event = click(overrides);
      follow(event, () => (went += 1));
      expect(went).toBe(0);
      expect(event.prevented).toBe(false);
    }
  });

  it("does not act on a click that something already handled", () => {
    let went = 0;
    follow(click({ defaultPrevented: true }), () => (went += 1));
    expect(went).toBe(0);
  });
});
