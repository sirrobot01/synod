import { describe, expect, it } from "vitest";
import { follow, match, path, type ScreenName } from "./router.svelte";

describe("routing", () => {
  it("reads every screen from its path", () => {
    expect(match("/")).toEqual({ screen: "overview", id: "" });
    expect(match("/devices")).toEqual({ screen: "devices", id: "" });
    expect(match("/storage")).toEqual({ screen: "storage", id: "" });
    expect(match("/backups")).toEqual({ screen: "unknown", id: "" });
    expect(match("/activity")).toEqual({ screen: "activity", id: "" });
    expect(match("/settings")).toEqual({ screen: "settings", id: "" });
    expect(match("/nowhere")).toEqual({ screen: "unknown", id: "" });
  });

  // An operation ID must survive a reload.
  it("keeps an operation identity in the path", () => {
    const id = "0f9c1a2b-4d5e-4f60-8a71-2b3c4d5e6f70";
    const address = path("operation", id);
    expect(address).toBe(`/activity/${id}`);
    expect(match(address)).toEqual({ screen: "operation", id });
  });

  it("round-trips every screen through its own address", () => {
    const cases: [ScreenName, string][] = [
      ["overview", ""],
      ["apps", ""],
      ["apps", "a1"],
      ["storage", ""],
      ["devices", ""],
      ["devices", "node-1"],
      ["activity", ""],
      ["operation", "op-1"],
      ["settings", ""],
      ["settings", "accounts"],
      ["settings", "recovery-kit"],
    ];
    for (const [screen, id] of cases) {
      expect(match(path(screen, id))).toEqual({ screen, id });
    }
  });

  it("survives a trailing slash and an encoded identity", () => {
    expect(match("/devices/node-1/")).toEqual({ screen: "devices", id: "node-1" });
    expect(match(path("operation", "op 1/2"))).toEqual({ screen: "operation", id: "op 1/2" });
  });
});

it("rejects malformed and extra path segments", () => {
  expect(match("/apps/%ZZ").screen).toBe("unknown");
  expect(match("/apps/a1/more").screen).toBe("unknown");
  expect(match("/settings/extra").screen).toBe("unknown");
  expect(path("settings", "general")).toBe("/settings");
  expect(path("apps", "a1", "storage=d1")).toBe("/apps/a1?storage=d1");
});

// Links from earlier builds still open the screen that replaced theirs.
it("reads the addresses of earlier builds", () => {
  expect(match("/nodes/n1")).toEqual({ screen: "devices", id: "n1" });
  expect(match("/destinations")).toEqual({ screen: "storage", id: "" });
  expect(match("/operations/op-1")).toEqual({ screen: "operation", id: "op-1" });
  expect(match("/snapshots/a1")).toEqual({ screen: "apps", id: "a1" });
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
