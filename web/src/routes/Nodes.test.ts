import { cleanup, render, screen, within } from "@testing-library/svelte";
import { afterEach, describe, expect, it } from "vitest";
import { nodes } from "../lib/mock";
import Nodes from "./Nodes.svelte";

// The screen reads no API. It draws the recorded fixture through the real
// freshness rules, so what is under test is how an expired report is presented.

function open(id = "") {
  render(Nodes, { id, onOpen: () => {} });
}

/** card returns the region that draws one node. */
function card(name: string) {
  const heading = screen.getByText(name);
  const found = heading.closest("article");
  if (!found) throw new Error(`no card for ${name}`);
  return found;
}

afterEach(cleanup);

describe("a node whose report expired", () => {
  it("reads unknown rather than absent, and keeps its placements", () => {
    open();
    const stale = card("node-03");
    // Silence is not deletion. The node is still drawn, its state reads unknown,
    // and its placements are not evicted from a report nobody received.
    expect(stale.querySelector(".badge")?.textContent?.trim()).toBe("unknown");
    expect(within(stale).getByText("pg-analytics/i1")).toBeTruthy();
    expect(within(stale).getByText("pg-core/i3")).toBeTruthy();
    expect(within(stale).getByText("redis-cache/s3")).toBeTruthy();
    expect(stale.textContent).not.toContain("deleted");
    // Every placement role reads unknown rather than carrying a stale answer.
    expect(within(stale).getAllByText("unknown").length).toBeGreaterThanOrEqual(4);
  });

  it("marks the last reading as last known, never as current", () => {
    open();
    const stale = card("node-03");
    // The capacity shown is a reading at a revision, not a measurement of now.
    expect(within(stale).getByText("? last 5 / 16")).toBeTruthy();
    expect(within(stale).getAllByText("rev 19").length).toBeGreaterThan(0);
    expect(within(stale).getByText(/limit 60s/)).toBeTruthy();
  });

  it("draws the expiry with more than colour", () => {
    open();
    // Hatching and a stronger edge, so the state survives greyscale.
    expect(card("node-03").className).toContain("hatched");
    expect(card("node-03").className).toContain("strong");
    expect(card("node-01").className).not.toContain("hatched");
  });

  it("leaves a fresh node alone", () => {
    open();
    const fresh = card("node-01");
    expect(within(fresh).getByText("schedulable")).toBeTruthy();
    expect(within(fresh).getByText("3s ago")).toBeTruthy();
  });

  it("narrows to one node when the address names one, so a deep link works", () => {
    open("node-03");
    expect(screen.getByText("node-03")).toBeTruthy();
    expect(screen.queryByText("node-01")).toBe(null);
    // A way back, because the address is the only state the screen holds.
    expect(screen.getByRole("link", { name: "All nodes" })).toBeTruthy();
  });

  it("says so plainly when the address names nothing", () => {
    open("node-99");
    expect(screen.getByText("No node has that name.")).toBeTruthy();
  });

  it("declares the fixture, so a placeholder is never read as an observation", () => {
    open();
    expect(screen.getByText(/no node read route/)).toBeTruthy();
    expect(nodes.some((node) => node.seenAgeSeconds > 60)).toBe(true);
  });
});
