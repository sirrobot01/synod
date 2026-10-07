import { cleanup, render, screen, within } from "@testing-library/svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import Shell from "./Shell.svelte";

// Screens that read the API are not under test here. The client answers nothing,
// so each screen draws its waiting state and the shell is what is measured.
vi.mock("./api", async () => {
  const real = await vi.importActual<typeof import("./api")>("./api");
  return {
    ...real,
    api: {
      operation: () => new Promise(() => {}),
      operations: () => new Promise(() => {}),
      nodes: () => new Promise(() => {}),
      protection: () => new Promise(() => {}),
      destinations: () => new Promise(() => {}),
      notifications: () => new Promise(() => {}),
      tokens: () => new Promise(() => {}),
    },
  };
});

const session = { actor_id: "root", role: "admin", user_id: "u1" };

function at(pathname: string) {
  window.history.pushState({}, "", pathname);
  render(Shell, { session, busy: false, onSignOut: () => {} });
}

beforeEach(() => window.history.pushState({}, "", "/"));
afterEach(cleanup);

describe("the shell", () => {
  it("draws the screen that the address names, so a deep link works", () => {
    at("/operations/op_7c14f2a9");
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("op_7c14f2a9");
  });

  it("keeps the section marked while a screen below it is open", () => {
    at("/operations/abc");
    // The breadcrumb names Operations too, so the mark is read from the navigation.
    const navigation = within(screen.getByLabelText("Screens"));
    const operations = navigation.getByRole("link", { name: "Activity" });
    // The mark says which part of the product the reader is in. An operation
    // belongs to Operations, so the mark must not move to the dashboard.
    expect(operations.getAttribute("aria-current")).toBe("page");
    expect(navigation.getByRole("link", { name: "Apps" }).getAttribute("aria-current")).toBe(null);
  });

  it("names every step back to the dashboard in the breadcrumb", () => {
    at("/operations/abc");
    const trail = screen.getByLabelText("Breadcrumb");
    expect(trail.textContent).toContain("Apps");
    expect(trail.textContent).toContain("Activity");
    expect(trail.textContent).toContain("abc");
  });

  it("changes the address when a screen is opened, so a reload returns to it", async () => {
    at("/");
    (within(screen.getByLabelText("Screens")).getByRole("link", { name: "Devices" }) as HTMLAnchorElement).click();
    await Promise.resolve();
    expect(window.location.pathname).toBe("/nodes");
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Devices");
  });

  it("answers an address that names no screen without drawing a screen", () => {
    at("/nowhere");
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Not found");
  });
});
