import { cleanup, render, screen, within } from "@testing-library/svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import Shell from "./Shell.svelte";

// Screens that read the API are not under test here. The client answers nothing,
// so each screen draws its waiting state and the shell is what is measured.
vi.mock("./api", async () => {
  const real = await vi.importActual<typeof import("./api")>("./api");
  const never = () => new Promise(() => {});
  return { ...real, api: { operation: never, operations: never, nodes: never, protection: never, destinations: never, notifications: never, tokens: never, installation: never } };
});

const session = { actor_id: "root", role: "admin", user_id: "u1" };

function at(pathname: string) {
  window.history.pushState({}, "", pathname);
  render(Shell, { session, busy: false, theme: "dark", onTheme: () => {}, onSignOut: () => {} });
}

beforeEach(() => window.history.pushState({}, "", "/"));
afterEach(cleanup);

describe("the shell", () => {
  it("opens the overview first", () => {
    at("/");
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Overview");
    expect(within(screen.getByLabelText("Screens")).getByRole("link", { name: "Overview" }).getAttribute("aria-current")).toBe("page");
  });

  it("names every screen in plain words", () => {
    at("/");
    const navigation = within(screen.getByLabelText("Screens"));
    for (const name of ["Overview", "Apps", "Devices", "Storage", "Activity", "Settings"]) expect(navigation.getByRole("link", { name })).toBeTruthy();
  });

  it("keeps the section marked while a screen below it is open", () => {
    at("/activity/abc");
    const navigation = within(screen.getByLabelText("Screens"));
    expect(navigation.getByRole("link", { name: "Activity" }).getAttribute("aria-current")).toBe("page");
    expect(navigation.getByRole("link", { name: "Overview" }).getAttribute("aria-current")).toBe(null);
  });

  it("changes the address when a screen is opened, so a reload returns to it", async () => {
    at("/");
    (within(screen.getByLabelText("Screens")).getByRole("link", { name: "Devices" }) as HTMLAnchorElement).click();
    await Promise.resolve();
    expect(window.location.pathname).toBe("/devices");
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Devices");
  });

  it("opens a deep link from an earlier build", () => {
    at("/destinations");
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Storage");
  });

  it("opens a Settings tab from the address and moves between tabs with the arrow keys", async () => {
    at("/settings/accounts");
    const accounts = screen.getByRole("tab", { name: "Accounts" });
    expect(accounts.getAttribute("aria-selected")).toBe("true");
    expect(screen.getByRole("tabpanel").getAttribute("aria-labelledby")).toBe("tab-accounts");
    accounts.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true }));
    await vi.waitFor(() => expect(window.location.pathname).toBe("/settings/api-tokens"));
    expect(screen.getByRole("tab", { name: "API tokens" }).getAttribute("aria-selected")).toBe("true");
    screen.getByRole("tab", { name: "General" }).click();
    await vi.waitFor(() => expect(window.location.pathname).toBe("/settings"));
  });

  it("answers an address that names no screen without drawing a screen", () => {
    at("/nowhere");
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Not found");
  });
});
