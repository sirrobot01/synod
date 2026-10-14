import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import Apps from "./Apps.svelte";
import { api } from "../lib/api";
import { plan, destinations, node } from "../test/fixtures";
vi.mock("../lib/api", async () => ({ ...await vi.importActual<typeof import("../lib/api")>("../lib/api"), api: { protection: vi.fn(), destinations: vi.fn(), nodes: vi.fn() } }));
beforeEach(() => { vi.mocked(api.protection).mockResolvedValue(plan); vi.mocked(api.destinations).mockResolvedValue(destinations); vi.mocked(api.nodes).mockResolvedValue([node]); });
afterEach(() => { cleanup(); vi.resetAllMocks(); });

it("shows the status word with its limit code, and a failed restore test", async () => {
  render(Apps);
  expect(await screen.findByText("Partial")).toBeTruthy();
  expect(screen.getByText("app_consistency_unverified")).toBeTruthy();
  expect(screen.getByRole("link", { name: "Failed test" }).getAttribute("href")).toBe("/activity/drill-failed");
  expect(screen.queryByText("Restorable")).toBeNull();
  expect(screen.getByText("1 of 1 scheduled. New apps are scheduled automatically.")).toBeTruthy();
});
it("does not call a failed read an empty installation", async () => {
  vi.mocked(api.protection).mockRejectedValue(new TypeError("offline"));
  render(Apps);
  expect(await screen.findByRole("alert")).toBeTruthy();
  expect(screen.queryByText("No apps found")).toBeNull();
});
it("keeps the last known apps and marks a failed refresh", async () => {
  render(Apps);
  await screen.findByText("notes");
  vi.mocked(api.protection).mockRejectedValue(new TypeError("offline"));
  await fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  await waitFor(() => expect(screen.getByText(/Not refreshed/)).toBeTruthy());
  expect(screen.getByText("notes")).toBeTruthy();
});
it("filters apps without treating no matches as no discovery", async () => {
  render(Apps);
  await screen.findByText("notes");
  await fireEvent.input(screen.getByRole("searchbox"), { target: { value: "not-here" } });
  expect(screen.getByText("No apps match.")).toBeTruthy();
});
it("says what to do when no app was found", async () => {
  vi.mocked(api.protection).mockResolvedValue({ ...plan, apps: [] });
  render(Apps);
  expect(await screen.findByText("No apps found")).toBeTruthy();
  expect(screen.getByRole("link", { name: "Devices" }).getAttribute("href")).toBe("/devices");
});
