import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import Apps from "./Apps.svelte";
import { api } from "../lib/api";
import { plan, destinations, node } from "../test/fixtures";
vi.mock("../lib/api", async () => ({ ...await vi.importActual<typeof import("../lib/api")>("../lib/api"), api: { protection: vi.fn(), destinations: vi.fn(), nodes: vi.fn() } }));
beforeEach(() => { vi.mocked(api.protection).mockResolvedValue(plan); vi.mocked(api.destinations).mockResolvedValue(destinations); vi.mocked(api.nodes).mockResolvedValue([node]); });
afterEach(() => { cleanup(); vi.resetAllMocks(); });
it("keeps the successful restore date beside a later failed drill", async () => {
  render(Apps, { go: vi.fn() });
  expect(await screen.findByText(/Tested Oct 1/)).toBeTruthy();
  expect(screen.getByRole("link", { name: "Latest drill failed" }).getAttribute("href")).toBe("/operations/drill-failed");
  expect(screen.getByText("Partial")).toBeTruthy();
  expect(screen.getByText("App consistency unverified")).toBeTruthy();
  expect(screen.queryByText("Protected")).toBeNull();
});
it("explains why a saved copy cannot give full protection", async () => {
  render(Apps, { id: "a1", go: vi.fn() });
  expect(await screen.findByText(/Full protection is unavailable/)).toBeTruthy();
  expect(screen.getByText(/will not pause writes/)).toBeTruthy();
});
it("does not call a failed read an empty installation", async () => {
  vi.mocked(api.protection).mockRejectedValue(new TypeError("offline"));
  render(Apps, { go: vi.fn() });
  expect(await screen.findByRole("alert")).toBeTruthy();
  expect(screen.queryByText(/No apps discovered/)).toBeNull();
});
it("keeps last known copies and marks a failed refresh", async () => {
  render(Apps, { go: vi.fn() });
  await screen.findByText("notes");
  vi.mocked(api.protection).mockRejectedValue(new TypeError("offline"));
  await fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  await waitFor(() => expect(screen.getByText(/This view could not refresh/)).toBeTruthy());
  expect(screen.getByText("notes")).toBeTruthy();
});
it("filters apps without treating no matches as no discovery", async () => {
  render(Apps, { go: vi.fn() });
  await screen.findByText("notes");
  await fireEvent.input(screen.getByRole("searchbox"), { target: { value: "not-here" } });
  expect(screen.getByText("No apps match this filter.")).toBeTruthy();
});
