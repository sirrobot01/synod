import { cleanup, fireEvent, render, screen, within } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import AppPage from "./AppPage.svelte";
import { api } from "../lib/api";
import { app, plan, destinations, node, snapshot } from "../test/fixtures";
vi.mock("../lib/api", async () => ({ ...await vi.importActual<typeof import("../lib/api")>("../lib/api"), api: { protection: vi.fn(), destinations: vi.fn(), nodes: vi.fn(), snapshots: vi.fn(), files: vi.fn() } }));
beforeEach(() => {
  vi.mocked(api.protection).mockResolvedValue(plan);
  vi.mocked(api.destinations).mockResolvedValue(destinations);
  vi.mocked(api.nodes).mockResolvedValue([node]);
  vi.mocked(api.snapshots).mockResolvedValue({ snapshots: [snapshot] });
});
afterEach(() => { cleanup(); vi.resetAllMocks(); window.history.pushState({}, "", "/"); });

function section(name: string) {
  return within(screen.getByRole("heading", { name }).closest("section")!);
}

it("names a limit by its code and says how to remove it", async () => {
  render(AppPage, { id: "a1" });
  await screen.findByRole("heading", { name: "Limits" });
  const limits = section("Limits");
  expect(limits.getByText("app_consistency_unverified")).toBeTruthy();
  expect(limits.getByText(/Synod does not pause writes/)).toBeTruthy();
  expect(screen.getByText("Partial")).toBeTruthy();
});
it("lists each path with what a backup does with it, and names database dumps by engine", async () => {
  vi.mocked(api.protection).mockResolvedValue({ ...plan, apps: [{ ...app, app: { ...app.app, definition: { ...app.app.definition, containers: [{ id: "c1", name: "notes-db", image: "postgres:16", database: "postgres" }] } } }] });
  render(AppPage, { id: "a1" });
  await screen.findByRole("heading", { name: "Paths" });
  const rows = within(section("Paths").getAllByRole("table")[0]!).getAllByRole("row").slice(1).map((row) => within(row).getAllByRole("cell").map((c) => c.textContent?.trim()).filter(Boolean).join(" "));
  expect(rows).toEqual(["included /srv/notes 1 KiB", "dumped notes-db PostgreSQL dump", "excluded /srv/media Large", "never /var/run/docker.sock host socket"]);
  expect(screen.getByText("auto (postgres)")).toBeTruthy();
});
it("names each limit and how to remove it", async () => {
  vi.mocked(api.protection).mockResolvedValue({ ...plan, apps: [{ ...plan.apps[0]!, limit_codes: ["data_excluded", "container_layer_data", "restore_unproven"] }] });
  render(AppPage, { id: "a1" });
  expect(await screen.findByText(/Large paths, media, or files over 20 GiB were skipped/)).toBeTruthy();
  expect(screen.getByText(/Move it to a volume or bind mount/)).toBeTruthy();
  expect(screen.getByText(/No restore test has passed/)).toBeTruthy();
});
it("lists the snapshots and reports partial storage coverage", async () => {
  vi.mocked(api.snapshots).mockResolvedValue({ snapshots: [snapshot], warning_codes: ["destination_unavailable"], truncated: true });
  render(AppPage, { id: "a1" });
  expect(await screen.findByText(/Some storage was unreadable/)).toBeTruthy();
  expect(screen.getByText(/List truncated/)).toBeTruthy();
  expect(screen.getByRole("button", { name: /Restore the snapshot of 03:01/ })).toBeTruthy();
});
it("reads only one storage when the address names it", async () => {
  window.history.pushState({}, "", "/apps/a1?storage=d1");
  render(AppPage, { id: "a1" });
  await screen.findByRole("heading", { name: "Snapshots" });
  await vi.waitFor(() => expect(api.snapshots).toHaveBeenCalledWith("a1", { via: "", destination: "d1" }));
});
it("does not call a storage failure an empty history", async () => {
  vi.mocked(api.snapshots).mockRejectedValue(new TypeError("offline"));
  render(AppPage, { id: "a1" });
  expect(await screen.findByRole("alert")).toBeTruthy();
  expect(screen.queryByText("No snapshots")).toBeNull();
});
it("opens the restore wizard at the device step from one snapshot", async () => {
  render(AppPage, { id: "a1" });
  await fireEvent.click(await screen.findByRole("button", { name: /Restore the snapshot of/ }));
  const dialog = within(screen.getByRole("dialog"));
  expect(dialog.getByText("Device").closest("li")?.getAttribute("aria-current")).toBe("step");
});
