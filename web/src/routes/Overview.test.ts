import { cleanup, fireEvent, render, screen, within } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import Overview from "./Overview.svelte";
import { api } from "../lib/api";
import { app, destinations, node, plan } from "../test/fixtures";
vi.mock("../lib/api", async () => ({ ...await vi.importActual<typeof import("../lib/api")>("../lib/api"), api: { protection: vi.fn(), destinations: vi.fn(), nodes: vi.fn(), operations: vi.fn(), installation: vi.fn(), registerJoinToken: vi.fn() } }));
const copied = { ...app, targets: [{ destination_id: "d1", selected: true, state: "partial", last_good: { completed_at: "2026-10-07T03:12:00Z" } }] };
beforeEach(() => {
  vi.mocked(api.protection).mockResolvedValue({ ...plan, apps: [copied] });
  vi.mocked(api.destinations).mockResolvedValue(destinations);
  vi.mocked(api.nodes).mockResolvedValue([node]);
  vi.mocked(api.operations).mockResolvedValue([{ id: "op1", app_id: "a1", kind: "backup", status: "succeeded", node_id: "n1", actor_id: "admin", failure_code: "", finished_at: "2026-10-07T03:12:41Z", created_at: "2026-10-07T03:12:00Z", updated_at: "", result: { destination_id: "d1", file_bytes: 1024 } }]);
  vi.mocked(api.installation).mockReturnValue(new Promise(() => {}));
});
afterEach(() => { cleanup(); vi.resetAllMocks(); });

function section(name: string) {
  return within(screen.getByRole("heading", { name }).closest("section")!);
}

it("draws the recovery matrix: apps under devices, one column per storage, then the restore test", async () => {
  render(Overview);
  await screen.findByRole("heading", { name: "Recovery matrix" });
  const matrix = section("Recovery matrix");
  expect(matrix.getAllByRole("columnheader").map((h) => h.textContent)).toEqual(["App", "Backup disk", "Restore test"]);
  expect(matrix.getByRole("link", { name: "odin" }).getAttribute("href")).toBe("/devices/n1");
  expect(matrix.getByRole("link", { name: "notes" }).getAttribute("href")).toBe("/apps/a1");
  const cell = matrix.getByRole("link", { name: /notes in Backup disk: Partial/ });
  expect(cell.getAttribute("href")).toBe("/apps/a1?storage=d1");
  expect(cell.textContent).toContain("Oct 07");
});
it("hatches a storage that an app is not scheduled to", async () => {
  vi.mocked(api.protection).mockResolvedValue({ ...plan, apps: [{ ...app, targets: [] }] });
  render(Overview);
  await screen.findByRole("heading", { name: "Recovery matrix" });
  expect(section("Recovery matrix").getByText("Not scheduled to this storage").closest("td")?.classList.contains("hatched")).toBe(true);
});
it("lists what needs action with its code, and recent runs as rows", async () => {
  render(Overview);
  await screen.findByRole("heading", { name: "Attention" });
  const attention = section("Attention");
  expect(attention.getByText("drill_container_stopped")).toBeTruthy();
  expect(attention.getByText("app_consistency_unverified")).toBeTruthy();
  const activity = section("Activity");
  expect(activity.getByRole("link", { name: "backup: notes" }).getAttribute("href")).toBe("/activity/op1");
  expect(api.operations).toHaveBeenCalledWith({ limit: 8 });
  expect(activity.getByText("41s")).toBeTruthy();
});
it("shows restorable apps and the newest snapshot in the header line", async () => {
  render(Overview);
  expect(await screen.findByText("1/1")).toBeTruthy();
  expect(screen.getByText("Newest snapshot").nextElementSibling?.textContent).toBe("Oct 07");
});
it("hides setup when every step is done", async () => {
  render(Overview);
  await screen.findByRole("heading", { name: "Recovery matrix" });
  expect(screen.queryByRole("heading", { name: "Setup" })).toBeNull();
});
it("shows the setup steps as rows with one action for the next step", async () => {
  vi.mocked(api.destinations).mockResolvedValue({ ...destinations, destinations: [] });
  vi.mocked(api.protection).mockResolvedValue({ ...plan, apps: [{ ...app, enabled: false }] });
  render(Overview);
  expect(await screen.findByText("1 of 3 done")).toBeTruthy();
  const steps = section("Setup");
  await fireEvent.click(steps.getByRole("button", { name: "Add storage" }));
  expect(screen.getByRole("dialog", { name: "Add storage" })).toBeTruthy();
});
it("opens Add device from the header", async () => {
  render(Overview);
  await fireEvent.click(await screen.findByRole("button", { name: "Add device" }));
  expect(screen.getByRole("dialog", { name: "Add device" })).toBeTruthy();
});
