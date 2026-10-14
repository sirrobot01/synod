import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/svelte";
import { afterEach, expect, it, vi } from "vitest";
import ScheduleBackups from "./ScheduleBackups.svelte";
import { api, Failure, type AppPlan } from "./api";
import { app, plan, destination, node } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { protect: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });

const open: AppPlan = { ...app, enabled: false, state: "unprotected" };
const other: AppPlan = { ...open, app: { ...open.app, id: "a2", definition: { ...open.app.definition, name: "wiki" } } };
const done: AppPlan = { ...app, app: { ...app.app, id: "a3", definition: { ...app.app.definition, name: "vault" } } };
const fresh = { ...plan, auto_protect: false, apps: [done, open, other] };

it("lists apps with their paths, and saves the schedule for the chosen ones after a review", async () => {
  vi.mocked(api.protect).mockResolvedValue({ id: "p1", revision: 4 });
  const saved = vi.fn();
  render(ScheduleBackups, { plan: fresh, destinations: [destination], nodes: [node], onClose: vi.fn(), onSaved: saved });
  expect(screen.getByRole("dialog", { name: "Schedule backups" })).toBeTruthy();
  expect(screen.getAllByText("1 path, 1 excluded")).toHaveLength(2);
  const rows = screen.getAllByRole("row").slice(1).map((row) => within(row).getAllByRole("cell")[1]?.textContent);
  expect(rows).toEqual(["notes", "wiki", "vault"]);
  expect((screen.getByRole("checkbox", { name: "vault (scheduled)" }) as HTMLInputElement).disabled).toBe(true);
  expect(screen.getByText("Storage").nextElementSibling?.textContent).toBe("Backup disk");
  await fireEvent.click(screen.getByRole("checkbox", { name: "wiki" }));
  await fireEvent.click(screen.getByRole("button", { name: "Review 1 app" }));
  expect(api.protect).not.toHaveBeenCalled();
  expect(screen.getByText("/srv/media")).toBeTruthy();
  await fireEvent.click(screen.getByRole("button", { name: "Save schedule" }));
  await waitFor(() => expect(saved).toHaveBeenCalled());
  expect(api.protect).toHaveBeenCalledWith(expect.objectContaining({ expected_revision: 3, all: false, app_ids: ["a1"], enabled: true }));
  expect(vi.mocked(api.protect).mock.calls[0]?.[0].settings.include_large).toBeUndefined();
});

it("schedules new apps too only when every app is chosen", async () => {
  vi.mocked(api.protect).mockResolvedValue({ id: "p1", revision: 4 });
  render(ScheduleBackups, { plan: fresh, destinations: [destination], nodes: [node], onClose: vi.fn(), onSaved: vi.fn() });
  const later = screen.getByRole("checkbox", { name: "Also schedule apps found later" }) as HTMLInputElement;
  expect(later.disabled).toBe(false);
  await fireEvent.click(screen.getByRole("checkbox", { name: "wiki" }));
  expect(later.disabled).toBe(true);
  await fireEvent.click(screen.getByRole("checkbox", { name: "wiki" }));
  await fireEvent.click(later);
  await fireEvent.click(screen.getByRole("button", { name: "Review 2 apps" }));
  await fireEvent.click(screen.getByRole("button", { name: "Save schedule" }));
  await waitFor(() => expect(api.protect).toHaveBeenCalledWith(expect.objectContaining({ all: true })));
});

it("does not offer an app whose device is not approved", () => {
  render(ScheduleBackups, { plan: fresh, destinations: [destination], nodes: [{ ...node, approved_at: null }], onClose: vi.fn(), onSaved: vi.fn() });
  expect((screen.getByRole("checkbox", { name: "wiki" }) as HTMLInputElement).disabled).toBe(true);
  expect(screen.getAllByText("device not approved")).toHaveLength(2);
});

it("requires a fresh plan after a revision conflict", async () => {
  vi.mocked(api.protect).mockRejectedValue(new Failure(409, { code: "protect_plan_changed", message: "Read the plan again.", requestID: "r1" }));
  const reread = vi.fn();
  render(ScheduleBackups, { plan: fresh, destinations: [destination], nodes: [node], onClose: vi.fn(), onSaved: reread });
  await fireEvent.click(screen.getByRole("button", { name: "Review 2 apps" }));
  await fireEvent.click(screen.getByRole("button", { name: "Save schedule" }));
  await fireEvent.click(await screen.findByRole("button", { name: "Reload schedule" }));
  expect(reread).toHaveBeenCalled();
});
