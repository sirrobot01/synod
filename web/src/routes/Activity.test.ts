import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import Activity from "./Activity.svelte";
import Shell from "../lib/Shell.svelte";
import { api, Failure, type Operation } from "../lib/api";
import { destinations, node, notifications, plan } from "../test/fixtures";
vi.mock("../lib/api", async () => ({ ...await vi.importActual<typeof import("../lib/api")>("../lib/api"), api: { operations: vi.fn(), notifications: vi.fn(), protection: vi.fn(), nodes: vi.fn(), destinations: vi.fn(), removeOperation: vi.fn(), clearOperations: vi.fn(), operation: vi.fn() } }));

const op = (n: number, status: Operation["status"] = "succeeded"): Operation => ({ id: `op${n}`, app_id: "a1", kind: "backup", status, node_id: "n1", actor_id: "schedule:s", failure_code: "", finished_at: status === "running" ? null : "2026-10-07T03:01:00Z", created_at: "2026-10-07T03:00:00Z", updated_at: "", result: { destination_id: "d1" } });
const page = (from: number, count: number) => Array.from({ length: count }, (_, i) => op(from + i));

beforeEach(() => {
  vi.mocked(api.notifications).mockResolvedValue(notifications);
  vi.mocked(api.protection).mockResolvedValue(plan);
  vi.mocked(api.nodes).mockResolvedValue([node]);
  vi.mocked(api.destinations).mockResolvedValue(destinations);
});
afterEach(() => { cleanup(); vi.resetAllMocks(); window.history.pushState({}, "", "/"); });

it("pages through older runs 50 at a time", async () => {
  vi.mocked(api.operations).mockResolvedValueOnce(page(1, 51)).mockResolvedValueOnce(page(51, 10));
  render(Activity);
  expect(await screen.findAllByRole("link", { name: /^backup: notes/ })).toHaveLength(50);
  expect(api.operations).toHaveBeenCalledWith({ limit: 51 });
  expect((screen.getByRole("button", { name: "Newer runs" }) as HTMLButtonElement).disabled).toBe(true);
  await fireEvent.click(screen.getByRole("button", { name: "Older runs" }));
  await waitFor(() => expect(api.operations).toHaveBeenLastCalledWith({ limit: 51, before: "op50" }));
  expect(screen.getAllByRole("link", { name: /^backup: notes/ })).toHaveLength(10);
  expect((screen.getByRole("button", { name: "Older runs" }) as HTMLButtonElement).disabled).toBe(true);
  expect((screen.getByRole("button", { name: "Newer runs" }) as HTMLButtonElement).disabled).toBe(false);
});

it("opens the run when the row is clicked", async () => {
  vi.mocked(api.operations).mockResolvedValue([op(1)]);
  vi.mocked(api.operation).mockReturnValue(new Promise(() => {}));
  window.history.pushState({}, "", "/activity");
  render(Shell, { session: { actor_id: "admin", role: "admin", user_id: "u1" }, busy: false, theme: "dark", onTheme: () => {}, onSignOut: () => {} });
  const link = await screen.findByRole("link", { name: /^backup: notes/ });
  expect(link.getAttribute("href")).toBe("/activity/op1");
  await fireEvent.click(link.closest("tr")!.querySelector("td:nth-child(3)")!);
  expect(window.location.pathname).toBe("/activity/op1");
});

it("removes one finished run after a confirmation, and offers no removal for running work", async () => {
  vi.mocked(api.operations).mockResolvedValue([op(1), op(2, "running")]);
  vi.mocked(api.removeOperation).mockResolvedValue(undefined);
  render(Activity);
  await screen.findAllByRole("link", { name: /^backup: notes/ });
  const buttons = screen.getAllByRole("button", { name: /from Activity$/ });
  expect(buttons).toHaveLength(1);
  await fireEvent.click(buttons[0]!);
  expect(screen.getByText(/App status and restore-test dates keep their history/)).toBeTruthy();
  await fireEvent.click(screen.getByRole("button", { name: "Remove" }));
  await waitFor(() => expect(api.removeOperation).toHaveBeenCalledWith("op1"));
});

it("clears finished runs and keeps the error of a refused removal", async () => {
  vi.mocked(api.operations).mockResolvedValue([op(1)]);
  vi.mocked(api.clearOperations).mockRejectedValue(new Failure(409, { code: "operation_active", message: "The operation is not finished.", requestID: "r1" }));
  render(Activity);
  await screen.findAllByRole("link", { name: /^backup: notes/ });
  await fireEvent.click(screen.getByRole("button", { name: "Clear finished" }));
  await fireEvent.click(screen.getAllByRole("button", { name: "Clear finished" }).at(-1)!);
  expect((await screen.findByRole("alert")).textContent).toContain("operation_active");
});
