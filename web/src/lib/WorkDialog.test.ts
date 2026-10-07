import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, describe, expect, it, vi } from "vitest";
import WorkDialog from "./WorkDialog.svelte";
import { api, Failure, type Operation } from "./api";
import { app, node, snapshot, destination } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { submit: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });

it("requires a named confirmation to replace and opens the accepted operation", async () => {
  vi.mocked(api.submit).mockResolvedValue({ id: "op1", status: "pending" } as Operation);
  const accepted = vi.fn();
  render(WorkDialog, { kind: "restore", app, snapshot, nodes: [node], destinations: [destination], onClose: vi.fn(), onAccepted: accepted });
  await fireEvent.change(screen.getByLabelText("Restore to device"), { target: { value: "n1" } });
  await fireEvent.click(screen.getByLabelText("Replace the existing app on the target device"));
  expect((screen.getByRole("button", { name: "Restore app" }) as HTMLButtonElement).disabled).toBe(true);
  await fireEvent.input(screen.getByLabelText("Type notes to confirm replacement"), { target: { value: "notes" } });
  await fireEvent.click(screen.getByRole("button", { name: "Restore app" }));
  await waitFor(() => expect(accepted).toHaveBeenCalledWith("op1"));
  expect(api.submit).toHaveBeenCalledWith("restores", expect.objectContaining({ replace: true, node_id: "n1", app_id: "a1", snapshot_id: "s1", destination_id: "d1" }));
});
it("keeps an uncertain restore request ID for its retry", async () => {
  vi.mocked(api.submit).mockRejectedValue(new Failure(503, { code: "outcome_unknown", message: "Inspect this operation.", requestID: "r1" }));
  render(WorkDialog, { kind: "restore", app, snapshot, nodes: [node], destinations: [destination], onClose: vi.fn(), onAccepted: vi.fn() });
  await fireEvent.change(screen.getByLabelText("Restore to device"), { target: { value: "n1" } });
  await fireEvent.click(screen.getByRole("button", { name: "Restore app" }));
  await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
  expect(vi.mocked(api.submit).mock.calls[1]).toEqual(vi.mocked(api.submit).mock.calls[0]);
  expect((screen.getByLabelText("Restore to device") as HTMLSelectElement).matches(":disabled")).toBe(true);
});
it("only offers the owner of a local repository", () => {
  render(WorkDialog, { kind: "restore", app, snapshot, nodes: [node, { ...node, id: "n2", name: "server" }], destinations: [{ ...destination, location: { kind: "local", path: "/srv/backups" } }], onClose: vi.fn(), onAccepted: vi.fn() });
  expect(screen.queryByRole("option", { name: "odin · arm64" })).toBeNull();
  expect(screen.getByRole("option", { name: "server · arm64" })).toBeTruthy();
});
