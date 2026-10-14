import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, expect, it, vi } from "vitest";
import WorkDialog from "./WorkDialog.svelte";
import { api, Failure, type Operation } from "./api";
import { app, node, destination } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { submit: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });

it("backs up now to the chosen storage and opens the accepted work", async () => {
  vi.mocked(api.submit).mockResolvedValue({ id: "op1", status: "pending" } as Operation);
  const accepted = vi.fn();
  render(WorkDialog, { kind: "backup", app, nodes: [node], destinations: [destination], onClose: vi.fn(), onAccepted: accepted });
  await fireEvent.click(screen.getByRole("button", { name: "Back up now" }));
  await waitFor(() => expect(accepted).toHaveBeenCalledWith("op1"));
  expect(api.submit).toHaveBeenCalledWith("backups", expect.objectContaining({ app_id: "a1", destination_id: "d1", method: "auto" }));
});
it("starts a restore test on the chosen device", async () => {
  vi.mocked(api.submit).mockResolvedValue({ id: "op2", status: "pending" } as Operation);
  render(WorkDialog, { kind: "drill", app, nodes: [node], destinations: [destination], onClose: vi.fn(), onAccepted: vi.fn() });
  const start = screen.getByRole("button", { name: "Start test" }) as HTMLButtonElement;
  expect(start.disabled).toBe(true);
  await fireEvent.change(screen.getByLabelText("Run on"), { target: { value: "n1" } });
  await fireEvent.click(start);
  await waitFor(() => expect(api.submit).toHaveBeenCalledWith("proofs", expect.objectContaining({ kind: "restore_drill", node_id: "n1", app_id: "a1" })));
});
it("keeps an uncertain request identity for its retry", async () => {
  vi.mocked(api.submit).mockRejectedValue(new Failure(503, { code: "outcome_unknown", message: "Inspect this operation.", requestID: "r1" }));
  render(WorkDialog, { kind: "backup", app, nodes: [node], destinations: [destination], onClose: vi.fn(), onAccepted: vi.fn() });
  await fireEvent.click(screen.getByRole("button", { name: "Back up now" }));
  await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
  expect(vi.mocked(api.submit).mock.calls[1]).toEqual(vi.mocked(api.submit).mock.calls[0]);
});
it("only offers the owner of a disk for a check", () => {
  render(WorkDialog, { kind: "integrity", destinationID: "d1", nodes: [node, { ...node, id: "n2", name: "server" }], destinations: [{ ...destination, node_id: "n2", location: { kind: "local", path: "/srv/backups" } }], onClose: vi.fn(), onAccepted: vi.fn() });
  expect(screen.queryByRole("option", { name: /^odin/ })).toBeNull();
  expect(screen.getByRole("option", { name: /^server/ })).toBeTruthy();
});
