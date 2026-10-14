import { cleanup, fireEvent, render, screen } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import RestoreDialog from "./RestoreDialog.svelte";
import { api, type Node, type Operation } from "./api";
import { app, node, snapshot, destination } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { submit: vi.fn(), operation: vi.fn() } }));

// odin runs the app. server is another online device. nas is offline. pi waits for approval.
const server: Node = { ...node, id: "n2", name: "server", architecture: "amd64" };
const nas: Node = { ...node, id: "n3", name: "nas", architecture: "amd64", last_seen_at: "2026-10-04T08:00:00Z" };
const pi: Node = { ...node, id: "n4", name: "pi", approved_at: null };
const older = { ...snapshot, id: "s0", completed_at: "2026-10-06T03:01:00Z" };
const props = { item: app, copies: [snapshot, older], destinations: [destination], nodes: [node, server, nas, pi], onClose: vi.fn() };

beforeEach(() => { vi.useFakeTimers({ shouldAdvanceTime: true }); vi.setSystemTime(new Date("2026-10-07T08:01:00Z")); });
afterEach(() => { cleanup(); vi.useRealTimers(); vi.resetAllMocks(); });

function device(name: string) {
  return screen.getByRole("button", { name: new RegExp(`^${name}`) }) as HTMLButtonElement;
}

it("walks through snapshot, device, and confirm, then follows the restore to its end", async () => {
  vi.mocked(api.submit).mockResolvedValue({ id: "op1", status: "pending", node_id: "n2" } as Operation);
  vi.mocked(api.operation).mockResolvedValue({ id: "op1", status: "succeeded", node_id: "n2", result: { project_directory: "/srv/notes" } } as Operation);
  const finished = vi.fn();
  render(RestoreDialog, { ...props, onFinished: finished });
  expect(screen.getByRole("button", { name: /03:01 today/ }).getAttribute("aria-pressed")).toBe("true");
  await fireEvent.click(screen.getByRole("button", { name: /03:01 yesterday/ }));
  await fireEvent.click(screen.getByRole("button", { name: "Next" }));
  expect((screen.getByRole("button", { name: "Next" }) as HTMLButtonElement).disabled).toBe(true);
  await fireEvent.click(device("server"));
  await fireEvent.click(screen.getByRole("button", { name: "Next" }));
  expect(screen.getByText("Load images from the snapshot.")).toBeTruthy();
  expect(screen.queryByLabelText("Type notes to confirm")).toBeNull();
  await fireEvent.click(screen.getByRole("button", { name: "Restore notes on server" }));
  expect(api.submit).toHaveBeenCalledWith("restores", expect.objectContaining({ app_id: "a1", node_id: "n2", snapshot_id: "s0", destination_id: "d1", replace: false }));
  await vi.advanceTimersByTimeAsync(3000);
  expect(await screen.findByText("notes restored on server")).toBeTruthy();
  expect(finished).toHaveBeenCalled();
});

it("disables devices that cannot take the snapshot and says why", async () => {
  render(RestoreDialog, { ...props, start: snapshot });
  expect(device("nas").disabled).toBe(true);
  expect(screen.getByText("Offline 3d")).toBeTruthy();
  expect(device("pi").disabled).toBe(true);
  expect(screen.getByText("Not approved")).toBeTruthy();
  expect(device("server").disabled).toBe(false);
});

it("disables a device whose architecture the snapshot has no image for", async () => {
  render(RestoreDialog, { ...props, copies: [{ ...snapshot, restore_architectures: ["arm64"] }], start: { ...snapshot, restore_architectures: ["arm64"] } });
  expect(device("server").disabled).toBe(true);
  expect(screen.getByText("No amd64 image in this snapshot")).toBeTruthy();
});

it("disables a device that cannot read a disk of another device", async () => {
  const disk = { ...destination, node_id: "n1", location: { kind: "local" as const, path: "/srv/backups" } };
  render(RestoreDialog, { ...props, destinations: [disk], start: snapshot });
  expect(device("server").disabled).toBe(true);
  expect(device("odin").disabled).toBe(false);
});

it("warns before it replaces the app on its own device and needs the name typed", async () => {
  vi.mocked(api.submit).mockResolvedValue({ id: "op2", status: "pending", node_id: "n1" } as Operation);
  vi.mocked(api.operation).mockReturnValue(new Promise(() => {}));
  render(RestoreDialog, { ...props, start: snapshot });
  await fireEvent.click(device("odin"));
  await fireEvent.click(screen.getByRole("button", { name: "Next" }));
  expect(screen.getByText(/Replaces the running notes on odin/)).toBeTruthy();
  const restore = screen.getByRole("button", { name: "Restore notes on odin" }) as HTMLButtonElement;
  expect(restore.disabled).toBe(true);
  await fireEvent.input(screen.getByLabelText("Type notes to confirm"), { target: { value: "notes" } });
  await fireEvent.click(restore);
  expect(api.submit).toHaveBeenCalledWith("restores", expect.objectContaining({ node_id: "n1", replace: true }));
});

it("keeps the request identity after a lost reply", async () => {
  vi.mocked(api.submit).mockRejectedValueOnce(new TypeError("network")).mockReturnValueOnce(new Promise(() => {}));
  render(RestoreDialog, { ...props, start: snapshot });
  await fireEvent.click(device("server"));
  await fireEvent.click(screen.getByRole("button", { name: "Next" }));
  await fireEvent.click(screen.getByRole("button", { name: "Restore notes on server" }));
  await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
  expect(vi.mocked(api.submit).mock.calls[1]).toEqual(vi.mocked(api.submit).mock.calls[0]);
});
