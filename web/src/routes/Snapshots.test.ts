import { cleanup, fireEvent, render, screen } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import Snapshots from "./Snapshots.svelte";
import { api, type FileHeader } from "../lib/api";
import { plan, destinations, node, snapshot } from "../test/fixtures";
vi.mock("../lib/api", async () => ({ ...await vi.importActual<typeof import("../lib/api")>("../lib/api"), api: { protection: vi.fn(), destinations: vi.fn(), nodes: vi.fn(), snapshots: vi.fn(), files: vi.fn() } }));
beforeEach(() => {
  vi.mocked(api.protection).mockResolvedValue(plan); vi.mocked(api.destinations).mockResolvedValue(destinations); vi.mocked(api.nodes).mockResolvedValue([node]);
  vi.mocked(api.snapshots).mockResolvedValue({ snapshots: [snapshot] });
});
afterEach(() => { cleanup(); vi.resetAllMocks(); });
it("reports partial repository coverage", async () => {
  vi.mocked(api.snapshots).mockResolvedValue({ snapshots: [snapshot], warning_codes: ["destination_unavailable"], truncated: true });
  render(Snapshots, { id: "a1", onApp: vi.fn(), onOperation: vi.fn() });
  expect(await screen.findByText(/Some repositories could not be read/)).toBeTruthy();
  expect(screen.getByText(/snapshot list is incomplete/)).toBeTruthy();
});
it("ignores an old directory reply after the snapshot selection changes", async () => {
  vi.mocked(api.snapshots).mockResolvedValue({ snapshots: [snapshot, { ...snapshot, id: "s2", completed_at: "2026-10-06T03:00:00Z" }] });
  let release: (value: FileHeader) => void = () => {};
  vi.mocked(api.files).mockImplementationOnce(() => new Promise(resolve => { release = resolve; })).mockResolvedValueOnce({ entries: [{ name: "new-file", type: "file", size: 3 }], path_type: "directory" });
  render(Snapshots, { id: "a1", onApp: vi.fn(), onOperation: vi.fn() });
  await fireEvent.click(await screen.findByRole("button", { name: /Oct 7/ }));
  await fireEvent.click(screen.getByRole("button", { name: /Oct 6/ }));
  expect(await screen.findByText("new-file")).toBeTruthy();
  release({ entries: [{ name: "old-file", type: "file", size: 3 }], path_type: "directory" });
  await Promise.resolve();
  expect(screen.queryByText("old-file")).toBeNull();
});
it("does not call a repository failure an empty history", async () => {
  vi.mocked(api.snapshots).mockRejectedValue(new TypeError("offline"));
  render(Snapshots, { id: "a1", onApp: vi.fn(), onOperation: vi.fn() });
  expect(await screen.findByRole("alert")).toBeTruthy();
  expect(screen.queryByText(/No saved snapshots were found/)).toBeNull();
});
it("shows the limits and restorable platforms of a snapshot before restore", async () => {
  vi.mocked(api.files).mockResolvedValue({ entries: [], path_type: "directory" });
  vi.mocked(api.snapshots).mockResolvedValue({ snapshots: [{ ...snapshot, consistency: "unverified", limits: ["image_platforms_limited"], restore_architectures: ["arm64"] }] });
  render(Snapshots, { id: "a1", onApp: vi.fn(), onOperation: vi.fn() });
  await fireEvent.click(await screen.findByRole("button", { name: /Oct 7/ }));
  expect(await screen.findByText(/holds only the source architecture of an image/)).toBeTruthy();
  expect(screen.getByText("arm64", { selector: "dd" })).toBeTruthy();
  expect(screen.getByText("unverified", { selector: "dd" })).toBeTruthy();
});
