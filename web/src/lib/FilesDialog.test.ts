import { cleanup, fireEvent, render, screen } from "@testing-library/svelte";
import { afterEach, expect, it, vi } from "vitest";
import FilesDialog from "./FilesDialog.svelte";
import { api, type FileHeader } from "./api";
import { app, destination, snapshot } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { files: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });

it("opens a folder of the chosen copy", async () => {
  vi.mocked(api.files).mockResolvedValueOnce({ entries: [{ name: "srv", type: "directory", size: 0 }], path_type: "directory" }).mockResolvedValueOnce({ entries: [{ name: "notes.md", type: "file", size: 3 }], path_type: "directory" });
  render(FilesDialog, { item: app, copies: [snapshot], destinations: [destination], onClose: vi.fn() });
  await fireEvent.click(await screen.findByRole("button", { name: "srv/" }));
  expect(await screen.findByText("notes.md")).toBeTruthy();
  expect(vi.mocked(api.files).mock.calls[1]?.[1]).toMatchObject({ snapshot: "s1", destination: "d1", path: "/srv" });
  expect(screen.getByRole("button", { name: "Download notes.md" })).toBeTruthy();
});

it("ignores an old folder reply after the copy changes", async () => {
  let release: (value: FileHeader) => void = () => {};
  vi.mocked(api.files).mockImplementationOnce(() => new Promise((resolve) => { release = resolve; })).mockResolvedValueOnce({ entries: [{ name: "new-file", type: "file", size: 3 }], path_type: "directory" });
  const older = { ...snapshot, id: "s2", completed_at: "2026-10-06T03:00:00Z" };
  render(FilesDialog, { item: app, copies: [snapshot, older], destinations: [destination], onClose: vi.fn() });
  await fireEvent.change(screen.getByLabelText("Snapshot"), { target: { value: "s2" } });
  expect(await screen.findByText("new-file")).toBeTruthy();
  expect(api.files).toHaveBeenLastCalledWith("a1", expect.objectContaining({ snapshot: "s2", path: "/" }));
  release({ entries: [{ name: "old-file", type: "file", size: 3 }], path_type: "directory" });
  await Promise.resolve();
  expect(screen.queryByText("old-file")).toBeNull();
});
