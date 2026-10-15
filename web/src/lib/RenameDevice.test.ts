import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, expect, it, vi } from "vitest";
import RenameDevice from "./RenameDevice.svelte";
import { api, Failure } from "./api";
import { node } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { renameNode: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });

const container = { ...node, name: "3f9a2c1b7e4d" };

it("renames a device that carries a container ID as its name", async () => {
  vi.mocked(api.renameNode).mockResolvedValue({ ...container, name: "odin" });
  const renamed = vi.fn();
  render(RenameDevice, { node: container, onClose: vi.fn(), onRenamed: renamed });
  const save = screen.getByRole("button", { name: "Rename" }) as HTMLButtonElement;
  expect(save.disabled).toBe(true);
  await fireEvent.input(screen.getByLabelText("Name"), { target: { value: "odin" } });
  await fireEvent.click(save);
  await waitFor(() => expect(renamed).toHaveBeenCalledWith(expect.objectContaining({ name: "odin" })));
  expect(api.renameNode).toHaveBeenCalledWith("n1", "odin");
});

it("says plainly that another device has the name", async () => {
  vi.mocked(api.renameNode).mockRejectedValue(new Failure(409, { code: "name_taken", message: "name_taken", requestID: "r1" }));
  render(RenameDevice, { node: container, onClose: vi.fn(), onRenamed: vi.fn() });
  await fireEvent.input(screen.getByLabelText("Name"), { target: { value: "server" } });
  await fireEvent.click(screen.getByRole("button", { name: "Rename" }));
  expect(await screen.findByText("Another device is named server. Choose another name.")).toBeTruthy();
});
