import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, describe, expect, it, vi } from "vitest";
import DestinationDialog from "./DestinationDialog.svelte";
import { api } from "./api";
import { node } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { addDestination: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });

async function fill() {
  await fireEvent.input(screen.getByLabelText("Name"), { target: { value: "Backups on odin" } });
  await fireEvent.change(screen.getByLabelText("Destination device"), { target: { value: node.id } });
  await fireEvent.input(screen.getByLabelText("Absolute directory path"), { target: { value: "/srv/synod" } });
}

describe("destination password", () => {
  it("uses the recovery kit password when the field is empty", async () => {
    vi.mocked(api.addDestination).mockResolvedValue({ id: "d1", name: "Backups on odin", location: { kind: "device" } });
    const saved = vi.fn();
    render(DestinationDialog, { nodes: [node], onClose: vi.fn(), onSaved: saved });
    await fill();
    expect(screen.getByText(/password in the recovery kit/)).toBeTruthy();
    await fireEvent.click(screen.getByRole("button", { name: "Add destination" }));
    await waitFor(() => expect(saved).toHaveBeenCalled());
    expect(vi.mocked(api.addDestination).mock.calls[0]?.[0].credentials.password).toBe("");
  });
  it("asks the user to save a custom password first", async () => {
    render(DestinationDialog, { nodes: [node], onClose: vi.fn(), onSaved: vi.fn() });
    await fill();
    await fireEvent.input(screen.getByLabelText("Repository password (optional)"), { target: { value: "custom-existing-password" } });
    expect((screen.getByRole("button", { name: "Add destination" }) as HTMLButtonElement).disabled).toBe(true);
    await fireEvent.click(screen.getByRole("checkbox", { name: /I saved this custom password/ }));
    expect((screen.getByRole("button", { name: "Add destination" }) as HTMLButtonElement).disabled).toBe(false);
  });
});
