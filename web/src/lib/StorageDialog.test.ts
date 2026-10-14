import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, describe, expect, it, vi } from "vitest";
import StorageDialog from "./StorageDialog.svelte";
import { api } from "./api";
import { node } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { addDestination: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });

async function fill() {
  await fireEvent.change(screen.getByLabelText("Device that stores the snapshots"), { target: { value: node.id } });
  await fireEvent.input(screen.getByLabelText("Path"), { target: { value: "/srv/synod" } });
  await fireEvent.input(screen.getByLabelText("Name"), { target: { value: "Backups on odin" } });
}

describe("storage types", () => {
  it("shows four types with what each survives, and recommends another device", () => {
    render(StorageDialog, { nodes: [node], onClose: vi.fn(), onSaved: vi.fn() });
    const types = ["Another device", "Local disk", "SFTP", "S3"].map((name) => screen.getByRole("button", { name: new RegExp(`^${name}`) }));
    expect(types).toHaveLength(4);
    expect(types[0]!.getAttribute("aria-pressed")).toBe("true");
    expect(types[0]!.textContent).toContain("recommended");
    expect(types[0]!.textContent).toContain("Survives the loss of the app host");
    expect(types[1]!.textContent).toContain("Lost with that host");
  });
  it("shows the fields of the chosen type", async () => {
    render(StorageDialog, { nodes: [node], onClose: vi.fn(), onSaved: vi.fn() });
    await fireEvent.click(screen.getByRole("button", { name: /^S3/ }));
    expect(screen.getByLabelText("Bucket")).toBeTruthy();
    expect(screen.queryByLabelText("Path")).toBeNull();
    await fireEvent.click(screen.getByRole("button", { name: /^SFTP/ }));
    expect(screen.getByLabelText("SSH private key")).toBeTruthy();
    await fireEvent.click(screen.getByRole("button", { name: /^Local disk/ }));
    expect(screen.getByLabelText("Device with the disk")).toBeTruthy();
  });
  it("adds a folder on another device with the recovery kit password", async () => {
    vi.mocked(api.addDestination).mockResolvedValue({ id: "d1", name: "Backups on odin", location: { kind: "device" } });
    const saved = vi.fn();
    render(StorageDialog, { nodes: [node], onClose: vi.fn(), onSaved: saved });
    await fill();
    expect(screen.getByText(/repository password in the recovery kit/)).toBeTruthy();
    await fireEvent.click(screen.getByRole("button", { name: "Add storage" }));
    await waitFor(() => expect(saved).toHaveBeenCalled());
    const entry = vi.mocked(api.addDestination).mock.calls[0]![0];
    expect(entry.credentials.password).toBe("");
    expect(entry.destination).toMatchObject({ name: "Backups on odin", node_id: "n1", location: { kind: "device", path: "/srv/synod", device: { node_id: "n1" } } });
  });
  it("asks the reader to save a custom password first", async () => {
    render(StorageDialog, { nodes: [node], onClose: vi.fn(), onSaved: vi.fn() });
    await fill();
    await fireEvent.input(screen.getByLabelText("Custom password (optional)"), { target: { value: "custom-existing-password" } });
    expect((screen.getByRole("button", { name: "Add storage" }) as HTMLButtonElement).disabled).toBe(true);
    await fireEvent.click(screen.getByRole("checkbox", { name: /I saved this password/ }));
    expect((screen.getByRole("button", { name: "Add storage" }) as HTMLButtonElement).disabled).toBe(false);
  });
});
