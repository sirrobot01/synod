import { cleanup, render, screen } from "@testing-library/svelte";
import { afterEach, describe, expect, it, vi } from "vitest";
import Destinations from "./Destinations.svelte";
import { api } from "../lib/api";
import { destination, destinations, node } from "../test/fixtures";
vi.mock("../lib/api", async () => {
  const real = await vi.importActual<typeof import("../lib/api")>("../lib/api");
  return { ...real, api: { destinations: vi.fn(), nodes: vi.fn() } };
});
afterEach(() => { cleanup(); vi.resetAllMocks(); });
describe("space reclamation", () => {
  it("names the maintenance device and links its failed run", async () => {
    const last = { id: "op-maintenance", node_id: "n1", destination_id: "d1", state: "failed" as const, result: { completed_at: "2026-10-07T03:00:00Z", failure_code: "maintenance_failed" } };
    vi.mocked(api.destinations).mockResolvedValue({ ...destinations, maintenance: [{ destination_id: destination.id, node_id: node.id, last }] });
    vi.mocked(api.nodes).mockResolvedValue([node]);
    render(Destinations, { onOperation: () => {} });
    expect(await screen.findByText("Space reclamation")).toBeTruthy();
    expect(screen.getByText("odin")).toBeTruthy();
    expect(screen.getByRole("link", { name: /failed/ }).getAttribute("href")).toContain("op-maintenance");
  });
  it("tells the user to approve a device when no device can maintain the repository", async () => {
    vi.mocked(api.destinations).mockResolvedValue({ ...destinations, maintenance: [{ destination_id: destination.id }] });
    vi.mocked(api.nodes).mockResolvedValue([]);
    render(Destinations, { onOperation: () => {} });
    expect(await screen.findByText("No device")).toBeTruthy();
    expect(screen.getByText(/Approve a device to remove expired snapshots/)).toBeTruthy();
  });
});
