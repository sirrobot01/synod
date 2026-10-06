import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import Nodes from "./Nodes.svelte";
import { api } from "../lib/api";
vi.mock("../lib/api", async () => {
  const real = await vi.importActual<typeof import("../lib/api")>("../lib/api");
  return { ...real, api: { nodes: vi.fn(), approveNode: vi.fn() } };
});
const node = {id:"n1", name:"odin", architecture:"arm64", runtime_version:"29.7.2", agent_version:"dev", approved_at:null, last_seen_at:"2026-10-06T08:00:00Z", connected_at:null, enrolled_at:"2026-10-06T07:00:00Z"};
beforeEach(() => { vi.mocked(api.nodes).mockResolvedValue([node]); vi.mocked(api.approveNode).mockResolvedValue({...node,approved_at:"2026-10-06T09:00:00Z"}); });
afterEach(() => { cleanup(); vi.resetAllMocks(); });
describe("device facts", () => {
  it("reads a joined device and approves it through the API", async () => {
    render(Nodes, {id:"", onOpen:()=>{}});
    expect(await screen.findByText("odin")).toBeTruthy();
    expect(screen.getByText(node.last_seen_at)).toBeTruthy();
    await fireEvent.click(screen.getByRole("button", {name:"Approve"}));
    expect(await screen.findByText("Approved")).toBeTruthy();
    expect(api.approveNode).toHaveBeenCalledWith("n1");
  });
  it("keeps the last facts when a refresh fails", async () => {
    render(Nodes, {id:"", onOpen:()=>{}});
    await screen.findByText("odin");
    vi.mocked(api.nodes).mockRejectedValue(new Error("offline"));
    await fireEvent.click(screen.getByRole("button", {name:"Refresh"}));
    expect(await screen.findByRole("alert")).toBeTruthy();
    expect(screen.getByText("odin")).toBeTruthy();
  });
  it("does not call a failed initial read an empty installation", async () => {
    vi.mocked(api.nodes).mockRejectedValue(new Error("offline"));
    render(Nodes, {id:"", onOpen:()=>{}});
    await screen.findByRole("alert");
    expect(screen.queryByText("No devices have joined yet.")).toBe(null);
  });
  it("waits for host facts before offering approval", async () => {
    vi.mocked(api.nodes).mockResolvedValue([{...node, architecture:""}]);
    render(Nodes, {id:"n1", onOpen:()=>{}});
    await waitFor(() => expect((screen.getByRole("button", {name:"Approve"}) as HTMLButtonElement).disabled).toBe(true));
  });
});
