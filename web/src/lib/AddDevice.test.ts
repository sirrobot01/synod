import { cleanup, fireEvent, render, screen } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import AddDevice from "./AddDevice.svelte";
import { api, type Node } from "./api";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { installation: vi.fn(), nodes: vi.fn(), registerJoinToken: vi.fn(), approveNode: vi.fn() } }));
const existing: Node = { id: "n1", name: "server", architecture: "amd64", runtime_version: "29", agent_version: "v0", enrolled_at: "2026-10-14T00:00:00Z", approved_at: "2026-10-14T00:00:00Z", connected_at: null, last_seen_at: null };
const added: Node = { ...existing, id: "n2", name: "pi", architecture: "arm64", approved_at: null };
beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  vi.mocked(api.installation).mockResolvedValue({ installation_id: "7d0f0f5e-9f5e-4a39-8d3a-2b3a4c5d6e7f", version: "v0", join_address: "192.0.2.20:8443", api_ca_fingerprint: "sha256:" + "ab".repeat(32) });
  vi.mocked(api.nodes).mockResolvedValue([existing]);
  vi.mocked(api.registerJoinToken).mockResolvedValue({ id: "t1", expires_at: "2026-10-15T00:00:00Z" });
});
afterEach(() => { cleanup(); vi.useRealTimers(); vi.resetAllMocks(); });
it("shows a filled-in Compose file, then approves the device that joins", async () => {
  const onChanged = vi.fn();
  render(AddDevice, { onClose: vi.fn(), onChanged });
  const compose = await screen.findByText(/SYNOD_JOIN_TOKEN: "synod_join_/);
  const token = /synod_join_[A-Za-z0-9_-]+/.exec(compose.textContent ?? "")![0];
  const decoded = JSON.parse(atob(token.slice(11).replaceAll("-", "+").replaceAll("_", "/")));
  const [secret, ttl] = vi.mocked(api.registerJoinToken).mock.calls[0]!;
  expect(decoded).toMatchObject({ address: "192.0.2.20:8443", secret });
  expect(ttl).toBe(86400);
  await fireEvent.click(screen.getByRole("tab", { name: "As a Linux service" }));
  expect(screen.getByText(new RegExp(`synod agent join '${token}'`))).toBeTruthy();
  // The device joins after the dialog opened. A known device is not the new one.
  vi.mocked(api.nodes).mockResolvedValue([existing, added]);
  vi.mocked(api.approveNode).mockResolvedValue({ ...added, approved_at: "2026-10-14T01:00:00Z" });
  await vi.advanceTimersByTimeAsync(3000);
  await fireEvent.click(await screen.findByRole("button", { name: "Approve" }));
  expect(api.approveNode).toHaveBeenCalledWith("n2");
  expect(await screen.findByText("pi approved")).toBeTruthy();
  expect(onChanged).toHaveBeenCalled();
});
