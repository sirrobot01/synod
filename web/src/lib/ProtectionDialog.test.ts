import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, describe, expect, it, vi } from "vitest";
import ProtectionDialog from "./ProtectionDialog.svelte";
import { api, Failure } from "./api";
import { app, plan, destination } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { protect: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });

describe("protection confirmation", () => {
  it("shows the included and excluded paths before it can write", async () => {
    vi.mocked(api.protect).mockResolvedValue({ id: "p1", revision: 4 });
    const saved = vi.fn();
    render(ProtectionDialog, { plan, apps: [app], destinations: [destination], all: true, onClose: vi.fn(), onSaved: saved });
    expect(api.protect).not.toHaveBeenCalled();
    await fireEvent.click(screen.getByRole("button", { name: "Review paths" }));
    expect(screen.getByText("/srv/media")).toBeTruthy();
    expect(screen.getByText("Never")).toBeTruthy();
    expect(api.protect).not.toHaveBeenCalled();
    await fireEvent.click(screen.getByRole("button", { name: "Confirm plan" }));
    await waitFor(() => expect(saved).toHaveBeenCalled());
    expect(api.protect).toHaveBeenCalledWith(expect.objectContaining({ expected_revision: 3, all: true, enabled: true, settings: app.settings }));
  });
  it("keeps the request unchanged after a lost reply", async () => {
    vi.mocked(api.protect).mockRejectedValueOnce(new TypeError("network")).mockResolvedValueOnce({ id: "p1", revision: 4 });
    render(ProtectionDialog, { plan, apps: [app], destinations: [destination], all: false, onClose: vi.fn(), onSaved: vi.fn() });
    await fireEvent.click(screen.getByRole("button", { name: "Review paths" }));
    await fireEvent.click(screen.getByRole("button", { name: "Confirm plan" }));
    await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
    expect(api.protect).toHaveBeenCalledTimes(2);
    expect(vi.mocked(api.protect).mock.calls[1]?.[0]).toEqual(vi.mocked(api.protect).mock.calls[0]?.[0]);
  });
  it("requires a fresh plan after a revision conflict", async () => {
    vi.mocked(api.protect).mockRejectedValue(new Failure(409, { code: "protect_plan_changed", message: "Read the plan again.", requestID: "r1" }));
    const reread = vi.fn();
    render(ProtectionDialog, { plan, apps: [app], destinations: [destination], all: true, onClose: vi.fn(), onSaved: reread });
    await fireEvent.click(screen.getByRole("button", { name: "Review paths" }));
    await fireEvent.click(screen.getByRole("button", { name: "Confirm plan" }));
    await fireEvent.click(await screen.findByRole("button", { name: "Read current plan" }));
    expect(reread).toHaveBeenCalled();
    expect(api.protect).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole("button", { name: "Confirm plan" })).toBeNull();
  });
  it("includes a selected Large path for one app", async () => {
    vi.mocked(api.protect).mockResolvedValue({ id: "p1", revision: 4 });
    render(ProtectionDialog, { plan, apps: [app], destinations: [destination], all: false, onClose: vi.fn(), onSaved: vi.fn() });
    await fireEvent.click(screen.getByRole("checkbox", { name: "/srv/media" }));
    await fireEvent.click(screen.getByRole("button", { name: "Review paths" }));
    expect(screen.getByText("You included this Large path.")).toBeTruthy();
    await fireEvent.click(screen.getByRole("button", { name: "Confirm plan" }));
    await waitFor(() => expect(api.protect).toHaveBeenCalled());
    expect(vi.mocked(api.protect).mock.calls[0]?.[0].settings.include_large).toEqual(["/srv/media"]);
  });
  it("never sends Large paths in the plan for all apps", async () => {
    vi.mocked(api.protect).mockResolvedValue({ id: "p1", revision: 4 });
    render(ProtectionDialog, { plan, apps: [app], destinations: [destination], all: true, onClose: vi.fn(), onSaved: vi.fn() });
    expect(screen.queryByRole("checkbox", { name: "/srv/media" })).toBe(null);
    await fireEvent.click(screen.getByRole("button", { name: "Review paths" }));
    await fireEvent.click(screen.getByRole("button", { name: "Confirm plan" }));
    await waitFor(() => expect(api.protect).toHaveBeenCalled());
    expect(vi.mocked(api.protect).mock.calls[0]?.[0].settings.include_large).toBeUndefined();
  });
});
