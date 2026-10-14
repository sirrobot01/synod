import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/svelte";
import { afterEach, describe, expect, it, vi } from "vitest";
import EditSchedule from "./EditSchedule.svelte";
import { api, Failure } from "./api";
import { app, plan, destination } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { protect: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });

describe("editing the schedule of one app", () => {
  it("shows the paths before it can save", async () => {
    vi.mocked(api.protect).mockResolvedValue({ id: "p1", revision: 4 });
    const saved = vi.fn();
    render(EditSchedule, { plan, item: app, destinations: [destination], onClose: vi.fn(), onSaved: saved });
    await fireEvent.click(screen.getByRole("button", { name: "Review" }));
    const table = within(screen.getByRole("table"));
    expect(table.getByText("/srv/media")).toBeTruthy();
    expect(table.getByText("never")).toBeTruthy();
    expect(api.protect).not.toHaveBeenCalled();
    await fireEvent.click(screen.getByRole("button", { name: "Save schedule" }));
    await waitFor(() => expect(saved).toHaveBeenCalled());
    expect(api.protect).toHaveBeenCalledWith(expect.objectContaining({ expected_revision: 3, all: false, app_ids: ["a1"], enabled: true }));
  });
  it("keeps the request unchanged after a lost reply", async () => {
    vi.mocked(api.protect).mockRejectedValueOnce(new TypeError("network")).mockResolvedValueOnce({ id: "p1", revision: 4 });
    render(EditSchedule, { plan, item: app, destinations: [destination], onClose: vi.fn(), onSaved: vi.fn() });
    await fireEvent.click(screen.getByRole("button", { name: "Review" }));
    await fireEvent.click(screen.getByRole("button", { name: "Save schedule" }));
    await fireEvent.click(await screen.findByRole("button", { name: "Retry same request" }));
    expect(api.protect).toHaveBeenCalledTimes(2);
    expect(vi.mocked(api.protect).mock.calls[1]?.[0]).toEqual(vi.mocked(api.protect).mock.calls[0]?.[0]);
  });
  it("requires a fresh plan after a revision conflict", async () => {
    vi.mocked(api.protect).mockRejectedValue(new Failure(409, { code: "protect_plan_changed", message: "Read the plan again.", requestID: "r1" }));
    const reread = vi.fn();
    render(EditSchedule, { plan, item: app, destinations: [destination], onClose: vi.fn(), onSaved: reread });
    await fireEvent.click(screen.getByRole("button", { name: "Review" }));
    await fireEvent.click(screen.getByRole("button", { name: "Save schedule" }));
    await fireEvent.click(await screen.findByRole("button", { name: "Reload schedule" }));
    expect(reread).toHaveBeenCalled();
    expect(api.protect).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole("button", { name: "Save schedule" })).toBeNull();
  });
  it("includes a selected excluded path", async () => {
    vi.mocked(api.protect).mockResolvedValue({ id: "p1", revision: 4 });
    render(EditSchedule, { plan, item: app, destinations: [destination], onClose: vi.fn(), onSaved: vi.fn() });
    await fireEvent.click(screen.getByRole("checkbox", { name: "/srv/media" }));
    await fireEvent.click(screen.getByRole("button", { name: "Review" }));
    expect(screen.getByText("Included by you")).toBeTruthy();
    await fireEvent.click(screen.getByRole("button", { name: "Save schedule" }));
    await waitFor(() => expect(api.protect).toHaveBeenCalled());
    expect(vi.mocked(api.protect).mock.calls[0]?.[0].settings.include_large).toEqual(["/srv/media"]);
  });
  it("offers a network share for inclusion and skips it by default", async () => {
    vi.mocked(api.protect).mockResolvedValue({ id: "p1", revision: 4 });
    const shared = { ...app, app: { ...app.app, definition: { ...app.app.definition, paths: [...(app.app.definition.paths ?? []), { source: "/mnt/nas/photos", kind: "bind", status: "Remote" as const, reason: "This path is on a network share (nfs4). Network shares are skipped by default." }] } } };
    render(EditSchedule, { plan, item: shared, destinations: [destination], onClose: vi.fn(), onSaved: vi.fn() });
    expect(screen.getByText("Excluded paths to include")).toBeTruthy();
    await fireEvent.click(screen.getByRole("checkbox", { name: "/mnt/nas/photos" }));
    await fireEvent.click(screen.getByRole("button", { name: "Review" }));
    expect(screen.getByText("Included by you")).toBeTruthy();
    await fireEvent.click(screen.getByRole("button", { name: "Save schedule" }));
    await waitFor(() => expect(api.protect).toHaveBeenCalled());
    expect(vi.mocked(api.protect).mock.calls[0]?.[0].settings.include_large).toEqual(["/mnt/nas/photos"]);
  });
  it("stops the backups of an app when the reader clears the box", async () => {
    vi.mocked(api.protect).mockResolvedValue({ id: "p1", revision: 4 });
    render(EditSchedule, { plan, item: app, destinations: [destination], onClose: vi.fn(), onSaved: vi.fn() });
    await fireEvent.click(screen.getByRole("checkbox", { name: "Back up this app daily" }));
    await fireEvent.click(screen.getByRole("button", { name: "Review" }));
    expect(screen.getByText(/Daily backups of notes stop/)).toBeTruthy();
    await fireEvent.click(screen.getByRole("button", { name: "Save schedule" }));
    await waitFor(() => expect(api.protect).toHaveBeenCalledWith(expect.objectContaining({ enabled: false })));
  });
});
