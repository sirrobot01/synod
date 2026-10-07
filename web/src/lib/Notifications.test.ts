import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import Notifications from "./Notifications.svelte";
import { api } from "./api";
import { notifications } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { notifications: vi.fn(), configureNotifications: vi.fn(), testNotification: vi.fn() } }));
beforeEach(() => vi.mocked(api.notifications).mockResolvedValue(notifications));
afterEach(() => { cleanup(); vi.resetAllMocks(); });
it("shows a queued test without claiming delivery", async () => {
  vi.mocked(api.testNotification).mockResolvedValue({ id: "alert1", revision: 2 });
  render(Notifications);
  await fireEvent.click(await screen.findByRole("button", { name: "Send test notification" }));
  expect(await screen.findByText(/Test queued: alert1/)).toBeTruthy();
  expect(screen.queryByText(/Test delivered/)).toBeNull();
});
it("clears the private URL after saving and does not store it in the browser", async () => {
  vi.mocked(api.configureNotifications).mockResolvedValue({ id: "n1", revision: 3 });
  render(Notifications);
  await fireEvent.click(await screen.findByRole("button", { name: "Configure alerts" }));
  await fireEvent.input(screen.getByLabelText("Shoutrrr notification URL"), { target: { value: "ntfy://private-test-topic" } });
  await fireEvent.click(screen.getByRole("button", { name: "Save notification settings" }));
  await waitFor(() => expect(screen.queryByLabelText("Shoutrrr notification URL")).toBeNull());
  expect(api.configureNotifications).toHaveBeenCalledWith(expect.objectContaining({ expected_revision: 2, settings: { url: "ntfy://private-test-topic", overdue_grace_hours: 24, missing_after_hours: 168, full_percent: 90 } }));
  expect(document.body.textContent).not.toContain("ntfy://private-test-topic");
});
