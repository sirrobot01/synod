import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import Notifications from "./Notifications.svelte";
import { api, Failure } from "./api";
import { notifications } from "../test/fixtures";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { notifications: vi.fn(), configureNotifications: vi.fn(), testNotification: vi.fn() } }));
beforeEach(() => vi.mocked(api.notifications).mockResolvedValue(notifications));
afterEach(() => { cleanup(); vi.resetAllMocks(); });
it("shows a queued test without claiming delivery", async () => {
  vi.mocked(api.testNotification).mockResolvedValue({ id: "alert1", revision: 2 });
  render(Notifications);
  await fireEvent.click(await screen.findByRole("button", { name: "Send test notification" }));
  expect(await screen.findByText(/Test queued alert1/)).toBeTruthy();
  expect(screen.queryByText(/Test delivered/)).toBeNull();
});
it("clears the private URL after saving and does not store it in the browser", async () => {
  vi.mocked(api.configureNotifications).mockResolvedValue({ id: "n1", revision: 3 });
  render(Notifications);
  await screen.findByLabelText("Shoutrrr notification URL");
  expect(screen.queryByRole("button", { name: "Configure" })).toBeNull();
  await fireEvent.input(screen.getByLabelText("Shoutrrr notification URL"), { target: { value: "ntfy://private-test-topic" } });
  await fireEvent.click(screen.getByRole("button", { name: "Save notification settings" }));
  await waitFor(() => expect((screen.getByLabelText("Shoutrrr notification URL") as HTMLInputElement).value).toBe(""));
  expect(api.configureNotifications).toHaveBeenCalledWith(expect.objectContaining({ expected_revision: 2, settings: { url: "ntfy://private-test-topic", overdue_grace_hours: 24, missing_after_hours: 168, full_percent: 90 } }));
  expect(document.body.textContent).not.toContain("ntfy://private-test-topic");
});

it("retries the exact unconfirmed settings while other changes stay disabled", async () => {
  vi.mocked(api.configureNotifications).mockRejectedValueOnce(new TypeError("offline")).mockResolvedValue({ id: "n1", revision: 3 });
  render(Notifications);
  await fireEvent.input(await screen.findByLabelText("Shoutrrr notification URL"), { target: { value: "ntfy://private-topic" } });
  await fireEvent.click(screen.getByRole("button", { name: "Save notification settings" }));
  const retry = await screen.findByRole("button", { name: "Retry same settings" });
  expect((screen.getByRole("button", { name: "Turn off" }) as HTMLButtonElement).disabled).toBe(true);
  expect((screen.getByRole("button", { name: "Send test notification" }) as HTMLButtonElement).disabled).toBe(true);
  await fireEvent.click(retry);
  await screen.findByText(/Saved\./);
  expect(api.configureNotifications).toHaveBeenCalledTimes(2);
  expect(vi.mocked(api.configureNotifications).mock.calls[0]).toEqual(vi.mocked(api.configureNotifications).mock.calls[1]);
});

it("turns notifications off without a URL and preserves that request on retry", async () => {
  vi.mocked(api.configureNotifications).mockRejectedValueOnce(new TypeError("offline")).mockResolvedValue({ id: "n1", revision: 3 });
  render(Notifications);
  await screen.findByRole("button", { name: "Turn off" });
  await fireEvent.input(screen.getByLabelText("Overdue grace (hours)"), { target: { value: "" } });
  await fireEvent.click(screen.getByRole("button", { name: "Turn off" }));
  await fireEvent.click(await screen.findByRole("button", { name: "Retry disable request" }));
  await screen.findByText(/Notifications off\./);
  expect(vi.mocked(api.configureNotifications).mock.calls[0]).toEqual(vi.mocked(api.configureNotifications).mock.calls[1]);
  expect(api.configureNotifications).toHaveBeenCalledWith(expect.objectContaining({ settings: { url: "", overdue_grace_hours: 24, missing_after_hours: 168, full_percent: 90 } }));
});

it("reloads a conflicting revision before allowing another save", async () => {
  vi.mocked(api.configureNotifications).mockRejectedValue(new Failure(409, { code: "notification_settings_changed", message: "Read current settings.", requestID: "r1" }));
  render(Notifications);
  await fireEvent.input(await screen.findByLabelText("Shoutrrr notification URL"), { target: { value: "ntfy://private-topic" } });
  await fireEvent.click(screen.getByRole("button", { name: "Save notification settings" }));
  const reload = await screen.findByRole("button", { name: "Read current settings" });
  expect((screen.getByRole("button", { name: "Save notification settings" }) as HTMLButtonElement).disabled).toBe(true);
  vi.mocked(api.notifications).mockResolvedValue({ ...notifications, revision: 7, settings: { ...notifications.settings, overdue_grace_hours: 48 } });
  await fireEvent.click(reload);
  await waitFor(() => expect((screen.getByLabelText("Overdue grace (hours)") as HTMLInputElement).value).toBe("48"));
  expect((screen.getByLabelText("Shoutrrr notification URL") as HTMLInputElement).value).toBe("");
  expect((screen.getByRole("button", { name: "Save notification settings" }) as HTMLButtonElement).disabled).toBe(false);
});
