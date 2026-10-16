import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/svelte";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import Settings from "./Settings.svelte";
import { api, Failure, type APIToken, type User } from "../lib/api";

vi.mock("../lib/api", async () => ({ ...await vi.importActual<typeof import("../lib/api")>("../lib/api"), api: {
  installation: vi.fn(), users: vi.fn(), tokens: vi.fn(), createUser: vi.fn(), disableUser: vi.fn(), createToken: vi.fn(), revokeToken: vi.fn(), setPassword: vi.fn(),
} }));

const session = { actor_id: "admin", role: "admin", user_id: "u1" };
const admin: User = { id: "u1", username: "admin", role: "admin", disabled: false };
const operator: User = { id: "u2", username: "alice", role: "admin", disabled: false };
const token: APIToken = { id: "t1", name: "backup", actor_id: "admin", role: "admin", created_at: "2026-10-01T00:00:00Z", last_used_at: null, expires_at: null, revoked_at: null };

beforeEach(() => {
  vi.mocked(api.installation).mockResolvedValue({ installation_id: "i1", version: "test", join_address: "synod:8443", api_ca_fingerprint: "sha256:test" });
  vi.mocked(api.users).mockResolvedValue([admin, operator]);
  vi.mocked(api.tokens).mockResolvedValue([]);
});
afterEach(() => { cleanup(); vi.resetAllMocks(); vi.unstubAllGlobals(); });

it("omits the theme control and recovery tab from Settings", async () => {
  render(Settings, { session });
  await screen.findByText("synod:8443");
  expect(screen.queryByRole("tab", { name: "Recovery kit" })).toBeNull();
  expect(screen.queryByRole("group", { name: "Theme" })).toBeNull();
});

it("adds an operator in a dialog and clears a cancelled form", async () => {
  render(Settings, { session, tab: "accounts" });
  await screen.findByText("alice");
  expect(screen.queryByLabelText("Username")).toBeNull();
  await fireEvent.click(screen.getByRole("button", { name: "Add operator" }));
  let dialog = within(screen.getByRole("dialog", { name: "Add operator" }));
  await fireEvent.input(dialog.getByLabelText("Username"), { target: { value: "discard" } });
  await fireEvent.click(dialog.getByRole("button", { name: "Cancel" }));
  expect(api.createUser).not.toHaveBeenCalled();
  await fireEvent.click(screen.getByRole("button", { name: "Add operator" }));
  dialog = within(screen.getByRole("dialog", { name: "Add operator" }));
  expect((dialog.getByLabelText("Username") as HTMLInputElement).value).toBe("");
  const bob = { ...operator, id: "u3", username: "bob" };
  vi.mocked(api.createUser).mockResolvedValue(bob);
  vi.mocked(api.users).mockResolvedValue([admin, operator, bob]);
  await fireEvent.input(dialog.getByLabelText("Username"), { target: { value: "Bob" } });
  await fireEvent.input(dialog.getByLabelText("Password"), { target: { value: "secret-password" } });
  await fireEvent.click(dialog.getByRole("button", { name: "Add operator" }));
  await screen.findByText("bob");
  expect(api.createUser).toHaveBeenCalledWith("bob", "secret-password");
  expect(screen.queryByRole("dialog")).toBeNull();
});

it("disables the selected operator by name and prevents self-disable", async () => {
  vi.mocked(api.disableUser).mockResolvedValue(undefined);
  render(Settings, { session, tab: "accounts" });
  await screen.findByText("alice");
  expect((screen.getByRole("button", { name: "Disable admin" }) as HTMLButtonElement).disabled).toBe(true);
  expect(screen.queryByLabelText("Disable user ID")).toBeNull();
  await fireEvent.click(screen.getByRole("button", { name: "Disable alice" }));
  const dialog = within(screen.getByRole("dialog", { name: "Disable alice" }));
  expect(api.disableUser).not.toHaveBeenCalled();
  await fireEvent.click(dialog.getByRole("button", { name: "Disable operator" }));
  await screen.findByText("Disabled");
  expect(api.disableUser).toHaveBeenCalledWith("u2");
  expect(screen.queryByRole("button", { name: "Disable alice" })).toBeNull();
});

it("keeps a failed disable in its dialog and allows retry", async () => {
  vi.mocked(api.disableUser).mockRejectedValueOnce(new Failure(503, { code: "dependency_unavailable", message: "Try again.", requestID: "r1" })).mockResolvedValue(undefined);
  render(Settings, { session, tab: "accounts" });
  await fireEvent.click(await screen.findByRole("button", { name: "Disable alice" }));
  await fireEvent.click(screen.getByRole("button", { name: "Disable operator" }));
  expect((await screen.findByRole("alert")).textContent).toContain("dependency_unavailable");
  expect(screen.queryByText("Disabled")).toBeNull();
  await fireEvent.click(screen.getByRole("button", { name: "Disable operator" }));
  await screen.findByText("Disabled");
});

it("keeps a failed account creation in the dialog", async () => {
  vi.mocked(api.createUser).mockRejectedValue(new Failure(409, { code: "username_taken", message: "Choose another username.", requestID: "r1" }));
  render(Settings, { session, tab: "accounts" });
  await screen.findByText("alice");
  await fireEvent.click(screen.getByRole("button", { name: "Add operator" }));
  const dialog = within(screen.getByRole("dialog"));
  await fireEvent.input(dialog.getByLabelText("Username"), { target: { value: "alice" } });
  await fireEvent.input(dialog.getByLabelText("Password"), { target: { value: "password" } });
  await fireEvent.click(dialog.getByRole("button", { name: "Add operator" }));
  expect((await dialog.findByRole("alert")).textContent).toContain("username_taken");
});

it("shows a token only in its creation dialog, including a clipboard failure", async () => {
  vi.stubGlobal("navigator", { clipboard: { writeText: vi.fn().mockRejectedValue(new Error("denied")) } });
  vi.mocked(api.createToken).mockResolvedValue({ ...token, token: "private-token-value" });
  render(Settings, { session, tab: "api-tokens" });
  await screen.findByText("No tokens.");
  expect(screen.queryByLabelText("Name")).toBeNull();
  await fireEvent.click(screen.getByRole("button", { name: "Create token" }));
  let dialog = within(screen.getByRole("dialog", { name: "Create API token" }));
  await fireEvent.input(dialog.getByLabelText("Name"), { target: { value: "backup" } });
  vi.mocked(api.tokens).mockResolvedValue([token]);
  await fireEvent.click(dialog.getByRole("button", { name: "Create token" }));
  dialog = within(await screen.findByRole("dialog", { name: "API token created" }));
  expect(api.createToken).toHaveBeenCalledWith("backup", "", null);
  expect((dialog.getByLabelText("API token") as HTMLTextAreaElement).value).toBe("private-token-value");
  await fireEvent.click(dialog.getByRole("button", { name: "Copy token" }));
  expect((await dialog.findByRole("alert")).textContent).toContain("copy it manually");
  await fireEvent.click(dialog.getByRole("button", { name: "Done" }));
  expect(screen.queryByDisplayValue("private-token-value")).toBeNull();
  expect(await screen.findByText("backup")).toBeTruthy();
  await fireEvent.click(screen.getByRole("button", { name: "Create token" }));
  expect((screen.getByLabelText("Name") as HTMLInputElement).value).toBe("");
});

it.each([
  { selected: "2030-03-10", end: new Date(2030, 2, 10, 23, 59, 59, 999) },
  { selected: "2030-12-31", end: new Date(2030, 11, 31, 23, 59, 59, 999) },
])("expires a token at the local end of $selected and lists only the date", async ({ selected, end }) => {
  const expiringToken = { ...token, expires_at: end.toISOString() };
  vi.mocked(api.createToken).mockResolvedValue({ ...expiringToken, token: "private-token-value" });
  render(Settings, { session, tab: "api-tokens" });
  await screen.findByText("No tokens.");
  await fireEvent.click(screen.getByRole("button", { name: "Create token" }));
  const dialog = within(screen.getByRole("dialog", { name: "Create API token" }));
  const expiry = dialog.getByLabelText("Expires") as HTMLInputElement;
  expect(expiry.type).toBe("date");
  await fireEvent.input(dialog.getByLabelText("Name"), { target: { value: "backup" } });
  await fireEvent.input(expiry, { target: { value: selected } });
  vi.mocked(api.tokens).mockResolvedValue([expiringToken]);
  await fireEvent.click(dialog.getByRole("button", { name: "Create token" }));
  await screen.findByRole("dialog", { name: "API token created" });
  expect(api.createToken).toHaveBeenCalledWith("backup", "", end.toISOString());
  await fireEvent.click(screen.getByRole("button", { name: "Done" }));
  const row = (await screen.findByText("backup")).closest("tr")!;
  expect(within(row).getAllByRole("cell")[4]!.textContent).toBe(end.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" }));
});

it("distinguishes expired tokens from active and revoked tokens", async () => {
  vi.mocked(api.tokens).mockResolvedValue([
    token,
    { ...token, id: "t2", name: "old", expires_at: "2000-01-01T00:00:00Z" },
    { ...token, id: "t3", name: "revoked", revoked_at: "2000-01-01T00:00:00Z" },
  ]);
  render(Settings, { session, tab: "api-tokens" });
  await screen.findByText("Expired");
  expect(screen.getByText("Active")).toBeTruthy();
  expect(screen.getByText("Revoked")).toBeTruthy();
});

it("reports a list failure and refreshes without claiming the list is empty", async () => {
  vi.mocked(api.users).mockRejectedValueOnce(new Error("offline"));
  render(Settings, { session, tab: "accounts" });
  await screen.findByText("Operators could not be loaded.");
  expect(screen.queryByText("No operators.")).toBeNull();
  await fireEvent.click(screen.getByRole("button", { name: "Refresh operators" }));
  await screen.findByText("alice");
  await waitFor(() => expect(screen.queryByRole("alert")).toBeNull());
});
