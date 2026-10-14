import { cleanup, fireEvent, render, screen } from "@testing-library/svelte";
import { afterEach, expect, it, vi } from "vitest";
import Setup from "./Setup.svelte";
import { api, Failure } from "./api";
vi.mock("./api", async () => ({ ...await vi.importActual<typeof import("./api")>("./api"), api: { setup: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });
const user = { id: "u1", username: "admin", role: "admin", disabled: false };
async function createAccount(password = "correct horse battery", confirm = password) {
  await fireEvent.input(screen.getByLabelText("Password"), { target: { value: password } });
  await fireEvent.input(screen.getByLabelText("Repeat the password"), { target: { value: confirm } });
  await fireEvent.click(screen.getByRole("button", { name: "Create account" }));
}
it("creates the account, shows the kit once, and continues only after the operator saves it", async () => {
  vi.mocked(api.setup).mockResolvedValue({ user, recovery_kit: "SYNOD RECOVERY KIT\nprivate-kit-password\n" });
  const onDone = vi.fn();
  render(Setup, { onDone, onSignIn: vi.fn() });
  await createAccount();
  expect(api.setup).toHaveBeenCalledWith("admin", "correct horse battery");
  expect(await screen.findByText(/private-kit-password/)).toBeTruthy();
  const next = screen.getByRole("button", { name: "Continue" }) as HTMLButtonElement;
  expect(next.disabled).toBe(true);
  await fireEvent.click(screen.getByRole("checkbox"));
  await fireEvent.click(next);
  expect(onDone).toHaveBeenCalledWith({ actor_id: "admin", role: "admin", user_id: "u1" });
  expect(document.body.textContent).not.toContain("private-kit-password");
});
it("refuses passwords that differ without calling the API", async () => {
  render(Setup, { onDone: vi.fn(), onSignIn: vi.fn() });
  await createAccount("correct horse battery", "another password");
  expect(await screen.findByText("The passwords are not the same.")).toBeTruthy();
  expect(api.setup).not.toHaveBeenCalled();
});
it("sends the operator to sign in when another account already exists", async () => {
  vi.mocked(api.setup).mockRejectedValue(new Failure(409, { code: "setup_complete", message: "An account exists. Sign in.", requestID: "r1" }));
  const onSignIn = vi.fn();
  render(Setup, { onDone: vi.fn(), onSignIn });
  await createAccount();
  await vi.waitFor(() => expect(onSignIn).toHaveBeenCalled());
});
