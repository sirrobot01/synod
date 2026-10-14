<script lang="ts">
  import { api, Failure, type Session } from "./api";
  import { errorView, type APIError } from "./state";
  import ErrorPanel from "./ErrorPanel.svelte";

  interface Props {
    onDone: (session: Session) => void;
    onSignIn: () => void;
  }
  let { onDone, onSignIn }: Props = $props();

  let username = $state("admin");
  let password = $state("");
  let confirm = $state("");
  let busy = $state(false);
  let failure = $state<APIError | null>(null);
  // The kit stays only in this component. It is never written to browser storage.
  let kit = $state("");
  let session = $state<Session | null>(null);
  let saved = $state(false);
  let copied = $state(false);

  async function submit(event: SubmitEvent) {
    event.preventDefault();
    failure = null;
    if (password !== confirm) {
      failure = { code: "password_mismatch", message: "The passwords are not the same.", requestID: "" };
      return;
    }
    busy = true;
    try {
      const result = await api.setup(username.trim().toLowerCase(), password);
      password = "";
      confirm = "";
      session = { actor_id: result.user.username, role: result.user.role, user_id: result.user.id };
      if (result.recovery_kit === "") {
        onDone(session);
        return;
      }
      kit = result.recovery_kit;
    } catch (error) {
      if (error instanceof Failure && error.detail.code === "setup_complete") {
        onSignIn();
        return;
      }
      failure = error instanceof Failure ? error.detail : { code: "transport_error", message: "The API could not be reached.", requestID: "" };
    } finally {
      busy = false;
    }
  }

  function download() {
    const url = URL.createObjectURL(new Blob([kit], { type: "text/plain" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = "synod-recovery-kit.txt";
    link.click();
    URL.revokeObjectURL(url);
  }

  async function copy() {
    await navigator.clipboard.writeText(kit);
    copied = true;
  }

  function finish() {
    kit = "";
    if (session) onDone(session);
  }
</script>

{#if kit === ""}
  <form onsubmit={submit} aria-labelledby="setup-heading">
    <h1 id="setup-heading">Set up Synod</h1>
    <p class="note">Create the administrator account.</p>
    <label class="field">Username
      <input id="setup-username" name="username" bind:value={username} autocomplete="username" required />
    </label>
    <label class="field">Password
      <input id="setup-password" name="password" type="password" bind:value={password} autocomplete="new-password" required />
    </label>
    <label class="field">Repeat the password
      <input id="setup-confirm" name="confirm" type="password" bind:value={confirm} autocomplete="new-password" required />
    </label>
    {#if failure}
      <ErrorPanel view={errorView(failure)} />
    {/if}
    <button type="submit" class="primary" disabled={busy}>{busy ? "Creating" : "Create account"}</button>
  </form>
{:else}
  <section aria-labelledby="kit-heading">
    <h1 id="kit-heading">Save your recovery kit</h1>
    <p class="note">The kit holds the repository password. It is shown once. Store it off every Synod device.</p>
    <pre class="mono kit">{kit}</pre>
    <div class="actions">
      <button type="button" onclick={download}>Download</button>
      <button type="button" onclick={copy}>{copied ? "Copied" : "Copy"}</button>
    </div>
    <label class="check">
      <input type="checkbox" bind:checked={saved} />
      I saved the kit in a safe place, away from my devices.
    </label>
    <button type="button" class="primary" disabled={!saved} onclick={finish}>Continue</button>
  </section>
{/if}

<style>
  h1 {
    font-size: 20px;
  }
  form,
  section {
    display: flex;
    flex-direction: column;
    gap: 14px;
  }
  form button,
  section > button {
    align-self: flex-start;
  }
  .kit {
    margin: 0;
    padding: 12px 14px;
    border: 1px solid var(--line);
    background: var(--canvas);
    font-size: 12px;
    max-height: 280px;
    overflow: auto;
  }
  .check {
    align-items: flex-start;
  }
</style>
