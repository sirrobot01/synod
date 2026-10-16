<script lang="ts">
  import { api, detail, type User } from "./api";
  import { errorView, type APIError } from "./state";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";

  let { onClose, onCreated }: { onClose: () => void; onCreated: (user: User) => void } = $props();
  let username = $state("");
  let password = $state("");
  let busy = $state(false);
  let failure = $state<APIError | null>(null);

  async function create() {
    if (busy) return;
    busy = true;
    failure = null;
    try {
      const user = await api.createUser(username.trim().toLowerCase(), password);
      password = "";
      onCreated(user);
    } catch (error) {
      failure = detail(error);
    } finally {
      busy = false;
    }
  }
</script>

<Dialog label="Add operator" title="Add operator" width={480} onClose={() => { if (!busy) onClose(); }}>
  <form id="operator-form" class="stack" onsubmit={(event) => { event.preventDefault(); create(); }}>
    <fieldset class="stack" disabled={busy}>
      <legend class="sr">Operator account</legend>
      <label class="field">Username<input bind:value={username} autocomplete="off" maxlength="64" pattern="[a-zA-Z0-9._\-]+" required /></label>
      <p class="hint">Letters, numbers, hyphens, underscores, and dots.</p>
      <label class="field">Password<input type="password" bind:value={password} autocomplete="new-password" required /></label>
    </fieldset>
    {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  </form>
  {#snippet footer()}
    <button type="button" class="ghost" disabled={busy} onclick={onClose}>Cancel</button>
    <button type="submit" form="operator-form" class="primary" disabled={busy}>{busy ? "Adding" : "Add operator"}</button>
  {/snippet}
</Dialog>
