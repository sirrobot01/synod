<script lang="ts">
  import { api, detail, type MintedToken } from "./api";
  import { errorView, type APIError } from "./state";
  import Dialog from "./ui/Dialog.svelte";
  import Icon from "./ui/Icon.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";

  let { actor, onClose, onCreated }: { actor: string; onClose: () => void; onCreated: (token: MintedToken) => void } = $props();
  let name = $state("");
  let tokenActor = $state("");
  let expires = $state("");
  let minted = $state<MintedToken | null>(null);
  let busy = $state(false);
  let failure = $state<APIError | null>(null);
  let copied = $state(false);
  let copyFailed = $state(false);

  async function create() {
    if (busy || minted) return;
    busy = true;
    failure = null;
    try {
      minted = await api.createToken(name.trim(), tokenActor.trim(), expires ? new Date(`${expires}T23:59:59.999`).toISOString() : null);
      onCreated(minted);
    } catch (error) {
      failure = detail(error);
    } finally {
      busy = false;
    }
  }

  async function copy() {
    if (!minted) return;
    try {
      await navigator.clipboard.writeText(minted.token);
      copied = true;
      copyFailed = false;
    } catch {
      copied = false;
      copyFailed = true;
    }
  }
</script>

<Dialog label={minted ? "API token created" : "Create API token"} title={minted ? "API token created" : "Create API token"} width={480} onClose={() => { if (!busy) onClose(); }}>
  {#if minted}
    <p>Shown once. Copy it before you close this dialog.</p>
    <label class="field">API token<textarea class="mono" readonly rows="4" value={minted.token} onclick={(event) => event.currentTarget.select()}></textarea></label>
    {#if copyFailed}<p role="alert" class="callout attn">Copy failed. Select the token and copy it manually.</p>{/if}
  {:else}
    <form id="token-form" class="stack" onsubmit={(event) => { event.preventDefault(); create(); }}>
      <fieldset class="stack" disabled={busy}>
        <legend class="sr">Token details</legend>
        <label class="field">Name<input bind:value={name} placeholder="ci-backup" maxlength="64" required /></label>
        <label class="field">Actor<input bind:value={tokenActor} placeholder={actor} maxlength="256" /></label>
        <p class="hint">Leave empty to use your username.</p>
        <label class="field">Expires<input type="date" bind:value={expires} aria-describedby="token-expiry-hint" /></label>
        <p id="token-expiry-hint" class="hint">Expires at the end of this day in your local time. Leave empty for no expiry.</p>
      </fieldset>
      {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
    </form>
  {/if}
  {#snippet footer()}
    {#if minted}
      <button type="button" onclick={copy}><Icon name={copied ? "check" : "copy"} />{copied ? "Copied" : "Copy token"}</button>
      <button type="button" class="primary" onclick={onClose}>Done</button>
    {:else}
      <button type="button" class="ghost" disabled={busy} onclick={onClose}>Cancel</button>
      <button type="submit" form="token-form" class="primary" disabled={busy}>{busy ? "Creating" : "Create token"}</button>
    {/if}
  {/snippet}
</Dialog>
