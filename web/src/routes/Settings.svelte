<script lang="ts">
  import { onMount, tick } from "svelte";
  import { api, detail, type APIToken, type Installation, type MintedToken, type Session } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import { swap } from "../lib/router.svelte";
  import { when } from "../lib/model";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Notifications from "../lib/Notifications.svelte";

  let { session, tab = "general", theme = "dark", onTheme = () => {} }: { session: Session; tab?: string; theme?: "dark" | "light"; onTheme?: () => void } = $props();

  const tabs = [
    { id: "general", label: "General" },
    { id: "notifications", label: "Notifications" },
    { id: "accounts", label: "Accounts" },
    { id: "recovery-kit", label: "Recovery kit" },
  ];
  let current = $derived(tabs.some((t) => t.id === tab) ? tab : "general");

  // A tab is part of the address, so a reload and a shared link open the same tab.
  async function choose(id: string, focus = false) {
    swap("settings", id);
    if (focus) {
      await tick();
      document.getElementById(`tab-${id}`)?.focus();
    }
  }
  function key(event: KeyboardEvent) {
    const index = tabs.findIndex((t) => t.id === current);
    const moves: Record<string, number> = { ArrowRight: index + 1, ArrowLeft: index - 1, Home: 0, End: tabs.length - 1 };
    const target = moves[event.key];
    if (target === undefined) return;
    event.preventDefault();
    choose(tabs[(target + tabs.length) % tabs.length]!.id, true);
  }

  const now = Date.now();
  let installation = $state<Installation | null>(null);
  let installationFailure = $state<APIError | null>(null);
  onMount(async () => {
    try { installation = await api.installation(); } catch (error) { installationFailure = detail(error); }
  });

  // Each region keeps its own failure, so a refused token never appears under
  // the password form that did not cause it.
  let tokens = $state<APIToken[]>([]);
  let tokenFailure = $state<APIError | null>(null);
  let minted = $state<MintedToken | null>(null);
  let tokenName = $state("");
  let tokenActor = $state("");
  let tokenExpires = $state("");
  let tokenBusy = $state(false);

  let accountFailure = $state<APIError | null>(null);
  let created = $state("");
  let newUsername = $state("");
  let newPassword = $state("");
  let disableID = $state("");
  let disabled = $state("");
  let accountBusy = $state(false);

  let passwordFailure = $state<APIError | null>(null);
  let currentPassword = $state("");
  let nextPassword = $state("");
  let passwordChanged = $state(false);
  let passwordBusy = $state(false);

  async function readTokens() {
    try {
      tokens = await api.tokens();
      tokenFailure = null;
    } catch (error) {
      tokenFailure = detail(error);
    }
  }

  $effect(() => {
    readTokens();
  });

  async function createToken(event: SubmitEvent) {
    event.preventDefault();
    tokenBusy = true;
    tokenFailure = null;
    minted = null;
    try {
      // An empty expiry is no expiry, which the API models as null.
      const expires = tokenExpires === "" ? null : new Date(tokenExpires).toISOString();
      minted = await api.createToken(tokenName.trim(), tokenActor.trim(), expires);
      tokenName = "";
      tokenExpires = "";
      await readTokens();
    } catch (error) {
      tokenFailure = detail(error);
    } finally {
      tokenBusy = false;
    }
  }

  async function revoke(id: string) {
    tokenBusy = true;
    tokenFailure = null;
    try {
      await api.revokeToken(id);
      await readTokens();
    } catch (error) {
      tokenFailure = detail(error);
    } finally {
      tokenBusy = false;
    }
  }

  async function createAccount(event: SubmitEvent) {
    event.preventDefault();
    accountBusy = true;
    accountFailure = null;
    created = "";
    try {
      const user = await api.createUser(newUsername.trim().toLowerCase(), newPassword);
      created = user.id;
      newUsername = "";
      newPassword = "";
    } catch (error) {
      accountFailure = detail(error);
    } finally {
      accountBusy = false;
    }
  }

  async function disable(event: SubmitEvent) {
    event.preventDefault();
    accountBusy = true;
    accountFailure = null;
    disabled = "";
    try {
      await api.disableUser(disableID.trim());
      disabled = disableID.trim();
      disableID = "";
    } catch (error) {
      accountFailure = detail(error);
    } finally {
      accountBusy = false;
    }
  }

  async function changePassword(event: SubmitEvent) {
    event.preventDefault();
    passwordBusy = true;
    passwordFailure = null;
    passwordChanged = false;
    try {
      await api.setPassword(session.user_id, currentPassword, nextPassword);
      passwordChanged = true;
    } catch (error) {
      passwordFailure = detail(error);
    } finally {
      // The two passwords leave the page whatever the answer was. A refused
      // attempt must not leave a password sitting in a field.
      currentPassword = "";
      nextPassword = "";
      passwordBusy = false;
    }
  }
</script>

<div class="page">
  <header class="page-head tabbed"><h1>Settings</h1></header>
  <div class="tabs" role="tablist" aria-label="Settings" tabindex="-1" onkeydown={key}>
    {#each tabs as t (t.id)}
      <button type="button" role="tab" id={`tab-${t.id}`} aria-selected={current === t.id} aria-controls="settings-panel" tabindex={current === t.id ? 0 : -1} onclick={() => choose(t.id)}>{t.label}</button>
    {/each}
  </div>
  <div class="panel" role="tabpanel" id="settings-panel" aria-labelledby={`tab-${current}`} tabindex="0">
    {#if current === "general"}
      {#if installationFailure}<ErrorPanel view={errorView(installationFailure)} />{/if}
      <dl class="facts">
        <dt>Installation</dt><dd class="mono">{installation?.installation_id ?? ""}</dd>
        <dt>Version</dt><dd class="mono">{installation?.version ?? ""}</dd>
        <dt>Controller address</dt><dd class="mono">{installation?.join_address ?? ""}</dd>
        <dt>API CA fingerprint</dt><dd class="mono">{installation?.api_ca_fingerprint ?? ""}</dd>
        <dt>Theme</dt><dd><span class="segmented" role="group" aria-label="Theme"><button type="button" class="small" aria-pressed={theme === "dark"} onclick={() => theme !== "dark" && onTheme()}>Dark</button><button type="button" class="small" aria-pressed={theme === "light"} onclick={() => theme !== "light" && onTheme()}>Light</button></span></dd>
      </dl>
    {:else if current === "notifications"}
      <Notifications />
    {:else if current === "accounts"}
      <section class="section" aria-labelledby="you">
        <h2 id="you">Your account</h2>
        <dl class="facts">
          <dt>User</dt><dd class="mono">{session.actor_id}</dd>
          <dt>Role</dt><dd>{session.role}</dd>
          <dt>ID</dt><dd class="mono">{session.user_id}</dd>
        </dl>
        <form onsubmit={changePassword}>
          <label for="current-password">Current password</label>
          <input id="current-password" type="password" bind:value={currentPassword} autocomplete="current-password" required />
          <label for="next-password">New password</label>
          <input id="next-password" type="password" bind:value={nextPassword} autocomplete="new-password" required />
          <p class="hint">Ends every other session of this account.</p>
          {#if passwordFailure}<ErrorPanel view={errorView(passwordFailure)} />{/if}
          {#if passwordChanged}<p class="done" role="status">Password changed. Other sessions ended.</p>{/if}
          <button type="submit" disabled={passwordBusy}>{passwordBusy ? "Saving" : "Change password"}</button>
        </form>
      </section>

      <section class="section" aria-labelledby="operators">
        <h2 id="operators">Operators</h2>
        {#if accountFailure}<ErrorPanel view={errorView(accountFailure)} />{/if}
        <form onsubmit={createAccount}>
          <label for="new-username">Username</label>
          <input id="new-username" class="mono" bind:value={newUsername} placeholder="ops-day" required />
          <label for="new-account-password">Password</label>
          <input id="new-account-password" type="password" bind:value={newPassword} autocomplete="new-password" required />
          <p class="hint">1 to 64 of a-z, 0-9, hyphen, underscore, dot.</p>
          {#if created}<p class="done" role="status">Created. User ID <span class="mono">{created}</span></p>{/if}
          <button type="submit" disabled={accountBusy}>{accountBusy ? "Saving" : "Create operator"}</button>
        </form>
        <form onsubmit={disable}>
          <label for="disable-id">Disable user ID</label>
          <input id="disable-id" class="mono" bind:value={disableID} placeholder="user ID" required />
          <p class="hint">The user cannot sign in. Their sessions end.</p>
          {#if disabled}<p class="done" role="status">Disabled <span class="mono">{disabled}</span></p>{/if}
          <button type="submit" class="quiet" disabled={accountBusy}>{accountBusy ? "Saving" : "Disable"}</button>
        </form>
      </section>

      <section class="section" aria-labelledby="tokens">
        <h2 id="tokens">API tokens</h2>
        {#if tokenFailure}<ErrorPanel view={errorView(tokenFailure)} />{/if}
        {#if minted}
          <!-- The plaintext appears once. The server keeps a verifier only. -->
          <div class="callout once" role="status">
            <p>Shown once. Copy it now.</p>
            <p class="mono plaintext">{minted.token}</p>
          </div>
        {/if}
        <form onsubmit={createToken}>
          <label for="token-name">Name</label>
          <input id="token-name" class="mono" bind:value={tokenName} placeholder="ci-deploy" required />
          <label for="token-actor">Actor</label>
          <input id="token-actor" class="mono" bind:value={tokenActor} placeholder={session.actor_id} />
          <label for="token-expires">Expires</label>
          <input id="token-expires" type="datetime-local" bind:value={tokenExpires} />
          <p class="hint">Empty actor: you. Empty expiry: never; revoke to end it.</p>
          <button type="submit" disabled={tokenBusy}>{tokenBusy ? "Saving" : "Create token"}</button>
        </form>
        <div class="scroll">
          <table class="nowrap">
            <thead><tr><th>Name</th><th>Actor</th><th>Created</th><th>Last used</th><th>Expires</th><th>State</th><th><span class="sr">Actions</span></th></tr></thead>
            <tbody>
              {#each tokens as token (token.id)}
                <tr>
                  <td class="mono">{token.name}</td>
                  <td class="mono">{token.actor_id}</td>
                  <td class="mono">{when(token.created_at, now)}</td>
                  <td class="mono">{when(token.last_used_at, now)}</td>
                  <td class="mono">{when(token.expires_at, now)}</td>
                  <td>{token.revoked_at === null ? "live" : "revoked"}</td>
                  <td class="end">{#if token.revoked_at === null}<button type="button" class="small quiet" onclick={() => revoke(token.id)} disabled={tokenBusy}>Revoke</button>{/if}</td>
                </tr>
              {:else}
                <tr><td colspan="7" class="dim">No tokens.</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>
    {:else}
      <div class="kit">
        <p>The recovery kit holds the repository password. With the kit and one surviving storage, <code>synod server init --from-repository</code> rebuilds the controller, and every app can be restored again.</p>
        <dl class="facts">
          <dt>Shown</dt><dd>once, when the first account was created</dd>
          <dt>Copy on the controller</dt><dd class="mono">/var/lib/synod/controller/recovery-kit.txt</dd>
          <dt>Keep it</dt><dd>off every Synod device, for example in a password manager</dd>
        </dl>
        <p class="hint">Delete the copy on the controller after you store the kit elsewhere.</p>
      </div>
    {/if}
  </div>
</div>

<style>
  .tabs {
    display: flex;
    gap: 0;
    border-bottom: 1px solid var(--line);
    margin-top: -12px;
    overflow-x: auto;
  }
  .tabs button {
    border: 0;
    border-bottom: 2px solid transparent;
    background: none;
    color: var(--ink-muted);
    padding: 0 16px;
    min-height: 40px;
    font-size: 14px;
    font-weight: 400;
  }
  .tabs button:hover {
    color: var(--ink);
    background: none;
  }
  .tabs button[aria-selected="true"] {
    color: var(--ink);
    border-bottom-color: var(--ink);
    font-weight: 600;
  }
  .panel {
    display: flex;
    flex-direction: column;
    gap: 36px;
    max-width: 880px;
  }
  .panel:focus {
    outline: none;
  }
  .panel:focus-visible {
    outline: 2px solid var(--active);
  }
  .facts {
    font-size: 14px;
  }
  form {
    display: grid;
    grid-template-columns: 180px minmax(0, 340px);
    gap: 8px 16px;
    align-items: center;
    margin: 0;
  }
  form label {
    font-size: 13px;
    color: var(--ink-muted);
  }
  form .hint,
  form .done,
  form button,
  form :global(.failure) {
    grid-column: 2;
  }
  form button {
    justify-self: start;
  }
  .done {
    font-size: 13px;
    color: var(--ok);
  }
  .once {
    border-color: var(--ink-dim);
  }
  .plaintext {
    overflow-wrap: anywhere;
    margin-top: 6px;
  }
  .segmented {
    display: inline-flex;
  }
  .segmented button + button {
    border-left: 0;
  }
  .segmented button[aria-pressed="true"] {
    background: var(--head);
    border-color: var(--ink-dim);
    color: var(--ink);
  }
  .segmented button[aria-pressed="false"] {
    color: var(--ink-muted);
  }
  .kit {
    display: flex;
    flex-direction: column;
    gap: 16px;
    max-width: 72ch;
  }
  @media (max-width: 600px) {
    form {
      grid-template-columns: 1fr;
    }
    form .hint,
    form .done,
    form button,
    form :global(.failure) {
      grid-column: 1;
    }
  }
</style>
