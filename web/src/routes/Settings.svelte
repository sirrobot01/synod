<script lang="ts">
  import { onMount, tick } from "svelte";
  import { api, detail, type APIToken, type Installation, type Session, type User } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import { swap } from "../lib/router.svelte";
  import { when } from "../lib/model";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Notifications from "../lib/Notifications.svelte";
  import Icon from "../lib/ui/Icon.svelte";
  import IconButton from "../lib/ui/IconButton.svelte";
  import CreateOperator from "../lib/CreateOperator.svelte";
  import CreateToken from "../lib/CreateToken.svelte";
  import Dialog from "../lib/ui/Dialog.svelte";
  import { toast } from "../lib/toast.svelte";

  let { session, tab = "general" }: { session: Session; tab?: string } = $props();

  const tabs = [
    { id: "general", label: "General" },
    { id: "notifications", label: "Notifications" },
    { id: "accounts", label: "Accounts" },
    { id: "api-tokens", label: "API tokens" },
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

  let now = $state(Date.now());
  let installation = $state<Installation | null>(null);
  let installationFailure = $state<APIError | null>(null);
  onMount(() => {
    api.installation().then((value) => installation = value).catch((error) => installationFailure = detail(error));
    readTokens();
    readUsers();
    const timer = setInterval(() => now = Date.now(), 1000);
    return () => clearInterval(timer);
  });

  // Each region keeps its own failure, so a refused token never appears under
  // the password form that did not cause it.
  let tokens = $state<APIToken[] | null>(null);
  let tokenFailure = $state<APIError | null>(null);
  let tokenBusy = $state(false);
  let addingToken = $state(false);
  let users = $state<User[] | null>(null);
  let accountFailure = $state<APIError | null>(null);
  let accountBusy = $state(false);
  let addingOperator = $state(false);
  let disabling = $state<User | null>(null);
  let disableFailure = $state<APIError | null>(null);

  let passwordFailure = $state<APIError | null>(null);
  let currentPassword = $state("");
  let nextPassword = $state("");
  let passwordChanged = $state(false);
  let passwordBusy = $state(false);

  async function readTokens() {
    tokenBusy = true;
    try {
      tokens = await api.tokens();
      tokenFailure = null;
    } catch (error) {
      tokenFailure = detail(error);
    } finally {
      tokenBusy = false;
    }
  }

  async function readUsers() {
    accountBusy = true;
    try {
      users = await api.users();
      accountFailure = null;
    } catch (error) {
      accountFailure = detail(error);
    } finally {
      accountBusy = false;
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

  async function disable() {
    if (!disabling || accountBusy || disabling.id === session.user_id) return;
    accountBusy = true;
    disableFailure = null;
    const selected = disabling;
    try {
      await api.disableUser(selected.id);
      users = users?.map((user) => user.id === selected.id ? { ...user, disabled: true } : user) ?? null;
      disabling = null;
      toast.show(`${selected.username} disabled`);
    } catch (error) {
      disableFailure = detail(error);
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
      </dl>
    {:else if current === "notifications"}
      <Notifications />
    {:else if current === "accounts"}
      <section class="section" aria-labelledby="you">
        <h2 id="you">Your account</h2>
        <dl class="facts">
          <dt>User</dt><dd class="mono">{session.actor_id}</dd>
          <dt>Role</dt><dd>{session.role}</dd>
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
        <div class="section-head"><h2 id="operators">Operators</h2><div class="actions">
          <IconButton icon="refresh" label="Refresh operators" onclick={readUsers} disabled={accountBusy} />
          <button class="primary" disabled={accountBusy} onclick={() => addingOperator = true}><Icon name="plus" />Add operator</button>
        </div></div>
        {#if accountFailure}<ErrorPanel view={errorView(accountFailure)} />{/if}
        <div class="scroll">
          <table>
            <thead><tr><th>Username</th><th>Role</th><th>State</th><th class="end"><span class="sr">Actions</span></th></tr></thead>
            <tbody>
              {#each users ?? [] as user (user.id)}
                <tr>
                  <td>{user.username}{#if user.id === session.user_id}<span class="hint you">(you)</span>{/if}</td>
                  <td>{user.role}</td><td>{user.disabled ? "Disabled" : "Active"}</td>
                  <td class="end">{#if !user.disabled}<IconButton icon="disable" tone="danger" label={`Disable ${user.username}`} disabled={accountBusy || user.id === session.user_id} onclick={() => { disabling = user; disableFailure = null; }} />{/if}</td>
                </tr>
              {:else}
                <tr><td colspan="4" class="dim">{users ? "No operators." : accountFailure ? "Operators could not be loaded." : "Reading operators…"}</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>

    {:else if current === "api-tokens"}
      <section class="section" aria-labelledby="tokens">
        <div class="section-head"><h2 id="tokens">API tokens</h2><div class="actions">
          <IconButton icon="refresh" label="Refresh tokens" onclick={readTokens} disabled={tokenBusy} />
          <button class="primary" disabled={tokenBusy} onclick={() => addingToken = true}><Icon name="plus" />Create token</button>
        </div></div>
        {#if tokenFailure}<ErrorPanel view={errorView(tokenFailure)} />{/if}
        <div class="scroll">
          <table class="nowrap">
            <colgroup><col /><col class="c-device" /><col class="c-time" /><col class="c-time" /><col class="c-time" /><col class="c-kind" /><col class="c-act" /></colgroup>
            <thead><tr><th>Name</th><th>Actor</th><th>Created</th><th>Last used</th><th>Expires</th><th>State</th><th class="end"><span class="sr">Actions</span></th></tr></thead>
            <tbody>
              {#each tokens ?? [] as token (token.id)}
                <tr>
                  <td class="mono">{token.name}</td>
                  <td class="mono">{token.actor_id}</td>
                  <td class="mono">{when(token.created_at, now)}</td>
                  <td class="mono">{when(token.last_used_at, now)}</td>
                  <td class="mono">{token.expires_at ? new Date(token.expires_at).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" }) : "Never"}</td>
                  <td>{token.revoked_at !== null ? "Revoked" : token.expires_at && Date.parse(token.expires_at) <= now ? "Expired" : "Active"}</td>
                  <td class="end">{#if token.revoked_at === null}<IconButton icon="trash" tone="danger" label={`Revoke ${token.name}`} onclick={() => revoke(token.id)} disabled={tokenBusy} />{/if}</td>
                </tr>
              {:else}
                <tr><td colspan="7" class="dim">{tokens ? "No tokens." : tokenFailure ? "Tokens could not be loaded." : "Reading tokens…"}</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>
    {/if}
  </div>
</div>

{#if addingOperator}
  <CreateOperator onClose={() => addingOperator = false} onCreated={(user) => {
    users = [...(users ?? []), user].sort((a, b) => a.username.localeCompare(b.username));
    addingOperator = false;
    toast.show(`${user.username} added`);
    readUsers();
  }} />
{/if}
{#if addingToken}
  <CreateToken actor={session.actor_id} onClose={() => addingToken = false} onCreated={(token) => {
    const { token: secret, ...record } = token;
    tokens = [record, ...(tokens ?? [])];
    readTokens();
  }} />
{/if}
{#if disabling}
  <Dialog label={`Disable ${disabling.username}`} title={`Disable ${disabling.username}?`} width={480} danger onClose={() => { if (!accountBusy) disabling = null; }}>
    <p>This operator will no longer be able to sign in. Their sessions will end.</p>
    {#if disableFailure}<ErrorPanel view={errorView(disableFailure)} />{/if}
    {#snippet footer()}
      <button class="ghost" disabled={accountBusy} onclick={() => disabling = null}>Cancel</button>
      <button class="danger" disabled={accountBusy} onclick={disable}>{accountBusy ? "Disabling" : "Disable operator"}</button>
    {/snippet}
  </Dialog>
{/if}

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
  .you { margin-left: 0.5em; }
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
