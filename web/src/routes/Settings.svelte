<script lang="ts">
  import { api, detail, type APIToken, type MintedToken, type Session } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Panel from "../lib/ui/Panel.svelte";

  let { session }: { session: Session } = $props();

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

<div class="screen">
  <header>
    <h1>Settings</h1>
    <p class="note">Accounts and API access.</p>
  </header>

  <Panel title="Your account" note="this writes to the API">
    <div class="pad">
      <dl class="facts mono wide">
        <dt>signed_in_as</dt><dd>{session.actor_id}</dd>
        <dt>role</dt><dd>{session.role}</dd>
        <dt>user_id</dt><dd>{session.user_id}</dd>
      </dl>
      <form onsubmit={changePassword}>
        <label for="current-password" class="label">Current password</label>
        <input id="current-password" type="password" bind:value={currentPassword} autocomplete="current-password" required />
        <label for="next-password" class="label">New password</label>
        <input id="next-password" type="password" bind:value={nextPassword} autocomplete="new-password" required />
        <p class="hint">A change ends every other session of this account.</p>
        {#if passwordFailure}<ErrorPanel view={errorView(passwordFailure)} />{/if}
        {#if passwordChanged}
          <p class="done" role="status">The password changed. Other sessions of this account have ended.</p>
        {/if}
        <button type="submit" class="primary" disabled={passwordBusy}>{passwordBusy ? "Sending" : "Change password"}</button>
      </form>
    </div>
  </Panel>

  <Panel title="API tokens" note="this reads and writes the API">
    <div class="pad">
      {#if tokenFailure}<ErrorPanel view={errorView(tokenFailure)} />{/if}
      {#if minted}
        <!-- The plaintext appears once. The server keeps a verifier only. -->
        <div class="once" role="status">
          <p>This plaintext appears once. The server keeps a verifier and cannot show it again.</p>
          <p class="mono plaintext">{minted.token}</p>
        </div>
      {/if}
      <form onsubmit={createToken}>
        <label for="token-name" class="label">Name</label>
        <input id="token-name" bind:value={tokenName} placeholder="ci-deploy" required />
        <label for="token-actor" class="label">Actor</label>
        <input id="token-actor" bind:value={tokenActor} placeholder={session.actor_id} />
        <p class="hint">An empty actor records the signed-in operator.</p>
        <label for="token-expires" class="label">Expires</label>
        <input id="token-expires" type="datetime-local" bind:value={tokenExpires} />
        <p class="hint">An empty expiry never expires. Revocation is then the only way to end it.</p>
        <button type="submit" class="primary" disabled={tokenBusy}>{tokenBusy ? "Sending" : "Create token"}</button>
      </form>
    </div>

    <div class="scroll">
      <div class="grid tokens">
        <div class="head">Name</div>
        <div class="head">Actor</div>
        <div class="head">Created</div>
        <div class="head">Last used</div>
        <div class="head">Expires</div>
        <div class="head">State</div>
        <div class="head"></div>
        {#each tokens as token (token.id)}
          <div class="cell">{token.name}</div>
          <div class="cell">{token.actor_id}</div>
          <div class="cell dim">{token.created_at}</div>
          <div class="cell dim">{token.last_used_at ?? "never"}</div>
          <div class="cell dim">{token.expires_at ?? "never"}</div>
          <div class="cell">{token.revoked_at === null ? "live" : "revoked"}</div>
          <div class="cell">
            {#if token.revoked_at === null}
              <button type="button" onclick={() => revoke(token.id)} disabled={tokenBusy}>Revoke</button>
            {/if}
          </div>
        {:else}
          <div class="cell empty">This installation holds no API token.</div>
        {/each}
      </div>
    </div>
  </Panel>

  <Panel title="Operator accounts" note="creating and disabling write to the API">
    <div class="pad">
      {#if accountFailure}<ErrorPanel view={errorView(accountFailure)} />{/if}
      <form onsubmit={createAccount}>
        <label for="new-username" class="label">Username</label>
        <input id="new-username" bind:value={newUsername} placeholder="ops-day" required />
        <label for="new-account-password" class="label">Password</label>
        <input id="new-account-password" type="password" bind:value={newPassword} autocomplete="new-password" required />
        <p class="hint">1 to 64 lower case letters, digits, hyphen, underscore, or dot.</p>
        {#if created}
          <p class="done" role="status">The account exists. Its user ID is <span class="mono">{created}</span>.</p>
        {/if}
        <button type="submit" class="primary" disabled={accountBusy}>{accountBusy ? "Sending" : "Create account"}</button>
      </form>

      <form onsubmit={disable}>
        <label for="disable-id" class="label">Disable user</label>
        <input id="disable-id" bind:value={disableID} placeholder="user ID" required />
        <p class="hint">A disabled account cannot sign in. Its sessions end.</p>
        {#if disabled}
          <p class="done" role="status">The account <span class="mono">{disabled}</span> is disabled.</p>
        {/if}
        <button type="submit" disabled={accountBusy}>{accountBusy ? "Sending" : "Disable"}</button>
      </form>

    </div>

  </Panel>
</div>

<style>
  .screen {
    display: flex;
    flex-direction: column;
    gap: 22px;
    max-width: 1240px;
  }
  header {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  header .note {
    font-size: 13px;
  }
  .pad {
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 16px;
  }

  .facts {
    display: grid;
    grid-template-columns: auto 1fr;
    row-gap: 3px;
    column-gap: 12px;
    margin: 7px 0 0;
    font-size: 10px;
  }
  .facts.wide {
    font-size: 11px;
    row-gap: 5px;
    margin: 0;
  }
  .facts dt {
    color: var(--ink-faint);
  }
  .facts dd {
    margin: 0;
    color: var(--ink-body);
    overflow-wrap: anywhere;
  }
  form {
    display: grid;
    grid-template-columns: 150px minmax(0, 300px);
    gap: 8px 12px;
    align-items: center;
    margin: 0;
  }
  form .hint,
  form .done,
  form button,
  form :global(.failure),
  form :global(.once) {
    grid-column: 1 / -1;
  }
  .hint {
    color: var(--ink-dim);
    font-size: 12px;
    margin: 0;
  }
  .done {
    margin: 0;
    font-size: 13px;
  }
  form button {
    justify-self: start;
  }
  .once {
    border: 1px solid var(--active);
    padding: 12px 14px;
  }
  .once p {
    margin: 0 0 6px;
  }
  .plaintext {
    overflow-wrap: anywhere;
    margin: 0;
  }

  .grid.tokens {
    grid-template-columns:
      minmax(120px, 1fr) minmax(110px, 0.8fr) minmax(170px, 1fr)
      minmax(170px, 1fr) minmax(170px, 1fr) minmax(80px, 0.5fr) minmax(90px, 0.5fr);
    min-width: 1020px;
  }
  .cell button {
    padding: 3px 9px;
  }
  .empty {
    grid-column: 1 / -1;
    color: var(--ink-dim);
  }
</style>
