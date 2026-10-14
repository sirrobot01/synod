<script lang="ts">
  import { api, Failure, type Session } from "./lib/api";
  import { errorView, type APIError } from "./lib/state";
  import ErrorPanel from "./lib/ErrorPanel.svelte";
  import Shell from "./lib/Shell.svelte";
  import Setup from "./lib/Setup.svelte";

  type Phase = "checking" | "setup" | "signed-out" | "signed-in";

  let phase = $state<Phase>("checking");
  let session = $state<Session | null>(null);
  let username = $state("");
  let password = $state("");
  let busy = $state(false);
  // The failure stays anchored to the form that caused it. Conflicts are not toasts.
  let failure = $state<APIError | null>(null);
  // The theme is a convenience of this browser. Storage can be absent, so a failure keeps the default.
  function savedTheme(): "dark" | "light" {
    try {
      return localStorage.getItem("synod-theme") === "light" ? "light" : "dark";
    } catch {
      return "dark";
    }
  }
  let theme = $state<"dark" | "light">(savedTheme());

  $effect(() => {
    document.documentElement.dataset["theme"] = theme;
    try {
      localStorage.setItem("synod-theme", theme);
    } catch {
      // The choice lasts until the page closes.
    }
  });

  function toggleTheme() {
    theme = theme === "dark" ? "light" : "dark";
  }

  async function resume() {
    try {
      session = await api.currentSession();
      phase = "signed-in";
    } catch {
      // No session is the ordinary first visit, not an error worth showing.
      // Before any account exists, the first visit sets up the installation.
      try {
        phase = (await api.setupStatus()).required ? "setup" : "signed-out";
      } catch {
        phase = "signed-out";
      }
    }
  }

  resume();

  async function submit(event: SubmitEvent) {
    event.preventDefault();
    busy = true;
    failure = null;
    try {
      const user = await api.signIn(username.trim().toLowerCase(), password);
      password = "";
      // The cookie is set, so the operator is signed in. A failed read of the
      // session must not send them back to a form with an emptied password.
      session = { actor_id: user.username, role: user.role, user_id: user.id };
      phase = "signed-in";
    } catch (error) {
      failure = error instanceof Failure ? error.detail : { code: "transport_error", message: "The API could not be reached.", requestID: "" };
    } finally {
      busy = false;
    }
  }

  async function signOut() {
    busy = true;
    failure = null;
    try {
      await api.signOut();
    } catch (error) {
      // Only a refused credential proves the session ended. Any other failure
      // leaves the session alive on the server, so showing the form would lie.
      const refused = error instanceof Failure && (error.status === 401 || error.status === 403);
      if (!refused) {
        failure = error instanceof Failure ? error.detail : { code: "transport_error", message: "The API could not be reached.", requestID: "" };
        busy = false;
        return;
      }
    }
    busy = false;
    session = null;
    phase = "signed-out";
  }
</script>

<a class="skip" href="#main">Skip to content</a>

{#if phase === "signed-in" && session}
  <Shell {session} {busy} {theme} onTheme={toggleTheme} onSignOut={signOut} />
  {#if failure}
    <div class="anchored"><ErrorPanel view={errorView(failure)} /></div>
  {/if}
{:else}
  <button
    class="theme ghost small"
    onclick={toggleTheme}
    aria-label={`Switch to the ${theme === "dark" ? "light" : "dark"} theme`}
  >
    {theme === "dark" ? "Light theme" : "Dark theme"}
  </button>
  <main id="main" tabindex="-1">
    <div class="gate">
      <div class="brand">
        <span class="mono name">synod</span>
        <span class="tagline">Backups for Docker apps</span>
      </div>
      {#if phase === "checking"}
        <p class="loading">Checking the session</p>
      {:else if phase === "setup"}
        <Setup
          onDone={(created) => {
            session = created;
            phase = "signed-in";
          }}
          onSignIn={() => (phase = "signed-out")}
        />
      {:else}
        <form onsubmit={submit} aria-labelledby="sign-in-heading">
          <h1 id="sign-in-heading">Sign in</h1>
          <label class="field">Username
            <input id="username" name="username" bind:value={username} autocomplete="username" required />
          </label>
          <label class="field">Password
            <input id="password" name="password" type="password" bind:value={password} autocomplete="current-password" required />
          </label>
          {#if failure}
            <ErrorPanel view={errorView(failure)} />
          {/if}
          <button type="submit" class="primary" disabled={busy}>{busy ? "Signing in" : "Sign in"}</button>
        </form>
      {/if}
    </div>
  </main>
{/if}

<style>
  .skip {
    position: absolute;
    left: -9999px;
  }
  .skip:focus {
    left: 8px;
    top: 8px;
    padding: 8px;
    background: var(--panel);
    border: 1px solid var(--line);
    z-index: 50;
  }
  .theme {
    position: fixed;
    right: 12px;
    top: 10px;
    z-index: 1;
  }
  /* A failure of the shell itself stays anchored to the shell, not to a screen. */
  .anchored {
    position: fixed;
    right: 12px;
    bottom: 12px;
    max-width: 420px;
    z-index: 2;
  }
  main {
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px 16px;
  }
  main:focus {
    outline: none;
  }
  .gate {
    width: min(420px, 100%);
    border: 1px solid var(--line);
    background: var(--panel);
    padding: 32px 28px;
    display: flex;
    flex-direction: column;
    gap: 24px;
  }
  .brand {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .brand .name {
    font-size: 18px;
    font-weight: 600;
    color: var(--ink);
  }
  .brand .tagline {
    font-size: 13px;
    color: var(--ink-muted);
  }
  h1 {
    font-size: 20px;
  }
  form {
    display: flex;
    flex-direction: column;
    gap: 14px;
  }
  form button {
    align-self: flex-start;
  }
</style>
