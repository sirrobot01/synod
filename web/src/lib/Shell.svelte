<script lang="ts">
  import { onDestroy } from "svelte";
  import { follow, Router, path, type ScreenName } from "./router.svelte";
  import type { Session } from "./api";
  import Overview from "../routes/Overview.svelte";
  import Apps from "../routes/Apps.svelte";
  import AppPage from "../routes/AppPage.svelte";
  import Devices from "../routes/Devices.svelte";
  import Storage from "../routes/Storage.svelte";
  import Activity from "../routes/Activity.svelte";
  import Operation from "../routes/Operation.svelte";
  import Settings from "../routes/Settings.svelte";

  let { session, busy, theme, onTheme, onSignOut }: { session: Session; busy: boolean; theme: "dark" | "light"; onTheme: () => void; onSignOut: () => void } = $props();

  const router = new Router();
  onDestroy(() => router.destroy());

  const screens: { screen: ScreenName; label: string }[] = [
    { screen: "overview", label: "Overview" },
    { screen: "apps", label: "Apps" },
    { screen: "devices", label: "Devices" },
    { screen: "storage", label: "Storage" },
    { screen: "activity", label: "Activity" },
    { screen: "settings", label: "Settings" },
  ];

  // A section stays marked while a screen below it is open, so a reader on one
  // operation still sees which part of the product they are in.
  let route = $derived(router.current);
  let section = $derived(route.screen === "operation" ? "activity" : route.screen);

  // A tab of Settings is part of its screen, so a tab change keeps the screen.
  let key = $derived(route.screen === "settings" ? "settings" : `${route.screen}/${route.id}`);
  let main = $state<HTMLElement | null>(null);
  let first = true;
  // A new screen moves focus to its content, so a keyboard reader starts there.
  $effect(() => {
    void key;
    if (first) { first = false; return; }
    main?.focus();
    window.scrollTo?.(0, 0);
  });

  function navigate(event: MouseEvent, screen: ScreenName, id = "") {
    follow(event, () => router.go(screen, id));
  }
</script>

<div class="shell">
  <div class="rail">
  <nav aria-label="Screens">
    <a class="brand mono" href={path("overview")} onclick={(event) => navigate(event, "overview")}>synod</a>
    <ul>
      {#each screens as item (item.screen)}
        <li>
          <a
            href={path(item.screen)}
            aria-current={section === item.screen ? "page" : undefined}
            onclick={(event) => navigate(event, item.screen)}
          >{item.label}</a>
        </li>
      {/each}
    </ul>
    <div class="foot">
      <span class="who"><span class="mono">{session.actor_id}</span>{#if session.role !== session.actor_id} <span class="role">({session.role})</span>{/if}</span>
      <span class="tools">
        <button type="button" class="small" onclick={onTheme} aria-label={`Switch to the ${theme === "dark" ? "light" : "dark"} theme`}>{theme === "dark" ? "Light" : "Dark"}</button>
        <button type="button" class="small" onclick={onSignOut} disabled={busy}>Sign out</button>
      </span>
    </div>
  </nav>
  </div>

  <main id="main" tabindex="-1" bind:this={main}>
    {#key key}
      {#if route.screen === "overview"}
        <Overview />
      {:else if route.screen === "apps" && route.id === ""}
        <Apps />
      {:else if route.screen === "apps"}
        <AppPage id={route.id} />
      {:else if route.screen === "devices"}
        <Devices id={route.id} />
      {:else if route.screen === "storage"}
        <Storage />
      {:else if route.screen === "activity"}
        <Activity />
      {:else if route.screen === "operation"}
        <Operation id={route.id} />
      {:else if route.screen === "settings"}
        <Settings {session} tab={route.id || "general"} {theme} {onTheme} />
      {:else}
        <div class="page">
          <h1>Not found</h1>
          <p class="note">No screen has this address. <a href={path("overview")} onclick={(event) => navigate(event, "overview")}>Open the overview</a></p>
        </div>
      {/if}
    {/key}
  </main>
</div>

<style>
  .shell {
    display: grid;
    grid-template-columns: 218px minmax(0, 1fr);
    min-height: 100vh;
  }
  /* The rail paints the full height of the page. The nav inside it stays in view. */
  .rail {
    border-right: 1px solid var(--line);
    background: var(--nav);
  }
  nav[aria-label="Screens"] {
    position: sticky;
    top: 0;
    height: 100vh;
    display: flex;
    flex-direction: column;
    padding: 18px 0 16px;
    overflow-y: auto;
  }
  .brand {
    padding: 0 16px 20px;
    font-size: 15px;
    font-weight: 600;
    letter-spacing: 0.02em;
    color: var(--ink);
    text-decoration: none;
  }
  ul {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
  }
  ul a {
    display: block;
    padding: 8px 16px 8px 14px;
    border-left: 2px solid transparent;
    color: var(--ink-muted);
    text-decoration: none;
    font-size: 14px;
  }
  ul a:hover {
    color: var(--ink);
    background: var(--head);
  }
  /* The open screen has a bar and a ground as well as a colour. */
  ul a[aria-current="page"] {
    color: var(--ink);
    background: var(--head);
    border-left-color: var(--ink);
  }
  .foot {
    margin-top: auto;
    padding: 14px 16px 0;
    border-top: 1px solid var(--line);
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .who {
    font-size: 13px;
    overflow-wrap: anywhere;
  }
  .role {
    color: var(--ink-muted);
  }
  .tools {
    display: flex;
    gap: 6px;
  }
  main {
    min-width: 0;
    padding: 32px 40px 72px;
  }
  main:focus {
    outline: none;
  }
  @media (max-width: 860px) {
    .shell {
      grid-template-columns: minmax(0, 1fr);
    }
    .rail {
      border-right: 0;
      border-bottom: 1px solid var(--line);
    }
    nav[aria-label="Screens"] {
      position: static;
      height: auto;
      padding: 12px 0 0;
      display: grid;
      grid-template-columns: 1fr auto;
      align-items: center;
    }
    .brand {
      padding: 0 16px;
    }
    ul {
      grid-column: 1 / -1;
      flex-direction: row;
      overflow-x: auto;
      margin-top: 10px;
      border-top: 1px solid var(--line);
    }
    ul a {
      white-space: nowrap;
      padding: 10px 12px;
      border-left: 0;
      border-bottom: 2px solid transparent;
    }
    ul a[aria-current="page"] {
      border-bottom-color: var(--ink);
    }
    .foot {
      grid-row: 1;
      grid-column: 2;
      margin: 0;
      padding: 0 16px;
      border: 0;
    }
    .who {
      display: none;
    }
    main {
      padding: 24px 16px 48px;
    }
  }
</style>
