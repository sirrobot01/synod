<script lang="ts">
  import { follow, Router, path, type ScreenName } from "./router.svelte";
  import type { Session } from "./api";
  import { components, gaps } from "./mock";
  import Mark from "./ui/Mark.svelte";
  import Operations from "../routes/Operations.svelte";
  import Operation from "../routes/Operation.svelte";
  import Settings from "../routes/Settings.svelte";
  import Fleet from "../routes/Fleet.svelte";
  import Nodes from "../routes/Nodes.svelte";
  import Backups from "../routes/Backups.svelte";

  let { session, busy, onSignOut }: { session: Session; busy: boolean; onSignOut: () => void } = $props();

  const router = new Router();

  // The navigation names the screens that specification 17 requires, in the order
  // an operator works through them: what is wrong, then where, then what to change.
  // The design puts a count beside each one. A count of apps or nodes would be
  // an observation the API does not serve, so none is drawn rather than invented.
  const destinations: { screen: ScreenName; label: string }[] = [
    { screen: "fleet", label: "Dashboard" },
    { screen: "nodes", label: "Nodes" },
    { screen: "backups", label: "Backups" },
    { screen: "operations", label: "Operations" },
    { screen: "settings", label: "Settings" },
  ];

  // A section stays marked while a screen below it is open, so an operator on an
  // operation still sees which part of the product they are in.
  const sections: Partial<Record<ScreenName, ScreenName>> = {
    operation: "operations",
  };

  let route = $derived(router.current);
  let section = $derived(sections[route.screen] ?? route.screen);
  let titles: Record<ScreenName, string> = {
    fleet: "Dashboard",
    nodes: "Nodes",
    backups: "Backups",
    operations: "Operations",
    operation: "Operation",
    settings: "Settings",
    unknown: "Not found",
  };

  // The trail names every step back to the dashboard, so a deep link still says
  // where the reader is. It is built from the route alone and reads nothing.
  let trail = $derived.by(() => {
    const steps: { label: string; screen: ScreenName; id: string }[] = [];
    switch (route.screen) {
      case "operation":
        steps.push({ label: "Operations", screen: "operations", id: "" });
        break;
      default:
        break;
    }
    return steps;
  });

  // Installation health belongs to the chrome, not to a screen, so it is drawn
  // here. The API serves none of it, which the tag beside it states.
  let installation = $derived(components.slice(0, 2));

  /** navigate keeps a link a link. Specification 17 requires an address that survives a reload. */
  function navigate(event: MouseEvent, screen: ScreenName, id = "") {
    follow(event, () => router.go(screen, id));
  }
</script>

<div class="shell">
  <nav aria-label="Screens">
    <div class="brand">
      <span class="mono name">synod</span>
      <span class="mono where">database control plane</span>
    </div>
    <ul>
      {#each destinations as destination (destination.screen)}
        <li>
          <a
            href={path(destination.screen)}
            aria-current={section === destination.screen ? "page" : undefined}
            onclick={(event) => navigate(event, destination.screen)}
          >{destination.label}</a>
        </li>
      {/each}
    </ul>
    <div class="foot">
      <div class="label">
        Installation health <span class="tag mono" title={gaps.health}>MOCK</span>
      </div>
      {#each installation as component (component.name)}
        <div class="health">
          <Mark mark={component.mark} text={false} />
          <span class="mono">{component.label}</span>
        </div>
      {/each}
      <p class="who mono">
        {session.actor_id}<br /><span class="dim">{session.role}</span>
      </p>
      <button onclick={onSignOut} disabled={busy}>Sign out</button>
    </div>
  </nav>

  <main id="main" tabindex="-1">
    <div class="bar">
      <nav class="trail" aria-label="Breadcrumb">
        <ol class="mono">
          <li><a href={path("fleet")} onclick={(event) => navigate(event, "fleet")}>Dashboard</a></li>
          {#each trail as step (step.screen + step.id)}
            <li>
              <a href={path(step.screen, step.id)} onclick={(event) => navigate(event, step.screen, step.id)}>{step.label}</a>
            </li>
          {/each}
          {#if route.screen !== "fleet"}
            <li aria-current="page">
              {route.id === "" ? titles[route.screen] : route.id}
            </li>
          {/if}
        </ol>
      </nav>
      <span class="cadence mono">an open operation is read every 5s</span>
    </div>

    <div class="screen">
      {#if route.screen === "fleet"}
        <Fleet go={(screen, id) => router.go(screen, id)} />
      {:else if route.screen === "nodes"}
        <Nodes id={route.id} onOpen={(id) => router.go("nodes", id)} />
      {:else if route.screen === "backups"}
        <Backups />
      {:else if route.screen === "operations"}
        <Operations onOpen={(id) => router.go("operation", id)} />
      {:else if route.screen === "operation"}
        <Operation id={route.id} />
      {:else if route.screen === "settings"}
        <Settings {session} />
      {:else}
        <h1>Not found</h1>
        <p class="note">This address does not name a screen.</p>
      {/if}
    </div>
  </main>
</div>

<style>
  .shell {
    display: grid;
    grid-template-columns: 218px 1fr;
    min-height: 100vh;
  }
  nav[aria-label="Screens"] {
    display: flex;
    flex-direction: column;
    gap: 2px;
    padding: 18px 0 24px;
    border-right: 1px solid var(--line);
    background: var(--nav);
  }
  .brand {
    padding: 0 16px 18px;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .brand .name {
    font-size: 14px;
    font-weight: 600;
    letter-spacing: 0.02em;
  }
  .brand .where {
    font-size: 10px;
    color: var(--ink-faint);
  }
  nav ul {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: 2px;
  }
  nav ul a {
    display: block;
    padding: 8px 16px;
    border-left: 2px solid transparent;
    color: var(--ink-muted);
    text-decoration: none;
    font-family: "IBM Plex Mono", ui-monospace, monospace;
    font-size: 12px;
  }
  nav ul a:hover {
    color: var(--ink);
  }
  /* The open screen carries a bar and a ground as well as a colour, so the mark
     is never colour alone. */
  nav ul a[aria-current="page"] {
    color: var(--ink);
    background: var(--head);
    border-left-color: var(--ink);
  }
  .foot {
    margin-top: auto;
    padding: 18px 16px 0;
    display: flex;
    flex-direction: column;
    gap: 8px;
    border-top: 1px solid var(--line);
  }
  .foot .label {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  .tag {
    font-size: 9px;
    letter-spacing: 0.12em;
    border: 1px dashed var(--line-strong);
    padding: 1px 4px;
    color: var(--ink-dim);
  }
  .health {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 11px;
  }
  .who {
    font-size: 10px;
    margin: 4px 0 0;
    line-height: 1.5;
  }
  .dim {
    color: var(--ink-faint);
  }
  .foot button {
    align-self: flex-start;
  }

  main {
    min-width: 0;
    display: flex;
    flex-direction: column;
  }
  main:focus {
    outline: none;
  }
  .bar {
    border-bottom: 1px solid var(--line);
    background: var(--nav);
    /* The theme control is fixed to the top right corner, so the bar leaves it room. */
    padding: 14px 92px 14px 24px;
    display: flex;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
  }
  .trail ol {
    list-style: none;
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin: 0;
    padding: 0;
    font-size: 12px;
    color: var(--ink-muted);
  }
  .trail li + li::before {
    content: "/";
    margin-right: 8px;
    color: var(--line-strong);
  }
  .trail a {
    color: var(--ink-muted);
    text-decoration: none;
  }
  .trail a:hover {
    color: var(--ink);
  }
  .cadence {
    margin-left: auto;
    font-size: 11px;
    color: var(--ink-faint);
  }
  .screen {
    flex: 1;
    min-width: 0;
    padding: 26px 24px 80px;
  }
</style>
