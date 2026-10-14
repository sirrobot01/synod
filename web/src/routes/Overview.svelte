<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type DestinationList, type Node, type Operation, type ProtectionPlan } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import { bytes } from "../lib/format";
  import { activity, cell, deviceState, nodeName, problemMark, problems, setup, short, summary, tested, took, when, type Step } from "../lib/model";
  import Link from "../lib/ui/Link.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import AddDevice from "../lib/AddDevice.svelte";
  import StorageDialog from "../lib/StorageDialog.svelte";
  import ScheduleBackups from "../lib/ScheduleBackups.svelte";

  let plan = $state<ProtectionPlan | null>(null);
  let listing = $state<DestinationList | null>(null);
  let nodes = $state<Node[]>([]);
  let operations = $state<Operation[]>([]);
  let loaded = $state(false);
  let failure = $state<APIError | null>(null);
  let busy = $state(false);
  let now = $state(Date.now());
  let modal = $state<"" | "device" | "storage" | "schedule">("");

  async function read() {
    if (busy) return;
    busy = true;
    const [p, d, n, o] = await Promise.allSettled([api.protection(), api.destinations(), api.nodes(), api.operations()]);
    if (p.status === "fulfilled") plan = p.value;
    if (d.status === "fulfilled") listing = d.value;
    if (n.status === "fulfilled") nodes = n.value;
    if (o.status === "fulfilled") operations = o.value;
    const first = [p, d, n].find((r) => r.status === "rejected");
    failure = first && first.status === "rejected" ? detail(first.reason) : null;
    loaded = loaded || !failure;
    now = Date.now();
    busy = false;
  }

  onMount(() => {
    read();
    const timer = setInterval(() => { if (!modal) read(); }, 15000);
    return () => clearInterval(timer);
  });

  let destinations = $derived(listing?.destinations ?? []);
  let steps = $derived(setup(nodes, destinations, plan));
  let issues = $derived(problems(plan, nodes, listing, now));
  let head = $derived(summary(plan, now));
  // Apps grouped under their device, in the order of the device list. A device without apps has no rows.
  let groups = $derived(nodes.map((node) => ({ node, state: deviceState(node, now), apps: (plan?.apps ?? []).filter((a) => a.app.node_id === node.id && a.app.present) })).filter((g) => g.apps.length));
  let names = $derived({
    app: (id: string) => plan?.apps.find((a) => a.app.id === id)?.app.definition.name ?? "app",
    node: (id: string) => nodeName(nodes, id),
    storage: (id?: string) => destinations.find((d) => d.id === id)?.name ?? "",
  });
  const actions: Record<Step["key"], { label: string; modal: "device" | "storage" | "schedule" }> = {
    devices: { label: "Add device", modal: "device" },
    storage: { label: "Add storage", modal: "storage" },
    schedule: { label: "Schedule backups", modal: "schedule" },
  };
</script>

<div class="page">
  <header class="page-head">
    <div class="stack">
      <h1>Overview</h1>
      {#if loaded && head.total}
        <dl class="line">
          <div><dt>Restorable</dt><dd class="mono">{head.restorable}/{head.total}</dd></div>
          <div><dt>Newest snapshot</dt><dd class="mono">{head.newest ? short(head.newest, now) : "none"}</dd></div>
          <div><dt>Next run</dt><dd class="mono">{head.next ? when(head.next, now) : "none"}</dd></div>
        </dl>
      {/if}
    </div>
    <div class="actions">
      <button type="button" onclick={() => (modal = "device")}>Add device</button>
      <button type="button" onclick={() => (modal = "storage")} disabled={!listing}>Add storage</button>
      <button type="button" class="primary" onclick={() => (modal = "schedule")} disabled={!plan || !destinations.length}>Schedule backups</button>
    </div>
  </header>

  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if !loaded && !failure}<p class="loading" role="status">Loading</p>{/if}

  {#if loaded && issues.length}
    <section class="section" aria-labelledby="attention">
      <h2 id="attention">Attention</h2>
      <ul class="issues">
        {#each issues as issue (issue.key)}
          <li>
            <span class="mark"><Mark mark={problemMark(issue.tone)} shapeOnly /></span>
            <span class="subject">{issue.subject}</span>
            <span class={issue.code ? "code condition" : "condition"}>{issue.condition}</span>
            <span class="detail">{issue.detail}</span>
            <span class="go">{#if issue.link}<Link screen={issue.link.screen} id={issue.link.id}>{issue.link.label}</Link>{/if}</span>
          </li>
        {/each}
      </ul>
    </section>
  {/if}

  {#if loaded && !steps.complete}
    <section class="section" aria-labelledby="setup">
      <div class="section-head"><h2 id="setup">Setup</h2><span class="note">{steps.done} of 3 done</span></div>
      <div class="scroll">
        <table>
          <tbody>
            {#each steps.steps as step (step.key)}
              <tr class:next={steps.next === step.key}>
                <td class="mark"><Mark mark={step.done ? { glyph: "✓", shape: "circle", tone: "ok", label: "Done" } : { glyph: "–", shape: "circle-open", tone: steps.next === step.key ? "active" : "neutral", label: "To do" }} shapeOnly /></td>
                <td class="subject">{step.title}</td>
                <td class="detail">{step.text}</td>
                <td class="end">{#if !step.done}<button type="button" class={steps.next === step.key ? "primary small" : "small"} onclick={() => (modal = actions[step.key].modal)} disabled={step.key === "schedule" && !destinations.length}>{actions[step.key].label}</button>{/if}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </section>
  {/if}

  {#if loaded && groups.length}
    <section class="section" aria-labelledby="matrix">
      <div class="section-head">
        <h2 id="matrix">Recovery matrix</h2>
        <span class="note">Newest snapshot of each app in each storage</span>
      </div>
      <div class="scroll">
        <table class="matrix">
          <thead>
            <tr>
              <th scope="col">App</th>
              {#each destinations as destination (destination.id)}<th scope="col" class="target">{destination.name}</th>{/each}
              <th scope="col">Restore test</th>
            </tr>
          </thead>
          {#each groups as group (group.node.id)}
            <tbody>
              <tr class="group">
                <th scope="rowgroup" colspan={destinations.length + 2}>
                  <Link screen="devices" id={group.node.id}>{group.node.name || group.node.id}</Link>
                  {#if group.state.label !== "Online"}<span class="state"><Mark mark={group.state.mark} shapeOnly />{group.state.label.toLowerCase()}</span>{/if}
                </th>
              </tr>
              {#each group.apps as item (item.app.id)}
                {@const test = tested(item, now)}
                <tr>
                  <th scope="row" class="app">
                    <Link screen="apps" id={item.app.id}>{item.app.definition.name}</Link>
                    {#if !item.enabled}<span class="dim">not scheduled</span>{/if}
                  </th>
                  {#each destinations as destination (destination.id)}
                    {@const c = cell(item, destination.id, now)}
                    {#if c.mark}
                      <td><Link screen="apps" id={item.app.id} query={`storage=${destination.id}`} class="cell" label={`${item.app.definition.name} in ${destination.name}: ${c.title}`}><Mark mark={c.mark} shapeOnly /><span class="mono">{c.value}</span></Link></td>
                    {:else}
                      <td class="hatched" title={c.title}><span class="sr">{c.title}</span></td>
                    {/if}
                  {/each}
                  <td title={test.title}><span class="cell">{#if test.mark}<Mark mark={test.mark} shapeOnly title={test.title} />{/if}<span class="mono" class:dim={test.value === "untested"}>{test.value}</span></span></td>
                </tr>
              {/each}
            </tbody>
          {/each}
        </table>
      </div>
      {#if !destinations.length}<p class="note">No storage yet. <button type="button" class="link" onclick={() => (modal = "storage")}>Add storage</button></p>{/if}
    </section>
  {:else if loaded && steps.complete}
    <div class="empty"><strong>No apps found</strong><span>Approved devices report their Compose projects and containers within a minute.</span></div>
  {/if}

  {#if loaded}
    <section class="section" aria-labelledby="recent">
      <div class="section-head"><h2 id="recent">Activity</h2><Link screen="activity">All activity</Link></div>
      {#if operations.length}
        <div class="scroll">
          <table class="nowrap">
            <thead><tr><th>Time</th><th>Kind</th><th>App</th><th>Target</th><th>Result</th><th class="num">Took</th><th class="num">Size</th></tr></thead>
            <tbody>
              {#each operations.slice(0, 8) as op (op.id)}
                {@const row = activity(op, names)}
                <tr>
                  <td class="mono">{short(op.finished_at ?? op.created_at, now)}</td>
                  <td><Link screen="operation" id={op.id}>{row.kind}</Link></td>
                  <td>{row.subject}</td>
                  <td>{row.target}</td>
                  <td class="result {row.tone}">{row.result}</td>
                  <td class="num mono">{row.took !== undefined ? took(row.took) : ""}</td>
                  <td class="num mono">{op.result?.file_bytes !== undefined ? bytes(op.result.file_bytes) : ""}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {:else}
        <p class="note">No backups, restores, or checks yet.</p>
      {/if}
    </section>
  {/if}
</div>

{#if modal === "device"}<AddDevice onClose={() => (modal = "")} onChanged={read} />{/if}
{#if modal === "storage"}<StorageDialog {nodes} onClose={() => (modal = "")} onSaved={() => { modal = ""; read(); }} />{/if}
{#if modal === "schedule" && plan}<ScheduleBackups {plan} {destinations} {nodes} onClose={() => (modal = "")} onSaved={() => { modal = ""; read(); }} />{/if}

<style>
  .line {
    display: flex;
    flex-wrap: wrap;
    gap: 6px 28px;
    margin: 0;
    font-size: 15px;
  }
  .line div {
    display: flex;
    gap: 8px;
    align-items: baseline;
  }
  .line dt {
    color: var(--ink-muted);
  }
  .line dd {
    margin: 0;
    font-size: 15px;
  }
  td.mark {
    width: 28px;
    padding-right: 0;
  }
  .issues {
    list-style: none;
    margin: 0;
    padding: 0;
    border: 1px solid var(--line);
    background: var(--panel);
  }
  .issues li {
    display: grid;
    grid-template-columns: 16px minmax(120px, 160px) minmax(0, 240px) minmax(0, 1fr) auto;
    gap: 4px 16px;
    align-items: baseline;
    padding: 9px 12px;
    font-size: 13px;
  }
  .issues li + li {
    border-top: 1px solid var(--line);
  }
  .issues .mark {
    align-self: center;
    display: flex;
  }
  .subject {
    font-weight: 600;
    overflow-wrap: anywhere;
  }
  .condition {
    color: var(--ink-body);
    overflow-wrap: anywhere;
  }
  .detail {
    color: var(--ink-muted);
  }
  .go {
    white-space: nowrap;
    text-align: right;
  }
  .go :global(a) {
    color: var(--ink);
    text-decoration-color: var(--line-strong);
  }
  @media (max-width: 760px) {
    .issues li {
      grid-template-columns: 16px minmax(0, 1fr) auto;
    }
    .issues .condition,
    .issues .detail {
      grid-column: 2 / -1;
    }
    .issues .go {
      grid-column: 3;
      grid-row: 1;
    }
  }
  tr.next .subject {
    color: var(--ink);
  }
  /* A storage name is a value, so it keeps its own case. */
  .matrix th.target {
    font: 600 13px/1.2 "IBM Plex Sans", ui-sans-serif, system-ui, sans-serif;
    letter-spacing: 0;
    text-transform: none;
    color: var(--ink);
  }
  .matrix th[scope="col"]:not(:first-child),
  .matrix td {
    white-space: nowrap;
  }
  .matrix tr.group th {
    background: var(--nav);
    color: var(--ink);
    font: 600 13px/1.4 "IBM Plex Sans", ui-sans-serif, system-ui, sans-serif;
    letter-spacing: 0;
    text-transform: none;
    border-top: 1px solid var(--line);
    padding-top: 10px;
  }
  .matrix tbody:first-of-type tr.group th {
    border-top: 0;
  }
  .matrix tr.group :global(a) {
    color: var(--ink);
    text-decoration: none;
  }
  .state {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    margin-left: 12px;
    font-weight: 400;
    color: var(--ink-muted);
  }
  /* The app column stays in view while the storage columns scroll on a narrow screen. */
  .matrix th[scope="col"]:first-child,
  .matrix th.app {
    position: sticky;
    left: 0;
    z-index: 1;
  }
  .matrix th.app {
    background: var(--panel);
    font: 400 14px/1.4 "IBM Plex Sans", ui-sans-serif, system-ui, sans-serif;
    letter-spacing: 0;
    text-transform: none;
    color: var(--ink);
    padding-left: 24px;
    border-top: 1px solid var(--line);
    border-bottom: 0;
    white-space: nowrap;
  }
  .matrix th.app :global(a) {
    color: var(--ink);
    text-decoration: none;
  }
  .matrix th.app :global(a:hover),
  .matrix :global(a.cell:hover) {
    text-decoration: underline;
  }
  .matrix th.app .dim {
    margin-left: 10px;
    font-size: 12px;
  }
  .matrix td {
    min-width: 128px;
  }
  .matrix :global(.cell) {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    color: var(--ink);
    text-decoration: none;
  }
  .result.ok { color: var(--ok); }
  .result.fail { color: var(--fail); }
  .result.active { color: var(--active); }
  .result.unc { color: var(--unc); }
  .result.neutral { color: var(--ink-muted); }
</style>
