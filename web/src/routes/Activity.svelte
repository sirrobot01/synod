<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type DestinationList, type Node, type NotificationStatus, type Operation, type ProtectionPlan } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import { bytes } from "../lib/format";
  import { activity, failureText, kinds, nodeName, took, when } from "../lib/model";
  import Link from "../lib/ui/Link.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";

  let operations = $state<Operation[]>([]);
  let alerts = $state<NotificationStatus | null>(null);
  let plan = $state<ProtectionPlan | null>(null);
  let listing = $state<DestinationList | null>(null);
  let nodes = $state<Node[]>([]);
  let failure = $state<APIError | null>(null);
  let alertFailure = $state<APIError | null>(null);
  let loaded = $state(false);
  let busy = $state(false);
  let filter = $state("");
  let kind = $state("");
  let now = $state(Date.now());

  let names = $derived({
    app: (id: string) => plan?.apps.find((a) => a.app.id === id)?.app.definition.name ?? "app",
    node: (id: string) => nodeName(nodes, id),
    storage: (id?: string) => listing?.destinations.find((d) => d.id === id)?.name ?? "",
  });
  let rows = $derived(operations.map((op) => ({ op, row: activity(op, names) })));
  let shown = $derived(rows.filter(({ op, row }) => (!kind || op.kind === kind) && `${row.subject} ${row.target} ${row.result} ${op.id}`.toLowerCase().includes(filter.toLowerCase())));

  async function read() {
    if (busy) return;
    busy = true;
    const results = await Promise.allSettled([api.operations(), api.notifications(), api.protection(), api.nodes(), api.destinations()]);
    if (results[0].status === "fulfilled") { operations = results[0].value; loaded = true; failure = null; } else failure = detail(results[0].reason);
    if (results[1].status === "fulfilled") { alerts = results[1].value; alertFailure = null; } else alertFailure = detail(results[1].reason);
    if (results[2].status === "fulfilled") plan = results[2].value;
    if (results[3].status === "fulfilled") nodes = results[3].value;
    if (results[4].status === "fulfilled") listing = results[4].value;
    now = Date.now();
    busy = false;
  }
  onMount(() => {
    read();
    const timer = setInterval(read, 10000);
    return () => clearInterval(timer);
  });
</script>

<div class="page">
  <header class="page-head">
    <div>
      <h1>Activity</h1>
      <p class="sub">Last 100 runs</p>
    </div>
    <div class="actions"><button type="button" onclick={read} disabled={busy}>Refresh</button></div>
  </header>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  <section class="section">
    <div class="tools">
      <label><span class="sr">Filter</span><input type="search" bind:value={filter} placeholder="Filter by app, target, or result" /></label>
      <label><span class="sr">Kind</span>
        <select bind:value={kind}>
          <option value="">All kinds</option>
          {#each [...new Set(operations.map((op) => op.kind))] as value}<option {value}>{kinds[value] ?? value}</option>{/each}
        </select>
      </label>
    </div>
    {#if !loaded && !failure}<p class="loading" role="status">Loading</p>{/if}
    {#if loaded}
      <div class="scroll">
        <table class="nowrap">
          <thead><tr><th>Started</th><th>Kind</th><th>App</th><th>Target</th><th>Result</th><th class="num">Took</th><th class="num">Size</th><th>By</th></tr></thead>
          <tbody>
            {#each shown as { op, row } (op.id)}
              <tr>
                <td class="mono">{when(op.created_at, now)}</td>
                <td><Link screen="operation" id={op.id}>{row.kind}</Link></td>
                <td>{row.subject}</td>
                <td>{row.target}</td>
                <td><span class="result {row.tone}">{row.result}</span>{#if op.status === "failed"}<small>{failureText(op.failure_code)} <span class="code">{op.failure_code}</span></small>{/if}</td>
                <td class="num mono">{row.took !== undefined ? took(row.took) : ""}</td>
                <td class="num mono">{op.result?.file_bytes !== undefined ? bytes(op.result.file_bytes) : ""}</td>
                <td class="dim">{op.actor_id.startsWith("schedule:") ? "schedule" : op.actor_id}</td>
              </tr>
            {:else}
              <tr><td colspan="8" class="dim">{operations.length ? "No runs match." : "No runs yet."}</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>
  <section class="section" aria-labelledby="alerts">
    <div class="section-head"><h2 id="alerts">Notifications sent</h2><Link screen="settings" id="notifications">Notification settings</Link></div>
    {#if alertFailure}<ErrorPanel view={errorView(alertFailure)} />{/if}
    <div class="scroll">
      <table>
        <thead><tr><th>Created</th><th>Message</th><th>Delivery</th><th class="num">Attempts</th></tr></thead>
        <tbody>
          {#each alerts?.alerts ?? [] as alert (alert.id)}
            <tr>
              <td class="mono">{when(alert.created_at, now)}</td>
              <td>{alert.message}{#if alert.operation_id} <Link screen="operation" id={alert.operation_id}>Run</Link>{/if}</td>
              <td><span class="result" class:ok={alert.state === "sent"} class:fail={alert.state === "failed"}>{alert.state}</span>{#if alert.failure_code}<small class="code">{alert.failure_code}</small>{/if}</td>
              <td class="num mono">{alert.attempts}</td>
            </tr>
          {:else}
            <tr><td colspan="4" class="dim">{alerts ? "None sent." : "Loading"}</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>
</div>

<style>
  .tools {
    display: grid;
    grid-template-columns: minmax(0, 360px) minmax(0, 200px);
    gap: 8px;
  }
  .result.ok { color: var(--ok); }
  .result.fail { color: var(--fail); }
  .result.active { color: var(--active); }
  .result.unc { color: var(--unc); }
  .result.neutral { color: var(--ink-muted); }
  @media (max-width: 600px) {
    .tools {
      grid-template-columns: 1fr;
    }
  }
</style>
