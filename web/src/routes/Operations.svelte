<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type Operation, type NotificationStatus, type ProtectionPlan, type Node } from "../lib/api";
  import { errorView, operationMark, type APIError } from "../lib/state";
  import { timestamp } from "../lib/format";
  import { follow, path } from "../lib/router.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Panel from "../lib/ui/Panel.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  let { onOpen }: { onOpen: (id: string) => void } = $props();
  let operations = $state<Operation[]>([]); let alerts = $state<NotificationStatus | null>(null);
  let apps = $state<ProtectionPlan | null>(null); let nodes = $state<Node[]>([]);
  let failure = $state<APIError | null>(null); let alertFailure = $state<APIError | null>(null);
  let loaded = $state(false); let busy = $state(false); let filter = $state(""); let kind = $state("");
  let shown = $derived(operations.filter(op => (!kind || op.kind === kind) && `${op.id} ${op.status} ${apps?.apps.find(a => a.app.id === op.app_id)?.app.definition.name ?? op.app_id}`.toLowerCase().includes(filter.toLowerCase())));
  async function read() {
    if (busy) return; busy = true;
    const results = await Promise.allSettled([api.operations(), api.notifications(), api.protection(), api.nodes()]);
    if (results[0].status === "fulfilled") { operations = results[0].value; loaded = true; failure = null; } else failure = detail(results[0].reason);
    if (results[1].status === "fulfilled") { alerts = results[1].value; alertFailure = null; } else alertFailure = detail(results[1].reason);
    if (results[2].status === "fulfilled") apps = results[2].value;
    if (results[3].status === "fulfilled") nodes = results[3].value;
    busy = false;
  }
  onMount(() => { read(); const timer = setInterval(read, 10000); return () => clearInterval(timer); });
</script>
<div class="page">
  <header class="page-head"><div><h1>Activity</h1><p class="note">The 100 most recent operations and notification deliveries.</p></div><div class="actions"><button onclick={read} disabled={busy}>Refresh</button></div></header>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  <div class="actions"><label>Find activity <input type="search" bind:value={filter} placeholder="App, status, or operation ID" /></label><label>Kind <select bind:value={kind}><option value="">All kinds</option>{#each [...new Set(operations.map(op => op.kind))] as value}<option value={value}>{value.replaceAll("_", " ")}</option>{/each}</select></label></div>
  <Panel title="Operations" stale={failure !== null}><div class="scroll"><table><thead><tr><th>Operation</th><th>State</th><th>App / device</th><th>Created</th></tr></thead><tbody>
    {#each shown as operation (operation.id)}<tr><td><a href={path("operation", operation.id)} onclick={(e) => follow(e, () => onOpen(operation.id))}>{operation.kind.replaceAll("_", " ")}</a><small class="mono wrap">{operation.id}</small></td><td><Mark mark={operationMark(operation.status)} />{#if operation.failure_code}<small class="mono">{operation.failure_code}</small>{/if}</td><td>{apps?.apps.find(a => a.app.id === operation.app_id)?.app.definition.name ?? (operation.app_id === "00000000-0000-0000-0000-000000000000" ? "Repository" : operation.app_id)}<small>{nodes.find(n => n.id === operation.node_id)?.name ?? operation.node_id}</small></td><td class="mono">{timestamp(operation.created_at)}</td></tr>
    {:else}<tr><td colspan="4">{loaded ? operations.length ? "No operations match." : "No operations have been recorded yet." : "Reading operations…"}</td></tr>{/each}
  </tbody></table></div></Panel>
  {#if alertFailure}<ErrorPanel view={errorView(alertFailure)} />{/if}
  <Panel title="Notification deliveries" stale={alertFailure !== null}><div class="scroll"><table><thead><tr><th>Alert</th><th>Delivery</th><th>Attempts</th><th>Created</th></tr></thead><tbody>
    {#each alerts?.alerts ?? [] as alert (alert.id)}<tr><td>{alert.message}{#if alert.operation_id}<small><a href={path("operation", alert.operation_id)} onclick={(e) => follow(e, () => onOpen(alert.operation_id!))}>Related operation</a></small>{/if}<small class="mono wrap">{alert.id}</small></td><td><Mark mark={operationMark(alert.state === "sent" ? "succeeded" : alert.state === "sending" ? "running" : alert.state as "pending" | "failed" | "cancelled")} />{#if alert.failure_code}<small class="mono">{alert.failure_code}</small>{/if}</td><td class="mono">{alert.attempts}</td><td class="mono">{timestamp(alert.created_at)}</td></tr>
    {:else}<tr><td colspan="4">{alerts ? "No notification deliveries recorded." : "Reading deliveries…"}</td></tr>{/each}
  </tbody></table></div></Panel>
</div>
