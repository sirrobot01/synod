<script lang="ts">
  import { onMount, untrack } from "svelte";
  import { bytes, timestamp } from "../lib/format";
  import { api, detail, type DestinationList, type Node, type Operation, type ProtectionPlan } from "../lib/api";
  import { errorView, finished, operationMark, type APIError } from "../lib/state";
  import { activity, failureText, nilID, nodeName, took } from "../lib/model";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Link from "../lib/ui/Link.svelte";
  import Mark from "../lib/ui/Mark.svelte";

  let { id }: { id: string } = $props();

  let operation = $state<Operation | null>(null);
  let failure = $state<APIError | null>(null);
  let reading = $state(false);

  let readID = "";
  let generation = 0;
  async function read() {
    if (readID === id) return;
    readID = id;
    const token = ++generation;
    // The address can change while a request is open. An answer for the previous
    // address must never be drawn under this one.
    const wanted = id;
    reading = true;
    try {
      const answer = await api.operation(wanted);
      if (wanted !== id || token !== generation) return;
      operation = answer;
      failure = null;
    } catch (error) {
      // The previous answer stays on the screen. Blanking it would hide the last
      // thing that was true, and a failed read proves nothing about the operation.
      if (wanted === id && token === generation) failure = detail(error);
    } finally {
      if (token === generation) { reading = false; readID = ""; }
    }
  }

  $effect(() => {
    // A changed address is a different operation, so drop the previous answer.
    const wanted = id;
    operation = null;
    failure = null;
    untrack(() => { if (wanted === id) read(); });
  });

  // An operation that no owner can change never changes again. Deriving this
  // keeps the poll below from restarting on every answer.
  let settled = $derived(operation !== null && finished(operation.status));

  $effect(() => {
    if (settled) return;
    const timer = setInterval(read, 5000);
    return () => clearInterval(timer);
  });

  let mark = $derived(operation === null ? null : operationMark(operation.status));

  // Names make the record readable. Without them the record still shows its identities.
  let plan = $state<ProtectionPlan | null>(null);
  let nodes = $state<Node[]>([]);
  let listing = $state<DestinationList | null>(null);
  onMount(async () => {
    const [p, n, d] = await Promise.allSettled([api.protection(), api.nodes(), api.destinations()]);
    if (p.status === "fulfilled") plan = p.value;
    if (n.status === "fulfilled") nodes = n.value;
    if (d.status === "fulfilled") listing = d.value;
  });
  let names = $derived({
    app: (app: string) => plan?.apps.find((a) => a.app.id === app)?.app.definition.name ?? "app",
    node: (node: string) => nodeName(nodes, node),
    storage: (storage?: string) => listing?.destinations.find((d) => d.id === storage)?.name ?? "",
  });
  let row = $derived(operation ? activity(operation, names) : null);
  let failureCode = $derived(operation?.failure_code || operation?.result?.failure_code || "");
  const statusText: Record<string, string> = { pending: "Queued. Not started.", running: "Running.", waiting: "Outcome unknown. The device must inspect the work before it continues.", succeeded: "Finished.", cancelled: "Cancelled.", cancelling: "Stopping." };
</script>

<div class="page">
  <header class="page-head">
    <div>
      <h1>{row ? `${row.kind.charAt(0).toUpperCase()}${row.kind.slice(1)}: ${row.subject}` : "Run"}</h1>
      <p class="sub"><Link screen="activity">Activity</Link></p>
    </div>
    <button type="button" onclick={read} disabled={reading}>{reading ? "Loading" : "Reload"}</button>
  </header>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if operation && mark && row}
    <div class="status">
      <Mark {mark} shapeOnly />
      <div class="stack tight">
        <p>{operation.status === "failed" ? failureText(failureCode) || "Failed." : operation.status === "succeeded" && row.result === "passed" ? "Passed." : statusText[operation.status]}</p>
        {#if failureCode}<span class="code">{failureCode}</span>{/if}
      </div>
    </div>
    <dl class="facts">
      <dt>App</dt><dd>{#if operation.app_id && operation.app_id !== nilID}<Link screen="apps" id={operation.app_id}>{names.app(operation.app_id)}</Link>{:else}<span class="dim">none (repository)</span>{/if}</dd>
      <dt>Device</dt><dd><Link screen="devices" id={operation.node_id}>{names.node(operation.node_id)}</Link></dd>
      <dt>Started by</dt><dd>{operation.actor_id.startsWith("schedule:") ? "schedule" : operation.actor_id || "synod"}</dd>
      <dt>Created</dt><dd class="mono">{timestamp(operation.created_at)}</dd>
      <dt>Finished</dt><dd class="mono">{operation.finished_at ? timestamp(operation.finished_at) : "not finished"}</dd>
      {#if row.took !== undefined}<dt>Took</dt><dd class="mono">{took(row.took)}</dd>{/if}
      {#if operation.result}
        {#if operation.result.destination_id}<dt>Storage</dt><dd><Link screen="storage">{names.storage(operation.result.destination_id) || operation.result.destination_id}</Link></dd>{/if}
        {#if operation.result.snapshot_id}<dt>Snapshot</dt><dd class="mono"><Link screen="apps" id={operation.app_id}>{operation.result.snapshot_id}</Link></dd>{/if}
        {#if operation.result.file_bytes !== undefined}<dt>Size</dt><dd class="mono">{bytes(operation.result.file_bytes)}</dd>{/if}
        {#if operation.result.downtime_ms !== undefined}<dt>Downtime</dt><dd class="mono">{operation.result.downtime_ms} ms</dd>{/if}
        {#if operation.result.safety_snapshot_id}<dt>Safety snapshot</dt><dd class="mono">{operation.result.safety_snapshot_id}</dd>{/if}
        {#if operation.result.target_app_id}<dt>Restored app</dt><dd><Link screen="apps" id={operation.result.target_app_id}>{names.app(operation.result.target_app_id)}</Link></dd>{/if}
        {#if operation.result.project_directory}<dt>Project directory</dt><dd class="mono">{operation.result.project_directory}</dd>{/if}
        {#if operation.result.read !== undefined}<dt>Verified</dt><dd class="mono">{operation.result.read}/{operation.result.contents ?? "?"} blocks, {bytes(operation.result.bytes_read)}</dd>{/if}
        {#if operation.result.containers !== undefined}<dt>Containers tested</dt><dd class="mono">{operation.result.containers}</dd>{/if}
        {#if operation.result.resource}<dt>Resource</dt><dd class="mono">{operation.result.resource}</dd>{/if}
        {#if operation.result.completed_at}<dt>Completed</dt><dd class="mono">{timestamp(operation.result.completed_at)}</dd>{/if}
      {/if}
      <dt>Kind</dt><dd class="mono">{operation.kind}</dd>
      <dt>ID</dt><dd class="mono">{operation.id}</dd>
    </dl>
    {#if operation.result?.recovery_pending?.length}<p class="callout danger">Inspect these resources on the device before another restore <span class="code">{operation.result.recovery_pending.join(", ")}</span></p>{/if}
    {#if operation.result?.restart_pending?.length}<p class="callout danger">Containers still need a restart <span class="code">{operation.result.restart_pending.join(", ")}</span></p>{/if}
    {#each operation.result?.warning_codes ?? [] as warning}<p class="callout attn">Finished with a warning <span class="code">{warning}</span></p>{/each}
    {#if operation.result?.checks?.length}
      <section class="section" aria-labelledby="checks">
        <h2 id="checks">Checks</h2>
        {#if operation.result.checks.some((check) => check.status !== "passed")}<p class="callout attn">Proof is incomplete or failed.</p>{/if}
        <div class="scroll">
          <table>
            <thead><tr><th>Result</th><th>Check</th><th>Resource</th><th>Detail</th></tr></thead>
            <tbody>{#each operation.result.checks as check}<tr><td class="result {check.status}">{check.status}</td><td class="mono">{check.kind}</td><td class="mono wrap">{check.resource}</td><td class="mono dim">{check.detail ?? ""}</td></tr>{/each}</tbody>
          </table>
        </div>
      </section>
    {/if}
  {:else if !failure}<p class="loading">Loading</p>{/if}
</div>

<style>
  h1 {
    overflow-wrap: anywhere;
  }
  .status {
    display: flex;
    gap: 12px;
    align-items: flex-start;
    font-size: 15px;
  }
  .tight {
    gap: 4px;
  }
  .facts {
    font-size: 14px;
  }
  .result.passed { color: var(--ok); }
  .result.failed { color: var(--fail); }
  .result.unchecked { color: var(--unc); }
</style>
