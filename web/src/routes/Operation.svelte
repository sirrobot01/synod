<script lang="ts">
  import { untrack } from "svelte";
  import { bytes, timestamp } from "../lib/format";
  import { path } from "../lib/router.svelte";
  import { api, detail, type Operation } from "../lib/api";
  import { errorView, finished, operationMark, type APIError } from "../lib/state";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
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
</script>
<div class="screen">
  <h1 class="mono">{id}</h1>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if operation && mark}
    <section class="panel detail">
      <div class="strip"><Mark {mark} /><span class="mono">{operation.kind}</span></div>
      <div class="body">
        {#if operation.status === "pending"}<p>The request is recorded. Work has not started.</p>{/if}
        {#if operation.status === "waiting"}<p>The result is unknown. The device must inspect the work before it can continue.</p>{/if}
        <dl class="facts mono">
          <dt>Status</dt><dd>{operation.status}</dd>
          <dt>Failure code</dt><dd>{operation.failure_code || "None"}</dd>
          <dt>App</dt><dd>{#if operation.app_id && operation.app_id !== "00000000-0000-0000-0000-000000000000"}<a href={path("apps", operation.app_id)}>{operation.app_id}</a>{:else}Repository{/if}</dd>
          <dt>Device</dt><dd><a href={path("nodes", operation.node_id)}>{operation.node_id}</a></dd>
          <dt>Created</dt><dd>{operation.created_at}</dd>
          <dt>Updated</dt><dd>{operation.updated_at}</dd>
          <dt>Finished</dt><dd>{operation.finished_at ?? "Not finished"}</dd>
        </dl>
        {#if operation.result}
          <section class="stack result">
            <h2>Result</h2>
            <dl class="facts mono">
              {#if operation.result.snapshot_id}<dt>Snapshot</dt><dd><a href={path("snapshots", operation.app_id)}>{operation.result.snapshot_id}</a></dd>{/if}
              {#if operation.result.destination_id}<dt>Destination</dt><dd><a href={path("destinations")}>{operation.result.destination_id}</a></dd>{/if}
              {#if operation.result.file_bytes !== undefined}<dt>Saved size</dt><dd>{bytes(operation.result.file_bytes)}</dd>{/if}
              {#if operation.result.downtime_ms !== undefined}<dt>Downtime</dt><dd>{operation.result.downtime_ms} ms</dd>{/if}
              {#if operation.result.safety_snapshot_id}<dt>Safety snapshot</dt><dd>{operation.result.safety_snapshot_id}</dd>{/if}
              {#if operation.result.target_app_id}<dt>Restored app</dt><dd><a href={path("apps", operation.result.target_app_id)}>{operation.result.target_app_id}</a></dd>{/if}
              {#if operation.result.project_directory}<dt>Restored directory</dt><dd>{operation.result.project_directory}</dd>{/if}
              {#if operation.result.read !== undefined}<dt>Verified content</dt><dd>{operation.result.read} of {operation.result.contents ?? "unknown"} blocks · {bytes(operation.result.bytes_read)}</dd>{/if}
              {#if operation.result.containers !== undefined}<dt>Tested containers</dt><dd>{operation.result.containers}</dd>{/if}
              {#if operation.result.resource}<dt>Affected resource</dt><dd>{operation.result.resource}</dd>{/if}
              {#if operation.result.completed_at}<dt>Completed</dt><dd>{timestamp(operation.result.completed_at)}</dd>{/if}
            </dl>
            {#if operation.result.recovery_pending?.length}<p class="callout danger">Recovery needs attention: <span class="mono">{operation.result.recovery_pending.join(", ")}</span>. Inspect these resources on the device before a new restore.</p>{/if}
            {#if operation.result.restart_pending?.length}<p class="callout danger">Containers still need a restart: <span class="mono">{operation.result.restart_pending.join(", ")}</span>.</p>{/if}
            {#each operation.result.warning_codes ?? [] as warning}<p class="callout mono">{warning}</p>{/each}
          </section>
        {/if}
        <button onclick={read} disabled={reading}>{reading ? "Reading" : "Read again"}</button>
      </div>
    </section>
  {:else if !failure}<p class="note">Reading the operation.</p>{/if}
</div>
<style>
 .screen { display:flex; flex-direction:column; gap:22px; }
 .strip { padding:16px; border-bottom:1px solid var(--line); display:flex; gap:16px; }
 .body { padding:16px; }
 .result { margin:22px 0; }
 h1 { overflow-wrap:anywhere; }
 h2 { margin:0; font-size:16px; }
 .facts { display:grid; grid-template-columns:auto 1fr; gap:10px; font-size:12px; }
 dt { color:var(--ink-muted); }
 dd { margin:0; overflow-wrap:anywhere; }
</style>
