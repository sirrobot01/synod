<script lang="ts">
  import { api, detail, type Operation } from "../lib/api";
  import { errorView, finished, operationMark, type APIError } from "../lib/state";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Mark from "../lib/ui/Mark.svelte";

  let { id }: { id: string } = $props();

  let operation = $state<Operation | null>(null);
  let failure = $state<APIError | null>(null);
  let reading = $state(false);

  async function read() {
    // The address can change while a request is open. An answer for the previous
    // address must never be drawn under this one.
    const wanted = id;
    reading = true;
    try {
      const answer = await api.operation(wanted);
      if (wanted !== id) return;
      operation = answer;
      failure = null;
    } catch (error) {
      // The previous answer stays on the screen. Blanking it would hide the last
      // thing that was true, and a failed read proves nothing about the operation.
      if (wanted === id) failure = detail(error);
    } finally {
      if (wanted === id) reading = false;
    }
  }

  $effect(() => {
    // A changed address is a different operation, so drop the previous answer.
    operation = null;
    failure = null;
    read();
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
          <dt>App</dt><dd>{operation.app_id}</dd>
          <dt>Device</dt><dd>{operation.node_id}</dd>
          <dt>Created</dt><dd>{operation.created_at}</dd>
          <dt>Updated</dt><dd>{operation.updated_at}</dd>
          <dt>Finished</dt><dd>{operation.finished_at ?? "Not finished"}</dd>
        </dl>
        <button onclick={read} disabled={reading}>{reading ? "Reading" : "Read again"}</button>
      </div>
    </section>
  {:else if !failure}<p class="note">Reading the operation.</p>{/if}
</div>
<style>
 .screen { display:flex; flex-direction:column; gap:22px; }
 .strip { padding:16px; border-bottom:1px solid var(--line); display:flex; gap:16px; }
 .body { padding:16px; }
 .facts { display:grid; grid-template-columns:auto 1fr; gap:10px; font-size:12px; }
 dt { color:var(--ink-muted); }
 dd { margin:0; overflow-wrap:anywhere; }
</style>
