<script lang="ts">
  import { onDestroy, untrack } from "svelte";
  import { api, detail, uncertain, type AppPlan, type Destination, type Node, type Operation, type Snapshot, type WorkSubmission } from "./api";
  import { errorView, finished, operationMark, type APIError } from "./state";
  import { bytes } from "./format";
  import { failureText, label, nodeName, targets, when } from "./model";
  import { path } from "./router.svelte";
  import Dialog from "./ui/Dialog.svelte";
  import Mark from "./ui/Mark.svelte";
  import Icon from "./ui/Icon.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";

  // A guided restore: pick a snapshot, pick a device, confirm, then follow the work.
  let { item, copies, start = undefined, destinations, nodes, onClose, onFinished = () => {} }: {
    item: AppPlan; copies: Snapshot[]; start?: Snapshot | undefined; destinations: Destination[]; nodes: Node[]; onClose: () => void; onFinished?: () => void;
  } = $props();

  const now = Date.now();
  const name = untrack(() => item.app.definition.name);
  let step = $state(untrack(() => (start ? 2 : 1)));
  let copyID = $state(untrack(() => start?.id ?? copies[0]?.id ?? ""));
  let nodeID = $state("");
  let confirmation = $state("");
  let busy = $state(false);
  let failure = $state<APIError | null>(null);
  let submitted = $state<WorkSubmission | null>(null);
  let operation = $state<Operation | null>(null);
  let timer: ReturnType<typeof setInterval> | undefined;

  let copy = $derived(copies.find((c) => c.id === copyID));
  let choices = $derived(copy ? targets(item, copy, nodes, destinations, now) : []);
  let choice = $derived(choices.find((c) => c.node.id === nodeID && !c.disabled));
  let storageNames = (ids: string[]) => ids.map((id) => destinations.find((d) => d.id === id)?.name ?? "unknown").join(", ");
  let ready = $derived(step === 1 ? !!copy : step === 2 ? !!choice : !!choice && (!choice.same || confirmation === name));

  // A device that can no longer take the snapshot is unselected.
  $effect(() => {
    if (nodeID && !choice) untrack(() => (nodeID = ""));
  });

  async function submit() {
    if (!copy || !choice) return;
    busy = true;
    submitted ??= { id: crypto.randomUUID(), app_id: item.app.id, node_id: choice.node.id, snapshot_id: copy.id, destination_id: choice.destinationID, replace: choice.same };
    try {
      operation = await api.submit("restores", submitted);
      failure = null;
      step = 4;
      follow();
    } catch (error) {
      failure = detail(error);
      if (!uncertain(error)) submitted = null;
    } finally {
      busy = false;
    }
  }

  // The dialog follows the restore until it ends. A failed read keeps the last answer.
  function follow() {
    clearInterval(timer);
    timer = setInterval(async () => {
      if (!operation || finished(operation.status)) {
        clearInterval(timer);
        return;
      }
      try {
        operation = await api.operation(operation.id);
        if (finished(operation.status)) {
          clearInterval(timer);
          onFinished();
        }
      } catch {
        // The next read tries again.
      }
    }, 3000);
  }
  onDestroy(() => clearInterval(timer));

  function next() {
    if (!ready) return;
    if (step < 3) step += 1;
    else submit();
  }
</script>

<Dialog label={`Restore ${name}`} title={`Restore ${name}`} width={720} danger={step === 3 && !!choice?.same} onClose={() => { if (!busy) onClose(); }}>
  {#snippet head()}
    <ol class="steps" aria-label="Steps">
      {#each ["Snapshot", "Device", "Confirm"] as label, index}
        <li class:on={step === index + 1} class:done={step > index + 1} aria-current={step === index + 1 ? "step" : undefined}>
          <span class="num mono">{index + 1}</span>{label}
        </li>
      {/each}
    </ol>
  {/snippet}
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if step === 1}
    {#if copies.length === 0}
      <div class="empty"><strong>No snapshots</strong><span>Back up the app first.</span></div>
    {:else}
      <div class="choices" role="group" aria-label="Snapshots">
        {#each copies as c (c.id)}
          <button type="button" class="pick" aria-pressed={c.id === copyID} onclick={() => (copyID = c.id)}>
            <span class="row"><span class="mono">{when(c.completed_at, now)}</span><span class="mono dim">{bytes(c.file_bytes)}</span></span>
            <span class="row small"><span class="dim">{storageNames(c.destination_ids)}</span><span class="mono dim">{nodes.some((n) => n.id === c.source_node_id) ? nodeName(nodes, c.source_node_id) : c.source_node_name} {c.architecture}</span></span>
            {#if c.limits?.length}<span class="code">{c.limits.join(", ")}</span>{/if}
          </button>
        {/each}
      </div>
    {/if}
  {:else if step === 2}
    {#if copy?.restore_architectures?.length}<p class="note">Images in this snapshot: <span class="mono">{copy.restore_architectures.join(", ")}</span></p>{/if}
    <div class="choices" role="group" aria-label="Devices">
      {#each choices as c (c.node.id)}
        <button type="button" class="pick" aria-pressed={c.node.id === nodeID} disabled={c.disabled} onclick={() => (nodeID = c.node.id)}>
          <span class="row"><strong>{label(c.node)}</strong><span class="mono dim">{c.node.architecture || "unknown"}</span></span>
          <span class="note small">{c.note}</span>
        </button>
      {:else}
        <div class="empty"><strong>No devices</strong><span>Add and approve a device.</span></div>
      {/each}
    </div>
  {:else if step === 3 && copy && choice}
    <dl class="facts">
      <dt>Snapshot</dt><dd class="mono">{when(copy.completed_at, now)}</dd>
      <dt>From</dt><dd>{storageNames([choice.destinationID])}</dd>
      <dt>To</dt><dd>{label(choice.node)} <span class="mono dim">{choice.node.architecture}</span></dd>
    </dl>
    <ol class="plan">
      <li>Preflight: ports, container names, images.</li>
      {#if choice.same}<li>Safety snapshot of the running {name}, then stop it.</li>{/if}
      <li>Load images from the snapshot.</li>
      <li>Restore bind mounts and volumes{copy.databases?.length ? ", then load the database dumps" : ""}.</li>
      <li>Start the project and run its checks.</li>
    </ol>
    {#if copy.exclusions?.length}
      <details>
        <summary>{copy.exclusions.length === 1 ? "1 path" : `${copy.exclusions.length} paths`} not in this snapshot</summary>
        <div class="scroll inset"><table><tbody>{#each copy.exclusions as excluded}<tr><td class="mono wrap">{excluded.source}</td><td class="dim">{excluded.reason}</td></tr>{/each}</tbody></table></div>
      </details>
    {/if}
    {#if choice.same}
      <div class="callout danger stack">
        <p>Replaces the running {name} on {label(choice.node)}. If the safety snapshot fails, nothing changes.</p>
        <label class="field">Type {name} to confirm<input class="mono" autocomplete="off" bind:value={confirmation} /></label>
      </div>
    {/if}
    {#if submitted}<p class="callout attn">No reply. Retry the same request <span class="code">{submitted.id}</span></p>{/if}
  {:else if step === 4 && operation}
    {@const mark = operationMark(operation.status)}
    <div class="progress" role="status">
      <Mark {mark} shapeOnly />
      <div class="stack tight">
        {#if operation.status === "succeeded"}
          <strong>{name} restored on {nodeName(nodes, operation.node_id)}</strong>
          {#if operation.result?.project_directory}<p class="mono">{operation.result.project_directory}</p>{/if}
        {:else if operation.status === "failed"}
          <strong>Restore failed</strong>
          <p class="note">{failureText(operation.failure_code || operation.result?.failure_code)}</p>
          <span class="code">{operation.failure_code || operation.result?.failure_code}</span>
        {:else if operation.status === "waiting"}
          <strong>Outcome unknown</strong>
          <p class="note">The device must inspect the work before it continues.</p>
        {:else if operation.status === "cancelled"}
          <strong>Restore cancelled</strong>
        {:else}
          <strong>{operation.status === "pending" ? "Queued" : "Restoring"}</strong>
          <p class="note">Closing this dialog does not stop the restore.</p>
        {/if}
      </div>
    </div>
    {#if operation.result?.safety_snapshot_id}<p class="note">Safety snapshot <span class="mono">{operation.result.safety_snapshot_id}</span></p>{/if}
    {#if operation.result?.recovery_pending?.length}<p class="callout danger">Resources need a manual check before another restore.</p>{/if}
    {#if operation.result?.checks?.length}<p class="note">Checks passed <span class="mono">{operation.result.checks.filter((c) => c.status === "passed").length}/{operation.result.checks.length}</span></p>{/if}
    <p><a href={path("operation", operation.id)}>Restore details</a></p>
  {/if}
  {#snippet footer()}
    {#if step === 4}
      <button type="button" class="primary" onclick={onClose}>Close</button>
    {:else}
      <button type="button" class="ghost" disabled={busy || (step > 1 && submitted !== null)} onclick={() => (step === 1 ? onClose() : (step -= 1))}>{#if step > 1}<Icon name="left" />{/if}{step === 1 ? "Cancel" : "Back"}</button>
      <button type="button" class={step === 3 && choice?.same ? "destructive" : "primary"} disabled={busy || !ready} onclick={next}>{#if step === 3}<Icon name="restore" />{/if}{busy ? "Starting" : step < 3 ? "Next" : submitted ? "Retry same request" : `Restore ${name} on ${choice ? label(choice.node) : "device"}`}{#if step < 3}<Icon name="right" />{/if}</button>
    {/if}
  {/snippet}
</Dialog>

<style>
  .steps {
    list-style: none;
    margin: 8px 0 0;
    padding: 0;
    display: flex;
    gap: 20px;
    flex-wrap: wrap;
  }
  .steps li {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 13px;
    color: var(--ink-dim);
  }
  .steps li.on {
    color: var(--ink);
    font-weight: 600;
  }
  .steps li.done {
    color: var(--ink-muted);
  }
  .num {
    width: 20px;
    height: 20px;
    border: 1px solid currentColor;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 11px;
  }
  .on .num {
    background: var(--ink);
    border-color: var(--ink);
    color: var(--invert-ink);
  }
  .choices {
    display: flex;
    flex-direction: column;
    gap: 6px;
    max-height: 400px;
    overflow: auto;
  }
  .row {
    display: flex;
    justify-content: space-between;
    gap: 8px;
  }
  .small {
    font-size: 12px;
  }
  .plan {
    margin: 0;
    padding-left: 20px;
    color: var(--ink-body);
    display: flex;
    flex-direction: column;
    gap: 2px;
    font-size: 13px;
  }
  .inset {
    margin-top: 8px;
  }
  .progress {
    display: flex;
    gap: 12px;
    align-items: flex-start;
    padding: 14px;
    border: 1px solid var(--line-strong);
  }
  .tight {
    gap: 4px;
  }
</style>
