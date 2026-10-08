<script lang="ts">
  import { untrack } from "svelte";
  import { api, detail, uncertain, type AppPlan, type Destination, type Node, type Snapshot, type WorkSubmission, type Method } from "./api";
  import { errorView, type APIError } from "./state";
  import { timestamp } from "./format";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";
  let { kind, app, snapshot, destinations, nodes, destinationID = "", onClose, onAccepted }: {
    kind: "backup" | "restore" | "drill" | "integrity"; app?: AppPlan; snapshot?: Snapshot;
    destinations: Destination[]; nodes: Node[]; destinationID?: string; onClose: () => void; onAccepted: (id: string) => void;
  } = $props();
  let nodeID = $state("");
  let target = $state(untrack(() => destinationID || snapshot?.destination_ids[0] || app?.settings.destinations?.[0] || destinations[0]?.id || ""));
  let method = $state<Method>(untrack(() => app?.settings.method ?? "auto"));
  let replace = $state(false);
  let confirmation = $state("");
  let busy = $state(false);
  let failure = $state<APIError | null>(null);
  let submitted = $state<WorkSubmission | null>(null);
  let title = $derived(({ backup: "Back up now", restore: "Restore app", drill: "Test restore", integrity: "Check repository" })[kind]);
  let allowedDestinations = $derived(destinations.filter(d => (!snapshot || snapshot.destination_ids.includes(d.id)) && (kind !== "backup" || d.location.kind !== "local" || d.node_id === app?.app.node_id)));
  let allowedNodes = $derived(nodes.filter(n => n.approved_at && (!target || destinations.find(d => d.id === target)?.location.kind !== "local" || destinations.find(d => d.id === target)?.node_id === n.id)));
  async function submit() {
    busy = true;
    submitted ??= {
      id: crypto.randomUUID(), ...(app ? { app_id: app.app.id } : {}),
      ...(target ? { destination_id: target } : {}),
      ...(snapshot ? { snapshot_id: snapshot.id } : {}),
      ...(kind === "backup" ? { method } : { node_id: nodeID }),
      ...(kind === "restore" ? { replace } : {}),
      ...(kind === "drill" ? { kind: "restore_drill" as const } : {}),
      ...(kind === "integrity" ? { kind: "integrity_check" as const } : {}),
    };
    try { const result = await api.submit(kind === "backup" ? "backups" : kind === "restore" ? "restores" : "proofs", submitted); onAccepted(result.id); }
    catch (error) { failure = detail(error); if (!uncertain(error)) submitted = null; }
    finally { busy = false; }
  }
</script>
<Dialog label={title} {title} danger={replace} onClose={() => { if (!busy) onClose(); }}>
  <form class="stack" onsubmit={(event) => { event.preventDefault(); submit(); }}>
    {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
    {#if app}<strong>{app.app.definition.name} <span class="dim">· {app.app.node_name}</span></strong>{/if}
    {#if snapshot}<dl class="facts mono"><dt>Snapshot</dt><dd>{snapshot.id}</dd><dt>Saved</dt><dd>{timestamp(snapshot.completed_at)}</dd><dt>Source platform</dt><dd>{snapshot.architecture}</dd></dl>{/if}
    <fieldset class="stack" disabled={busy || submitted !== null}>
      <legend class="label">{kind === "restore" ? "Restore choices" : "Run choices"}</legend>
      <label>Destination<select required bind:value={target} onchange={() => { nodeID = ""; }}><option value="" disabled>Select a destination</option>{#each allowedDestinations as d}<option value={d.id}>{d.name} · {d.location.kind}</option>{/each}</select></label>
      {#if kind === "backup"}
        <label>Copy method<select bind:value={method}><option value="auto">Automatic</option><option value="live">Live</option><option value="dump">Dump</option><option value="stop">Stop</option></select></label>
        <p class="note">Automatic never stops the app. It dumps databases and copies other files while the app runs.</p>
        {#if method === "stop"}<p class="callout">Stop stops the app briefly for the final copy.</p>{/if}
      {:else}
        <label>{kind === "restore" ? "Restore to device" : "Run on device"}<select required bind:value={nodeID}><option value="" disabled>Select an approved device</option>{#each allowedNodes as node}<option value={node.id}>{node.name || node.id} · {node.architecture || "unknown platform"}</option>{/each}</select></label>
        <p class="note">The device must be online. Local destinations can only be read by their owner.</p>
      {/if}
      {#if kind === "restore"}
        <p>Synod checks image support, names, and ports before it starts the app. A source image can support more than one architecture.</p>
        {#if snapshot?.exclusions?.length}<p class="callout">This snapshot excludes {snapshot.exclusions.length} paths. Check the exclusions below the snapshot before you restore.</p>{/if}
        <label class="check"><input type="checkbox" bind:checked={replace} />Replace the existing app on the target device</label>
        {#if replace}
          <p class="callout danger">This stops and replaces the target app. Synod takes a safety snapshot first. If that snapshot fails, replacement stops.</p>
          <label>Type {app?.app.definition.name} to confirm replacement<input autocomplete="off" required bind:value={confirmation} /></label>
        {/if}
      {:else if kind === "drill"}<p>Synod restores this snapshot in a scratch area with no network or published ports. It checks the containers, then removes the drill.</p>
      {:else if kind === "integrity"}<p>Synod reads and verifies a sample of the repository. This does not test whether an app starts.</p>{/if}
    </fieldset>
    {#if submitted}<p class="callout">The reply was not confirmed. Retry this request or inspect its activity: <a href={`/operations/${submitted.id}`}>{submitted.id}</a>.</p>{/if}
    <div class="actions"><button class:primary={!replace} class:destructive={replace} disabled={busy || !target || (kind !== "backup" && !allowedNodes.some(n => n.id === nodeID)) || (replace && confirmation !== app?.app.definition.name)}>{busy ? "Submitting…" : submitted ? "Retry same request" : title}</button></div>
  </form>
</Dialog>
