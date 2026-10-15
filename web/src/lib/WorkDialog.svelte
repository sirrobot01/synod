<script lang="ts">
  import { untrack } from "svelte";
  import { api, detail, uncertain, type AppPlan, type Destination, type Node, type WorkSubmission, type Method } from "./api";
  import { errorView, type APIError } from "./state";
  import { label, online } from "./model";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";

  // Starts a backup now, a restore test, or a check of backup storage.
  let { kind, app, destinations, nodes, destinationID = "", onClose, onAccepted }: {
    kind: "backup" | "drill" | "integrity"; app?: AppPlan;
    destinations: Destination[]; nodes: Node[]; destinationID?: string; onClose: () => void; onAccepted: (id: string) => void;
  } = $props();
  const now = Date.now();
  let target = $state(untrack(() => destinationID || app?.settings.destinations?.[0] || destinations[0]?.id || ""));
  let nodeID = $state("");
  let method = $state<Method>(untrack(() => app?.settings.method ?? "auto"));
  let busy = $state(false);
  let failure = $state<APIError | null>(null);
  // A request whose reply was lost keeps its identity for the retry.
  let submitted = $state<WorkSubmission | null>(null);
  let name = $derived(app?.app.definition.name ?? "");
  let title = $derived(kind === "backup" ? `Back up ${name}` : kind === "drill" ? `Test restore: ${name}` : `Check ${destinations.find((d) => d.id === target)?.name ?? "storage"}`);
  let note = $derived(kind === "backup" ? "One snapshot now. The schedule is unchanged." : kind === "drill" ? "Restores the newest snapshot into an isolated network, runs its checks, then removes it." : "Reads a sample of the repository and verifies it.");
  let allowedDestinations = $derived(destinations.filter((d) => kind !== "backup" || d.location.kind !== "local" || d.node_id === app?.app.node_id));
  let local = $derived(destinations.find((d) => d.id === target)?.location.kind === "local" ? destinations.find((d) => d.id === target)?.node_id : undefined);
  let allowedNodes = $derived(nodes.filter((n) => n.approved_at && (!local || local === n.id)));

  async function submit() {
    busy = true;
    submitted ??= {
      id: crypto.randomUUID(), ...(app ? { app_id: app.app.id } : {}),
      ...(target ? { destination_id: target } : {}),
      ...(kind === "backup" ? { method } : { node_id: nodeID }),
      ...(kind === "drill" ? { kind: "restore_drill" as const } : {}),
      ...(kind === "integrity" ? { kind: "integrity_check" as const } : {}),
    };
    try {
      const result = await api.submit(kind === "backup" ? "backups" : "proofs", submitted);
      onAccepted(result.id);
    } catch (error) {
      failure = detail(error);
      if (!uncertain(error)) submitted = null;
    } finally {
      busy = false;
    }
  }
</script>

<Dialog label={title} {title} {note} onClose={() => { if (!busy) onClose(); }}>
  <form id="work-form" class="stack" onsubmit={(event) => { event.preventDefault(); submit(); }}>
    {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
    <fieldset class="stack" disabled={busy || submitted !== null}>
      <legend class="sr">Choices</legend>
      <label class="field">Storage
        <select required bind:value={target} onchange={() => { nodeID = ""; }}>
          <option value="" disabled>Choose storage</option>
          {#each allowedDestinations as d (d.id)}<option value={d.id}>{d.name}</option>{/each}
        </select>
      </label>
      {#if kind === "backup"}
        <label class="field">Method
          <select bind:value={method}>
            <option value="auto">auto</option>
            <option value="live">live</option>
            <option value="dump">dump</option>
            <option value="stop">stop</option>
          </select>
        </label>
        {#if method === "stop"}<p class="callout attn">stop takes the app down for the final pass.</p>{/if}
      {:else}
        <label class="field">Run on
          <select required bind:value={nodeID}>
            <option value="" disabled>Choose a device</option>
            {#each allowedNodes as node (node.id)}<option value={node.id}>{label(node)} ({node.architecture || "unknown"}){online(node, now) ? "" : ", offline"}</option>{/each}
          </select>
        </label>
      {/if}
    </fieldset>
    {#if submitted}<p class="callout attn">No reply. Retry the same request <span class="code">{submitted.id}</span></p>{/if}
  </form>
  {#snippet footer()}
    <button type="button" class="ghost" onclick={onClose} disabled={busy}>Cancel</button>
    <button type="submit" form="work-form" class="primary" disabled={busy || !target || (kind !== "backup" && !allowedNodes.some((n) => n.id === nodeID))}>{busy ? "Starting" : submitted ? "Retry same request" : kind === "backup" ? "Back up now" : kind === "drill" ? "Start test" : "Start check"}</button>
  {/snippet}
</Dialog>
