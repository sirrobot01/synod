<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type Destination, type DestinationList, type Node, type ProtectionPlan } from "../lib/api";
  import { errorView, operationMark, type APIError } from "../lib/state";
  import { bytes } from "../lib/format";
  import { failureText, kindNames, nodeName, storageNewest, used, when, where } from "../lib/model";
  import { go } from "../lib/router.svelte";
  import Link from "../lib/ui/Link.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Icon from "../lib/ui/Icon.svelte";
  import IconButton from "../lib/ui/IconButton.svelte";
  import Dialog from "../lib/ui/Dialog.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import StorageDialog from "../lib/StorageDialog.svelte";
  import WorkDialog from "../lib/WorkDialog.svelte";

  let listing = $state<DestinationList | null>(null);
  let nodes = $state<Node[]>([]);
  let plan = $state<ProtectionPlan | null>(null);
  let failure = $state<APIError | null>(null);
  let removalFailure = $state<APIError | null>(null);
  let busy = $state(false);
  let removing = $state(false);
  let add = $state(false);
  let remove = $state<Destination | null>(null);
  let check = $state<Destination | null>(null);
  let now = $state(Date.now());
  let readAt = $state(0);

  async function read() {
    if (busy) return;
    busy = true;
    try {
      const [d, n] = await Promise.all([api.destinations(), api.nodes()]);
      listing = d; nodes = n; failure = null; readAt = Date.now();
    } catch (error) {
      failure = detail(error);
    } finally {
      busy = false;
      now = Date.now();
    }
    try { plan = await api.protection(); } catch { /* The newest snapshot stays unknown. */ }
  }
  async function confirmRemove() {
    if (!remove) return;
    removing = true;
    try {
      await api.removeDestination(remove.id);
      remove = null;
      await read();
    } catch (error) {
      removalFailure = detail(error);
    } finally {
      removing = false;
    }
  }
  onMount(() => {
    read();
    const timer = setInterval(() => { if (!add && !remove && !check) read(); }, 15000);
    return () => clearInterval(timer);
  });
  const verdict = (value: string) => (value === "succeeded" ? "passed" : value === "failed" ? "failed" : "running");
</script>

<div class="page">
  <header class="page-head">
    <div>
      <h1>Storage</h1>
      {#if listing}<p class="sub">{listing.destinations.length === 1 ? "1 encrypted repository" : `${listing.destinations.length} encrypted repositories`}</p>{/if}
    </div>
    <div class="actions">
      <IconButton icon="refresh" label="Refresh" onclick={read} disabled={busy} />
      <button type="button" class="primary" onclick={() => (add = true)} disabled={!listing}><Icon name="plus" />Add storage</button>
    </div>
  </header>
  {#if failure}
    <ErrorPanel view={errorView(failure)} />
    {#if listing}<p class="callout attn hatched">Not refreshed. Showing data from {when(new Date(readAt).toISOString(), now)}.</p>{/if}
  {/if}
  {#if !listing && !failure}<p class="loading" role="status">Loading</p>{/if}
  {#if listing}
    {#if listing.destinations.length === 0}
      <div class="empty"><strong>No storage</strong><span>Snapshots need a place to go. A folder on a second device is the simplest start.</span><button type="button" class="primary" onclick={() => (add = true)}><Icon name="plus" />Add storage</button></div>
    {/if}
    {#each listing.destinations as destination (destination.id)}
      {@const place = where(destination, nodes)}
      {@const space = listing.space.find((s) => s.destination_id === destination.id)}
      {@const fresh = !!space?.known && !!space.observed_at && now - Date.parse(space.observed_at) <= 300000}
      {@const share = used(listing, destination.id)}
      {@const proof = listing.checks?.find((p) => p.destination_id === destination.id)}
      {@const upkeep = listing.maintenance?.find((m) => m.destination_id === destination.id)}
      <section class="section target" aria-labelledby={`s-${destination.id}`}>
        <div class="section-head">
          <h2 id={`s-${destination.id}`}>{destination.name} <span class="kind">{kindNames[destination.location.kind]}</span></h2>
          <div class="actions">
            <button type="button" class="small" onclick={() => (check = destination)}><Icon name="check" />Check now</button>
            <IconButton icon="trash" tone="danger" label={`Remove ${destination.name}`} onclick={() => { remove = destination; removalFailure = null; }} />
          </div>
        </div>
        <div class="body">
          <dl class="facts">
            <dt>Host</dt><dd class="mono">{place.host || "unknown"}</dd>
            <dt>Path</dt><dd class="mono">{place.path || "/"}</dd>
            <dt>Newest snapshot</dt><dd class="mono">{when(storageNewest(plan, destination.id), now, "none")}</dd>
            <dt>Last check</dt>
            <dd>
              {#if proof}
                <Link screen="operation" id={proof.id} class="inline"><Mark mark={operationMark(proof.state)} shapeOnly /><span class="mono">{when(proof.result.completed_at, now, "running")}</span> {verdict(proof.state)}</Link>
                {#if proof.state === "failed"}<span class="block">{failureText(proof.result.failure_code)} <span class="code">{proof.result.failure_code}</span></span>{/if}
              {:else}<span class="dim">never</span>{/if}
            </dd>
            <dt>Cleanup</dt>
            <dd>
              {#if !upkeep?.node_id}
                <span class="dim">no device assigned; approve a device</span>
              {:else}
                {nodeName(nodes, upkeep.node_id)}
                {#if upkeep.last}<Link screen="operation" id={upkeep.last.id} class="inline"><Mark mark={operationMark(upkeep.last.state)} shapeOnly /><span class="mono">{when(upkeep.last.result.completed_at, now, "running")}</span></Link>{:else}<span class="dim">, no run yet</span>{/if}
                {#if upkeep.last?.result.maintenance && upkeep.last.result.maintenance !== "completed"}<span class="block dim">First full cleanup on this device starts after 6 h.</span>{/if}
              {/if}
            </dd>
          </dl>
          <div class="space">
            <span class="label">Space</span>
            {#if share !== undefined && space}
              <svg class="meter" class:stale={!fresh} viewBox="0 0 100 8" preserveAspectRatio="none" role="img" aria-label={`${share}% used`}><rect width="100" height="8" class="track"></rect><rect width={Math.max(1, share)} height="8" class:full={share >= 90}></rect></svg>
              <span class="mono">{bytes(space.total_bytes - space.available_bytes)} used of {bytes(space.total_bytes)}, {share}%</span>
              {#if !fresh}<span class="dim">reported {when(space.observed_at, now)}</span>{/if}
            {:else}
              <span class="meter unknown hatched" aria-hidden="true"></span>
              <span class="dim">not reported</span>
            {/if}
          </div>
        </div>
        <p class="code">{destination.id}</p>
      </section>
    {/each}
    {#if listing.delivery.length}
      <section class="section" aria-labelledby="delivery">
        <div class="section-head"><h2 id="delivery">Storage list on devices</h2><span class="mono dim">revision {listing.revision}</span></div>
        <div class="scroll">
          <table>
            <colgroup><col /><col class="c-result" /><col class="c-time" /></colgroup>
            <thead><tr><th>Device</th><th>State</th><th>Acknowledged</th></tr></thead>
            <tbody>
              {#each listing.delivery as delivery (delivery.node_id)}
                <tr><td><Link screen="devices" id={delivery.node_id}>{nodeName(nodes, delivery.node_id)}</Link></td><td>{delivery.current ? "current" : "pending"}</td><td class="mono">{when(delivery.acknowledged_at, now)}</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>
    {/if}
  {/if}
</div>
{#if add}<StorageDialog {nodes} onClose={() => (add = false)} onSaved={() => { add = false; read(); }} />{/if}
{#if remove}
  <Dialog label="Remove storage" title={`Remove ${remove.name}`} danger width={480} onClose={() => { if (!removing) remove = null; }}>
    <p>Backups to this storage stop. The repository and its snapshots stay where they are.</p>
    {#if removalFailure}<ErrorPanel view={errorView(removalFailure)} />{/if}
    {#snippet footer()}
      <button type="button" class="ghost" onclick={() => (remove = null)} disabled={removing}>Cancel</button>
      <button type="button" class="destructive" onclick={confirmRemove} disabled={removing}><Icon name="trash" />{removing ? "Removing" : "Remove storage"}</button>
    {/snippet}
  </Dialog>
{/if}
{#if check && listing}<WorkDialog kind="integrity" destinationID={check.id} destinations={listing.destinations.filter((d) => d.id === check!.id)} {nodes} onClose={() => (check = null)} onAccepted={(op) => go("operation", op)} />{/if}

<style>
  .target {
    padding-bottom: 24px;
    border-bottom: 1px solid var(--line);
  }
  .kind {
    margin-left: 10px;
    font-size: 13px;
    font-weight: 400;
    color: var(--ink-muted);
  }
  .body {
    display: grid;
    grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr);
    gap: 32px;
  }
  .space {
    display: flex;
    flex-direction: column;
    gap: 6px;
    font-size: 13px;
  }
  .label {
    color: var(--ink-muted);
  }
  .meter {
    width: 100%;
    height: 8px;
    display: block;
  }
  .meter rect {
    fill: var(--ink-body);
  }
  .meter rect.track {
    fill: var(--line);
  }
  .meter rect.full {
    fill: var(--attn);
  }
  .meter.stale {
    opacity: 0.5;
  }
  .meter.unknown {
    border: 1px solid var(--line-strong);
  }
  .facts :global(.inline) {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    color: var(--ink);
  }
  .block {
    display: block;
    font-size: 13px;
  }
  @media (max-width: 760px) {
    .body {
      grid-template-columns: 1fr;
    }
  }
</style>
