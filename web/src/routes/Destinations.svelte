<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type Destination, type DestinationList, type Node } from "../lib/api";
  import { errorView, operationMark, type APIError } from "../lib/state";
  import { bytes, timestamp } from "../lib/format";
  import { follow, path } from "../lib/router.svelte";
  import Panel from "../lib/ui/Panel.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Dialog from "../lib/ui/Dialog.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import DestinationDialog from "../lib/DestinationDialog.svelte";
  import WorkDialog from "../lib/WorkDialog.svelte";
  let { onOperation }: { onOperation: (id: string) => void } = $props();
  let listing = $state<DestinationList | null>(null); let nodes = $state<Node[]>([]);
  let failure = $state<APIError | null>(null); let removalFailure = $state<APIError | null>(null);
  let busy = $state(false); let removing = $state(false); let add = $state(false);
  let remove = $state<Destination | null>(null); let check = $state<Destination | null>(null);
  let now = $state(Date.now()); let readAt = $state(0);
  async function read() {
    if (busy) return; busy = true;
    try { const [d, n] = await Promise.all([api.destinations(), api.nodes()]); listing = d; nodes = n; failure = null; readAt = Date.now(); }
    catch (error) { failure = detail(error); } finally { busy = false; }
  }
  async function confirmRemove() {
    if (!remove) return; removing = true;
    try { await api.removeDestination(remove.id); remove = null; await read(); }
    catch (error) { removalFailure = detail(error); } finally { removing = false; }
  }
  onMount(() => { read(); const timer = setInterval(() => { now = Date.now(); if (!add && !remove && !check) read(); }, 15000); return () => clearInterval(timer); });
</script>
<div class="page">
  <header class="page-head"><div><h1>Destinations</h1><p class="note">Encrypted repositories for your app snapshots and recovery catalog.</p></div><div class="actions"><button onclick={read} disabled={busy}>Refresh</button><button class="primary" onclick={() => add = true} disabled={!listing}>Add destination</button></div></header>
  {#if failure}<ErrorPanel view={errorView(failure)} />{#if listing}<p class="callout hatched">Last read: {timestamp(new Date(readAt).toISOString())}. These reports could not refresh.</p>{/if}{/if}
  {#if !listing && !failure}<p role="status" class="note">Reading destinations…</p>{/if}
  {#each listing?.destinations ?? [] as destination (destination.id)}
    {@const space = listing?.space.find(s => s.destination_id === destination.id)}
    {@const fresh = space?.known && space.observed_at && now - Date.parse(space.observed_at) <= 300000}
    {@const proof = listing?.checks?.find(p => p.destination_id === destination.id)}
    {@const upkeep = listing?.maintenance?.find(m => m.destination_id === destination.id)}
    <Panel title={destination.name} note={destination.location.kind} stale={!!space?.known && !fresh}>
      {#snippet actions()}<button onclick={() => check = destination}>Check integrity</button><button class="quiet" onclick={() => { remove = destination; removalFailure = null; }}>Remove…</button>{/snippet}
      <div class="columns pad">
        <dl class="facts mono"><dt>Location</dt><dd>{destination.location.path || `${destination.location.s3?.endpoint}/${destination.location.s3?.bucket}/${destination.location.s3?.prefix ?? ""}`}</dd>
          {#if destination.node_id}<dt>Device</dt><dd>{nodes.find(n => n.id === destination.node_id)?.name ?? destination.node_id}</dd>{/if}
          {#if destination.location.sftp}<dt>SFTP</dt><dd>{destination.location.sftp.user}@{destination.location.sftp.host}:{destination.location.sftp.port}</dd>{/if}
          <dt>Destination ID</dt><dd>{destination.id}</dd></dl>
        <div class="stack">
          {#if fresh && space}<div><span class="label">Filesystem space</span><p class="mono">{bytes(space.available_bytes)} free of {bytes(space.total_bytes)}</p><meter min="0" max={space.total_bytes} value={space.total_bytes - space.available_bytes} aria-label={`Used filesystem space on ${destination.name}`}></meter><p class="note">{timestamp(space.observed_at)} · shared with other files on this filesystem.</p></div>
          {:else}<div><span class="label">Space</span><p>Unknown</p><p class="note">{space?.observed_at ? `Last report: ${timestamp(space.observed_at)}` : "This destination has no current capacity report."}</p></div>{/if}
          <div><span class="label">Integrity check</span>{#if proof}<p><a href={path("operation", proof.id)} onclick={(e) => follow(e, () => onOperation(proof.id))}><Mark mark={operationMark(proof.state)} /></a></p><span class="note">{timestamp(proof.result.completed_at)}</span>{:else}<p>No check recorded.</p>{/if}</div>
          <div><span class="label">Space reclamation</span>
            {#if !upkeep?.node_id}<p>No device</p><p class="note">Approve a device to remove expired snapshots from this repository.</p>
            {:else}<p>{nodes.find(n => n.id === upkeep.node_id)?.name ?? upkeep.node_id}</p>
              {#if upkeep.last}<p><a href={path("operation", upkeep.last.id)} onclick={(e) => follow(e, () => onOperation(upkeep.last!.id))}><Mark mark={operationMark(upkeep.last.state)} /></a></p>
                <span class="note">{timestamp(upkeep.last.result.completed_at)}{#if upkeep.last.result.maintenance && upkeep.last.result.maintenance !== "completed"} · The first full run on this device starts after a six-hour wait.{/if}</span>
              {:else}<p class="note">No run recorded. This device runs maintenance each day.</p>{/if}
            {/if}</div>
        </div>
      </div>
    </Panel>
  {:else}{#if listing}<div class="panel pad"><h2>No destination yet</h2><p>Add a directory on another Synod device, an SFTP server, an S3-compatible bucket, or a local path.</p></div>{/if}{/each}
  {#if listing?.delivery.length}<Panel title="Settings delivery" note={`Revision ${listing.revision}`}><div class="scroll"><table><thead><tr><th>Device</th><th>Settings</th><th>Last acknowledgement</th></tr></thead><tbody>{#each listing.delivery as delivery}<tr><td>{delivery.node_name}</td><td>{delivery.current ? "Current" : "Waiting for device"}</td><td class="mono">{timestamp(delivery.acknowledged_at)}</td></tr>{/each}</tbody></table></div></Panel>{/if}
</div>
{#if add}<DestinationDialog {nodes} onClose={() => add = false} onSaved={() => { add = false; read(); }} />{/if}
{#if remove}<Dialog label="Remove destination" title={`Remove ${remove.name}`} danger placement="centre" onClose={() => { if (!removing) remove = null; }}>
  <p>Remove this destination from Synod. Backups that depend on it will stop. This keeps the repository files and its saved snapshots.</p>
  {#if removalFailure}<ErrorPanel view={errorView(removalFailure)} />{/if}
  <div class="actions"><button class="destructive" onclick={confirmRemove} disabled={removing}>{removing ? "Removing…" : "Remove destination"}</button></div>
</Dialog>{/if}
{#if check && listing}<WorkDialog kind="integrity" destinationID={check.id} destinations={listing.destinations.filter(d => d.id === check!.id)} {nodes} onClose={() => check = null} onAccepted={onOperation} />{/if}
<style>meter { width:100%; height:10px; accent-color:var(--active); }</style>
