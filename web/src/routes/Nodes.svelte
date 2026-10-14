<script lang="ts">
  import { onMount } from "svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import { api, detail, type Node } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import { follow, path } from "../lib/router.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import AddDevice from "../lib/AddDevice.svelte";
  let { id, onOpen }: { id: string; onOpen: (id: string) => void } = $props();
  let nodes = $state<Node[]>([]);
  let now = $state(Date.now());
  let loaded = $state(false);
  let busy = $state(false);
  let failure = $state<APIError | null>(null);
  let adding = $state(false);
  let shown = $derived(id === "" ? nodes : nodes.filter(node => node.id === id));

  async function read() {
    if (busy) return;
    busy = true;
    try { nodes = await api.nodes(); loaded = true; failure = null; }
    catch (error) { failure = detail(error); }
    finally { busy = false; }
  }
  async function approve(node: Node) {
    busy = true;
    try {
      const approved = await api.approveNode(node.id);
      nodes = nodes.map(value => value.id === approved.id ? approved : value);
      failure = null;
    } catch (error) { failure = detail(error); }
    finally { busy = false; }
  }
  onMount(() => { read(); const timer = setInterval(() => { now = Date.now(); read(); }, 15000); return () => clearInterval(timer); });
</script>
<header>
  <h1>Devices</h1>
  <p class="note">Joined devices and their last reported facts. A device can be offline between backups.</p>
  <div class="actions">
    <button class="primary" onclick={() => (adding = true)}>Add device</button>
    <button onclick={read} disabled={busy}>{busy ? "Reading" : "Refresh"}</button>
  </div>
  {#if id !== ""}<a href={path("nodes")} onclick={(event) => follow(event, () => onOpen(""))}>All devices</a>{/if}
</header>
{#if adding}<AddDevice onClose={() => (adding = false)} onChanged={read} />{/if}
{#if failure}<ErrorPanel view={errorView(failure)} />{/if}
{#each shown as node (node.id)}
  <article class="panel">
    <Mark mark={node.last_seen_at && now - Date.parse(node.last_seen_at) <= 120000 ? { glyph: "✓", shape: "circle", tone: "ok", label: "Recent report" } : { glyph: "?", shape: "circle-dashed", tone: "neutral", label: "No recent report" }} />
    <h2><a href={path("nodes", node.id)} onclick={(event) => follow(event, () => onOpen(node.id))}>{node.name || node.id}</a></h2>
    <dl class="mono">
      <dt>Device ID</dt><dd>{node.id}</dd>
      <dt>Architecture</dt><dd>{node.architecture || "Not reported"}</dd>
      <dt>Docker</dt><dd>{node.runtime_version || "Not reported"}</dd>
      <dt>Agent</dt><dd>{node.agent_version || "Not reported"}</dd>
      <dt>Last seen</dt><dd>{node.last_seen_at ?? "Never connected"}</dd>
      <dt>Approval</dt><dd>{node.approved_at ? "Approved" : "Pending approval"}</dd>
    </dl>
    {#if !node.approved_at}<button onclick={() => approve(node)} disabled={busy || !node.architecture}>Approve</button>{/if}
  </article>
{:else}
  {#if loaded}<p class="note">{id === "" ? "No devices have joined yet." : "This device was not found."}</p>{/if}
{/each}
<style>
  .actions { display:flex; gap:8px; }
  header { display:flex; align-items:start; flex-direction:column; gap:10px; margin-bottom:22px; }
  article { padding:20px; margin-bottom:16px; }
  h2 { font-size:16px; }
  dl { display:grid; grid-template-columns:110px 1fr; gap:8px; font-size:12px; }
  dt { color:var(--ink-muted); }
  dd { margin:0; overflow-wrap:anywhere; }
</style>
