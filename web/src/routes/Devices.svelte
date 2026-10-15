<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type Node, type ProtectionPlan } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import { protectionMark, timestamp } from "../lib/format";
  import { deviceState, label, newest, when } from "../lib/model";
  import Link from "../lib/ui/Link.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import AddDevice from "../lib/AddDevice.svelte";
  import RenameDevice from "../lib/RenameDevice.svelte";
  import Icon from "../lib/ui/Icon.svelte";
  import IconButton from "../lib/ui/IconButton.svelte";
  import { toast } from "../lib/toast.svelte";

  let { id }: { id: string } = $props();
  let nodes = $state<Node[]>([]);
  let plan = $state<ProtectionPlan | null>(null);
  let now = $state(Date.now());
  let loaded = $state(false);
  let busy = $state(false);
  let failure = $state<APIError | null>(null);
  let adding = $state(false);
  let renaming = $state<Node | null>(null);
  // An agent on a bridge network reports its container ID as the host name.
  const containerLike = (name: string) => /^[0-9a-f]{12}$/.test(name);
  let node = $derived(nodes.find((n) => n.id === id));
  let appsOf = (nodeID: string) => (plan?.apps ?? []).filter((a) => a.app.node_id === nodeID && a.app.present);

  async function read() {
    if (busy) return;
    busy = true;
    try {
      nodes = await api.nodes();
      loaded = true;
      failure = null;
    } catch (error) {
      failure = detail(error);
    } finally {
      busy = false;
      now = Date.now();
    }
    // The apps are extra facts. A failed read of them never hides the devices.
    try { plan = await api.protection(); } catch { /* The app counts stay empty. */ }
  }
  async function approve(target: Node) {
    busy = true;
    try {
      const approved = await api.approveNode(target.id);
      nodes = nodes.map((value) => (value.id === approved.id ? approved : value));
      failure = null;
      toast.show(`Approved ${label(approved)}`);
    } catch (error) {
      failure = detail(error);
    } finally {
      busy = false;
    }
  }
  onMount(() => {
    read();
    const timer = setInterval(() => { if (!adding && !renaming) read(); }, 15000);
    return () => clearInterval(timer);
  });
</script>

<div class="page">
  <header class="page-head">
    <div>
      <h1>{id && node ? label(node) : "Devices"}</h1>
      {#if id && node}<p class="sub"><Link screen="devices">All devices</Link></p>{:else if loaded}<p class="sub">{nodes.length === 1 ? "1 device" : `${nodes.length} devices`}</p>{/if}
    </div>
    <div class="actions">
      <IconButton icon="refresh" label="Refresh" onclick={read} disabled={busy} />
      {#if id && node}<button type="button" onclick={() => (renaming = node!)}><Icon name="edit" />Rename</button>{/if}
      <button type="button" class="primary" onclick={() => (adding = true)}><Icon name="plus" />Add device</button>
    </div>
  </header>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if !loaded && !failure}<p class="loading" role="status">Loading</p>{/if}

  {#if id && loaded && !node}
    <div class="empty"><strong>Unknown device</strong><span>It may have been removed.</span><Link screen="devices">All devices</Link></div>
  {:else if id && node}
    {@const health = deviceState(node, now)}
    {#if !node.approved_at}
      <div class="callout attn approve">
        <span>{node.architecture ? "Joined and waiting for approval." : "Joined. Waiting for the first report before approval."}</span>
        <button type="button" class="primary" onclick={() => approve(node)} disabled={busy || !node.architecture}>Approve</button>
      </div>
    {/if}
    <dl class="facts">
      <dt>State</dt><dd><Mark mark={health.mark} /></dd>
      <dt>Architecture</dt><dd class="mono">{node.architecture || "not reported"}</dd>
      <dt>Docker</dt><dd class="mono">{node.runtime_version || "not reported"}</dd>
      <dt>Agent</dt><dd class="mono">{node.agent_version || "not reported"}</dd>
      <dt>Last report</dt><dd class="mono">{when(node.last_seen_at, now)}</dd>
      <dt>Joined</dt><dd class="mono">{when(node.enrolled_at, now)}</dd>
      <dt>ID</dt><dd class="mono">{node.id}</dd>
    </dl>
    <section class="section" aria-labelledby="apps">
      <h2 id="apps">Apps</h2>
      {#if appsOf(node.id).length}
        <div class="scroll">
          <table>
            <colgroup><col /><col class="c-status" /><col class="c-time" /></colgroup>
            <thead><tr><th>App</th><th>Status</th><th>Newest snapshot</th></tr></thead>
            <tbody>
              {#each appsOf(node.id) as item (item.app.id)}
                <tr><td><Link screen="apps" id={item.app.id}>{item.app.definition.name}</Link></td><td><Mark mark={protectionMark(item.state)} /></td><td class="mono">{when(newest(item), now, "none")}</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      {:else}
        <p class="note">No apps reported. The device can still hold storage and run restores.</p>
      {/if}
    </section>
    <p class="code">last report {timestamp(node.last_seen_at)}</p>
  {:else if loaded}
    {#if nodes.length === 0}
      <div class="empty"><strong>No devices</strong><span>Run the agent on a Docker host, then approve it here.</span><button type="button" class="primary" onclick={() => (adding = true)}><Icon name="plus" />Add device</button></div>
    {:else}
      <div class="scroll">
        <table class="nowrap">
          <colgroup><col /><col class="c-id" /><col class="c-state" /><col class="c-num" /><col class="c-num" /><col class="c-num" /><col class="c-time" /><col class="c-num" /><col class="c-act" /></colgroup>
          <thead><tr><th>Device</th><th>ID</th><th>State</th><th>Arch</th><th>Docker</th><th>Agent</th><th>Last report</th><th class="num">Apps</th><th class="end"><span class="sr">Actions</span></th></tr></thead>
          <tbody>
            {#each nodes as row (row.id)}
              {@const health = deviceState(row, now)}
              <tr>
                <td class="truncate" title={label(row)}><Link screen="devices" id={row.id} class="name">{label(row)}</Link>{#if containerLike(row.name)}<small>Container ID as name. <button type="button" class="link" onclick={() => (renaming = row)}>Rename</button></small>{/if}</td>
                <td class="mono dim" title={row.id}>{row.id.slice(0, 8)}</td>
                <td><Mark mark={health.mark} /></td>
                <td class="mono">{row.architecture}</td>
                <td class="mono">{row.runtime_version}</td>
                <td class="mono">{row.agent_version}</td>
                <td class="mono">{when(row.last_seen_at, now)}</td>
                <td class="num mono">{appsOf(row.id).length}</td>
                <td class="end"><span class="row-actions">
                  {#if !row.approved_at}<button type="button" class="small" onclick={() => approve(row)} disabled={busy || !row.architecture} title={row.architecture ? "" : "Waiting for the first report"}>Approve</button>{/if}
                  <IconButton icon="edit" label={`Rename ${label(row)}`} onclick={() => (renaming = row)} />
                </span></td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  {/if}
</div>
{#if adding}<AddDevice onClose={() => (adding = false)} onChanged={read} />{/if}
{#if renaming}<RenameDevice node={renaming} onClose={() => (renaming = null)} onRenamed={(renamed) => { nodes = nodes.map((value) => (value.id === renamed.id ? renamed : value)); renaming = null; }} />{/if}

<style>
  .approve {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    flex-wrap: wrap;
  }
  .facts {
    font-size: 14px;
  }
  td :global(.name) {
    color: var(--ink);
    font-weight: 600;
    text-decoration: none;
  }
  td :global(.name:hover) {
    text-decoration: underline;
  }
</style>
