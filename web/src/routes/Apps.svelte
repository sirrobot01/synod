<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type DestinationList, type Node, type ProtectionPlan } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import { protectionMark } from "../lib/format";
  import { newest, nodeName, tested, when } from "../lib/model";
  import Link from "../lib/ui/Link.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Icon from "../lib/ui/Icon.svelte";
  import IconButton from "../lib/ui/IconButton.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import ScheduleBackups from "../lib/ScheduleBackups.svelte";

  let plan = $state<ProtectionPlan | null>(null);
  let listing = $state<DestinationList | null>(null);
  let nodes = $state<Node[]>([]);
  let failure = $state<APIError | null>(null);
  let busy = $state(false);
  let readAt = $state(0);
  let now = $state(Date.now());
  let filter = $state("");
  let filterInput = $state<HTMLInputElement>();
  let scheduling = $state(false);

  let shown = $derived(plan?.apps.filter((item) => `${item.app.definition.name} ${item.app.node_name} ${protectionMark(item.state).label}`.toLowerCase().includes(filter.toLowerCase())) ?? []);
  let scheduled = $derived(plan?.apps.filter((item) => item.enabled).length ?? 0);

  async function read() {
    if (busy) return;
    busy = true;
    try {
      const [p, d, n] = await Promise.all([api.protection(), api.destinations(), api.nodes()]);
      plan = p; listing = d; nodes = n; failure = null; readAt = Date.now();
    } catch (error) {
      failure = detail(error);
    } finally {
      busy = false;
      now = Date.now();
    }
  }
  onMount(() => {
    read();
    const timer = setInterval(() => { if (!scheduling) read(); }, 15000);
    return () => clearInterval(timer);
  });
  function shortcut(event: KeyboardEvent) {
    if (event.key === "/" && !scheduling && !(event.target instanceof HTMLElement && event.target.closest("input, textarea, select, [contenteditable]"))) {
      event.preventDefault();
      filterInput?.focus();
    }
  }
</script>

<svelte:window onkeydown={shortcut} />
<div class="page">
  <header class="page-head">
    <div>
      <h1>Apps</h1>
      {#if plan}<p class="sub">{scheduled} of {plan.apps.length} scheduled{plan.auto_protect ? ". New apps are scheduled automatically." : ""}</p>{/if}
    </div>
    <div class="actions">
      <IconButton icon="refresh" label="Refresh" onclick={read} disabled={busy} />
      <button type="button" class="primary" onclick={() => (scheduling = true)} disabled={!plan || !listing?.destinations.length}>Schedule backups</button>
    </div>
  </header>
  {#if failure}
    <ErrorPanel view={errorView(failure)} />
    {#if plan}<p class="callout attn hatched">Not refreshed. Showing data from {when(new Date(readAt).toISOString(), now)}.</p>{/if}
  {/if}
  {#if !plan && !failure}<p class="loading" role="status">Loading</p>{/if}
  {#if plan}
    {#if !listing?.destinations.length}<p class="callout">No storage yet. <Link screen="storage">Add storage</Link> before you schedule backups.</p>{/if}
    {#if plan.apps.length === 0}
      <div class="empty"><strong>No apps found</strong><span>Approved devices report their Compose projects and containers within a minute.</span><Link screen="devices">Devices</Link></div>
    {:else}
      <section class="section">
        <label class="search"><span class="sr">Filter apps</span><Icon name="search" /><input type="search" placeholder="Filter by app, device, or status" bind:value={filter} bind:this={filterInput} aria-keyshortcuts="/" /></label>
        <div class="scroll">
          <table>
            <colgroup><col /><col class="c-device" /><col class="c-status" /><col class="c-time" /><col class="c-result" /><col class="c-time" /></colgroup>
            <thead><tr><th>App</th><th>Device</th><th>Status</th><th>Newest snapshot</th><th>Restore test</th><th>Schedule</th></tr></thead>
            <tbody>
              {#each shown as item (item.app.id)}
                {@const last = newest(item)}
                {@const test = tested(item, now)}
                {@const codes = (item.limit_codes ?? []).filter((c) => c !== "restore_unproven")}
                <tr>
                  <td><Link screen="apps" id={item.app.id} class="name">{item.app.definition.name}</Link>{#if !item.app.present}<small>not in last scan</small>{/if}</td>
                  <td>{nodeName(nodes, item.app.node_id)}</td>
                  <td>
                    <Mark mark={protectionMark(item.state)} />
                    {#if codes.length}<small class="code">{codes.join(", ")}</small>{/if}
                  </td>
                  <td class="mono" class:dim={!last}>{last ? when(last, now) : "none"}</td>
                  <td>{#if test.mark}<span class="cell"><Mark mark={test.mark} shapeOnly title={test.title} /><span class="mono">{test.value}</span></span>{/if}{#if item.last_drill?.state === "failed"}<small><Link screen="operation" id={item.last_drill.id}>Failed test</Link></small>{/if}</td>
                  <td class="mono">{item.enabled ? `${item.settings.at} ${item.settings.time_zone}` : ""}{#if !item.enabled}<span class="dim">not scheduled</span>{/if}</td>
                </tr>
              {:else}
                <tr><td colspan="6" class="dim">No apps match.</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>
    {/if}
  {/if}
</div>
{#if scheduling && plan && listing}<ScheduleBackups {plan} destinations={listing.destinations} {nodes} onClose={() => (scheduling = false)} onSaved={() => { scheduling = false; read(); }} />{/if}

<style>
  .search {
    max-width: 360px;
  }
  td :global(.name) {
    color: var(--ink);
    font-weight: 600;
    text-decoration: none;
  }
  td :global(.name:hover) {
    text-decoration: underline;
  }
  .cell {
    display: inline-flex;
    gap: 8px;
    align-items: center;
  }
  td small.code {
    display: block;
  }
</style>
