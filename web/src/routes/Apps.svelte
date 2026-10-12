<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type ProtectionPlan, type DestinationList, type Node, type AppPlan } from "../lib/api";
  import { errorView, operationMark, type APIError } from "../lib/state";
  import { bytes, duration, timestamp, protectionMark } from "../lib/format";
  import { follow, path, type ScreenName } from "../lib/router.svelte";
  import Panel from "../lib/ui/Panel.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import AppPaths from "../lib/AppPaths.svelte";
  import ProtectionDialog from "../lib/ProtectionDialog.svelte";
  import WorkDialog from "../lib/WorkDialog.svelte";
  let { id = "", go }: { id?: string; go: (screen: ScreenName, id?: string) => void } = $props();
  let plan = $state<ProtectionPlan | null>(null);
  let destinations = $state<DestinationList | null>(null);
  let nodes = $state<Node[]>([]);
  let failure = $state<APIError | null>(null);
  let busy = $state(false);
  let readAt = $state(0);
  let now = $state(Date.now());
  let filter = $state("");
  let filterInput = $state<HTMLInputElement>();
  let selection = $state<AppPlan[] | null>(null);
  let all = $state(false);
  let backup = $state<AppPlan | null>(null);
  let item = $derived(plan?.apps.find(item => item.app.id === id));
  let shown = $derived(plan?.apps.filter(item => `${item.app.definition.name} ${item.app.node_name} ${item.state}`.toLowerCase().includes(filter.toLowerCase())) ?? []);
  let protectedCount = $derived(plan?.apps.filter(item => item.enabled).length ?? 0);
  async function read() {
    if (busy) return;
    busy = true;
    try {
      const [p, d, n] = await Promise.all([api.protection(), api.destinations(), api.nodes()]);
      plan = p; destinations = d; nodes = n; failure = null; readAt = Date.now();
    } catch (error) { failure = detail(error); }
    finally { busy = false; }
  }
  function protect(items: AppPlan[], every: boolean) { selection = items; all = every; }
  onMount(() => { read(); const timer = setInterval(() => { now = Date.now(); if (!selection && !backup) read(); }, 15000); return () => clearInterval(timer); });
  function shortcut(event: KeyboardEvent) {
    if (event.key === "/" && !selection && !backup && !(event.target instanceof HTMLElement && event.target.closest("input, textarea, select, [contenteditable]"))) { event.preventDefault(); filterInput?.focus(); }
  }
</script>
<svelte:window onkeydown={shortcut} />
<div class="page">
  <header class="page-head">
    <div><h1>{id ? (item?.app.definition.name ?? "App") : "Apps"}</h1><p class="note">{id ? "Backup coverage, saved copies, and restore tests." : "Your Docker apps. See what is protected and what you can restore."}</p></div>
    <div class="actions"><button onclick={read} disabled={busy}>{busy ? "Reading…" : "Refresh"}</button>
      {#if plan && !id}<button class="primary" onclick={() => protect(plan!.apps, true)} disabled={!destinations?.destinations.length}>Protect all</button>{/if}
    </div>
  </header>
  {#if failure}<ErrorPanel view={errorView(failure)} />{#if plan}<p class="callout hatched">This view could not refresh. Last read: {timestamp(new Date(readAt).toISOString())}.</p>{/if}{/if}
  {#if !plan && !failure}<p class="note" role="status">Reading the protection plan…</p>{/if}
  {#if plan && !id}
    <div class="metrics">
      <div class="metric"><span class="label">Scheduled apps</span><strong class="mono">{protectedCount} / {plan.apps.length}</strong><span class="note">Protection is {plan.auto_protect ? "automatic for new apps" : "off for new apps"}.</span></div>
      <div class="metric"><span class="label">With a saved copy</span><strong class="mono">{plan.apps.filter(item => item.data_loss_window_seconds !== undefined).length}</strong><span class="note">Only successful snapshots count.</span></div>
      <div class="metric"><span class="label">Restore tested</span><strong class="mono">{plan.apps.filter(item => item.restore_tested_at).length}</strong><span class="note">Check each app for its last test date.</span></div>
    </div>
    {#if !destinations?.destinations.length}<div class="callout">Add a destination to start protecting apps. <a href={path("destinations")} onclick={(e) => follow(e, () => go("destinations"))}>Add a destination →</a></div>{/if}
    <label class="actions">Find an app <input type="search" placeholder="App, device, or state" bind:value={filter} bind:this={filterInput} aria-keyshortcuts="/" /></label>
    <Panel title="All apps" note={`${plan.apps.length} discovered`} stale={failure !== null}>
      <div class="scroll"><table><thead><tr><th>App / device</th><th>Backup state</th><th>Data-loss window</th><th>Restore test</th></tr></thead><tbody>
        {#each shown as row (row.app.id)}<tr>
          <td><a class="mono" href={path("apps", row.app.id)} onclick={(e) => follow(e, () => go("apps", row.app.id))}>{row.app.definition.name}</a><small class="dim">{row.app.node_name} · {row.app.definition.kind}</small>{#if !row.app.present}<small>Not in last scan</small>{/if}</td>
          <td><Mark mark={protectionMark(row.state)} />{#if row.limit_codes?.includes("app_consistency_unverified")}<small>App consistency unverified</small>{/if}{#if row.only_on_source}<small>Only on source device</small>{/if}{#if !row.settings_current && row.enabled}<small>Settings await the device</small>{/if}</td>
          <td class="mono">{duration(row.data_loss_window_seconds === undefined ? undefined : row.data_loss_window_seconds + Math.max(0, (now - readAt) / 1000))}</td>
          <td>{row.restore_tested_at ? `Tested ${timestamp(row.restore_tested_at)}` : "Not tested"}{#if row.last_drill?.state === "failed"}<small><a href={path("operation", row.last_drill.id)} onclick={(e) => follow(e, () => go("operation", row.last_drill!.id))}>Latest drill failed</a></small>{/if}</td>
        </tr>{:else}<tr><td colspan="4">{plan.apps.length ? "No apps match this filter." : "No apps discovered yet. Join and approve a device with Docker apps."}</td></tr>{/each}
      </tbody></table></div>
    </Panel>
    <p class="note">Data-loss window is the time since the last good snapshot. An offline device can still have good backups.</p>
  {:else if item}
    <div class="actions"><button class="primary" onclick={() => go("snapshots", id)}>Snapshots & restore</button><button onclick={() => protect([item!], false)}>Edit protection</button><button onclick={() => backup = item!} disabled={!item.app.present || !destinations?.destinations.length}>Back up now</button></div>
    <div class="metrics">
      <div class="metric"><span class="label">Backup state</span><Mark mark={protectionMark(item.state)} /><span class="note">{item.blocked_code || (item.enabled ? "Scheduled daily" : "Scheduled backups are off")}</span></div>
      <div class="metric"><span class="label">Data-loss window</span><strong class="mono">{duration(item.data_loss_window_seconds === undefined ? undefined : item.data_loss_window_seconds + Math.max(0, (now - readAt) / 1000))}</strong></div>
      <div class="metric"><span class="label">Restore test</span><span>{item.restore_tested_at ? `Restore tested on ${timestamp(item.restore_tested_at)}` : "Not tested yet"}</span>{#if item.last_drill}<a href={path("operation", item.last_drill.id)} onclick={(e) => follow(e, () => go("operation", item!.last_drill!.id))}><Mark mark={operationMark(item.last_drill.state)} /></a>{/if}</div>
    </div>
    {#if item.only_on_source}<p class="callout">The only backup copies are on this app’s device. Add a destination on another device to cover device loss.</p>{/if}
    {#if item.limit_codes?.includes("app_consistency_unverified")}<p class="callout">Full protection is unavailable. The last copy did not read all files and databases of this app from one filesystem snapshot. Put the app and database data on one btrfs subvolume, ZFS dataset, or LVM volume for full protection. Synod will not pause writes to gain full-protection status.</p>{/if}
    <div class="columns">
      <Panel title="Protection settings"><dl class="facts mono pad"><dt>Daily time</dt><dd>{item.settings.at} · {item.settings.time_zone}</dd><dt>Method</dt><dd>{item.settings.method}</dd><dt>Keep</dt><dd>{item.settings.retention.daily} daily · {item.settings.retention.weekly} weekly · {item.settings.retention.monthly} monthly</dd><dt>Device settings</dt><dd>{item.settings_current ? "Current" : "Awaiting acknowledgement"}</dd></dl></Panel>
      <Panel title="Source device"><dl class="facts mono pad"><dt>Device</dt><dd><a href={path("nodes", item.app.node_id)} onclick={(e) => follow(e, () => go("nodes", item!.app.node_id))}>{item.app.node_name}</a></dd><dt>Last discovery</dt><dd>{timestamp(item.app.observed_at)}</dd><dt>Present</dt><dd>{item.app.present ? "Yes, at last discovery" : "Not in last scan"}</dd><dt>App ID</dt><dd>{item.app.id}</dd></dl></Panel>
    </div>
    <Panel title="Copies by destination"><div class="scroll"><table><thead><tr><th>Destination</th><th>State</th><th>Last good snapshot</th><th>Next / current due slot</th></tr></thead><tbody>
      {#each item.targets as target}<tr><td>{destinations?.destinations.find(d => d.id === target.destination_id)?.name ?? target.destination_id}<small>{target.selected ? "Selected" : "Retained, no new backups"}</small></td><td><Mark mark={protectionMark(target.state)} />{#if target.last_operation_id}<small><a href={path("operation", target.last_operation_id)} onclick={(e) => follow(e, () => go("operation", target.last_operation_id))}>View activity</a></small>{/if}{#if target.failure_code}<small class="mono">{target.failure_code}</small>{/if}</td><td>{timestamp(target.last_good?.completed_at)}{#if target.last_good}<small>{bytes(target.last_good.file_bytes)} · downtime {target.last_good.downtime_ms ?? 0} ms</small>{/if}</td><td>{timestamp(target.due_at)}</td></tr>
      {:else}<tr><td colspan="4">No destination selected.</td></tr>{/each}
    </tbody></table></div></Panel>
    <Panel title="Paths" note="Included and Dumped are copied. Large and Never are excluded."><AppPaths app={item.app} /></Panel>
    <Panel title="Containers at last discovery"><div class="scroll"><table><thead><tr><th>Container</th><th>Image</th><th>Platform</th><th>Last state</th></tr></thead><tbody>{#each item.app.definition.containers ?? [] as container}<tr><td class="mono">{container.name}</td><td class="mono wrap">{container.image}</td><td class="mono">{container.platform || "Unknown"}</td><td>{container.state || "Unknown"}</td></tr>{/each}</tbody></table></div></Panel>
  {:else if id && plan}<p class="callout">This app was not found. <a href={path("apps")}>Return to apps.</a></p>{/if}
</div>
{#if selection && plan && destinations}<ProtectionDialog {plan} apps={selection} destinations={destinations.destinations} {all} onClose={() => selection = null} onSaved={() => { selection = null; read(); }} />{/if}
{#if backup && destinations}<WorkDialog kind="backup" app={backup} destinations={destinations.destinations} {nodes} onClose={() => backup = null} onAccepted={(id) => go("operation", id)} />{/if}
