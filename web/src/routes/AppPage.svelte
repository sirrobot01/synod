<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type DestinationList, type FileHeader, type Node, type ProtectionPlan, type Snapshot } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import { bytes, limits, protectionMark } from "../lib/format";
  import { ago, blocked, failureText, nextRun, newest, nodeName, stateWords, when } from "../lib/model";
  import { go } from "../lib/router.svelte";
  import Link from "../lib/ui/Link.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import AppPaths from "../lib/AppPaths.svelte";
  import EditSchedule from "../lib/EditSchedule.svelte";
  import WorkDialog from "../lib/WorkDialog.svelte";
  import RestoreDialog from "../lib/RestoreDialog.svelte";
  import FilesDialog from "../lib/FilesDialog.svelte";

  let { id }: { id: string } = $props();

  let plan = $state<ProtectionPlan | null>(null);
  let listing = $state<DestinationList | null>(null);
  let nodes = $state<Node[]>([]);
  let failure = $state<APIError | null>(null);
  let copies = $state<Snapshot[]>([]);
  let header = $state<FileHeader | null>(null);
  let copiesFailure = $state<APIError | null>(null);
  let copiesRead = $state(false);
  let reading = $state(false);
  let via = $state("");
  // A cell of the recovery matrix opens this page narrowed to one storage.
  let storage = $state(new URLSearchParams(window.location.search).get("storage") ?? "");
  let now = $state(Date.now());
  let modal = $state<"" | "restore" | "files" | "schedule" | "backup" | "drill">("");
  let chosen = $state<Snapshot | undefined>(undefined);

  let item = $derived(plan?.apps.find((a) => a.app.id === id));
  let destinations = $derived(listing?.destinations ?? []);
  let storageNames = (ids: string[]) => ids.map((d) => destinations.find((x) => x.id === d)?.name ?? "unknown").join(", ");
  let last = $derived(item ? newest(item) : undefined);
  let next = $derived(item ? nextRun(item) : undefined);
  let mark = $derived(item ? protectionMark(item.state) : null);
  let selected = $derived(item?.targets.filter((t) => t.selected).map((t) => destinations.find((d) => d.id === t.destination_id)?.name).filter(Boolean) ?? []);
  let dumps = $derived((item?.app.definition.containers ?? []).filter((c) => c.database).map((c) => c.database));
  let codes = $derived((item?.limit_codes ?? []));

  async function read() {
    try {
      const [p, d, n] = await Promise.all([api.protection(), api.destinations(), api.nodes()]);
      plan = p; listing = d; nodes = n; failure = null;
    } catch (error) {
      failure = detail(error);
    } finally {
      now = Date.now();
    }
  }

  async function readCopies() {
    if (reading) return;
    reading = true;
    try {
      header = await api.snapshots(id, { via, destination: storage });
      copies = header.snapshots ?? [];
      copiesFailure = null;
      copiesRead = true;
    } catch (error) {
      copiesFailure = detail(error);
    } finally {
      reading = false;
    }
  }

  onMount(() => {
    read().then(() => { if (item) readCopies(); });
    const timer = setInterval(() => { if (!modal) read(); }, 15000);
    return () => clearInterval(timer);
  });

  function open(kind: typeof modal, copy?: Snapshot) {
    chosen = copy;
    modal = kind;
  }
</script>

<div class="page">
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if !plan && !failure}<p class="loading" role="status">Loading</p>{/if}
  {#if plan && !item}
    <div class="empty"><strong>Unknown app</strong><span>It is not in the inventory of any device.</span><Link screen="apps">Apps</Link></div>
  {/if}
  {#if item && mark}
    <header class="page-head">
      <div>
        <h1>{item.app.definition.name}</h1>
        <p class="sub">{item.app.definition.kind === "compose" ? "Compose project" : "Container"} on <Link screen="devices" id={item.app.node_id}>{item.app.node_name}</Link>{#if item.app.definition.project_directory}, <span class="mono">{item.app.definition.project_directory}</span>{/if}</p>
      </div>
      <div class="actions">
        <button type="button" onclick={() => open("backup")} disabled={!item.app.present || !destinations.length}>Back up now</button>
        <button type="button" class="primary" onclick={() => open("restore")} disabled={!copies.length}>Restore…</button>
        <button type="button" onclick={() => open("files")} disabled={!copies.length}>Browse files</button>
        <button type="button" onclick={() => open("drill")} disabled={!copies.length}>Test restore</button>
      </div>
    </header>

    <div class="summary">
      <dl class="facts">
        <dt>Status</dt><dd><Mark {mark} /></dd>
        <dt>Newest</dt><dd>{#if last}<span class="mono">{when(last, now)}</span><span class="dim gap">{ago(last, now)}</span>{:else}<span class="dim">no snapshot</span>{/if}</dd>
        <dt>Tested</dt><dd>{#if item.restore_tested_at}<span class="mono">{when(item.restore_tested_at, now)}</span>{item.last_drill?.node_id ? ` on ${nodeName(nodes, item.last_drill.node_id)}` : ""}{:else}<span class="dim">untested</span>{/if}{#if item.last_drill?.state === "failed"}<span class="gap"><Link screen="operation" id={item.last_drill.id}>latest test failed</Link></span>{/if}</dd>
      </dl>
      <dl class="facts">
        <dt>Schedule</dt><dd>{#if item.enabled}<span class="mono">daily {item.settings.at} {item.settings.time_zone}, keep {item.settings.retention.daily}d {item.settings.retention.weekly}w {item.settings.retention.monthly}m</span>{:else}<span class="dim">not scheduled</span>{/if}<button type="button" class="link gap" onclick={() => open("schedule")}>Edit schedule</button></dd>
        <dt>Storage</dt><dd>{selected.join(", ") || "none"}</dd>
        <dt>Method</dt><dd class="mono">{item.settings.method}{dumps.length ? ` (${dumps.join(", ")})` : ""}</dd>
        {#if item.enabled && next}<dt>Next run</dt><dd class="mono">{when(next, now)}</dd>{/if}
        {#if item.enabled && !item.settings_current}<dt>Device</dt><dd class="dim">has not received the schedule yet</dd>{/if}
      </dl>
    </div>

    {#if item.blocked_code || item.only_on_source || codes.length || item.targets.some((t) => t.failure_code) || item.state === "overdue"}
      <section class="section" aria-labelledby="limits">
        <h2 id="limits">Limits</h2>
        <div class="scroll">
          <table>
            <tbody>
              {#if item.blocked_code}<tr><td class="code">{item.blocked_code}</td><td>{blocked[item.blocked_code] ?? stateWords("blocked").text}</td></tr>{/if}
              {#if item.state === "overdue"}<tr><td class="code">overdue</td><td>{stateWords("overdue").text}</td></tr>{/if}
              {#each item.targets.filter((t) => t.failure_code) as target}<tr><td class="code">{target.failure_code}</td><td>{failureText(target.failure_code)} Storage {destinations.find((d) => d.id === target.destination_id)?.name ?? "unknown"}.{#if target.last_operation_id} <Link screen="operation" id={target.last_operation_id}>Run details</Link>{/if}</td></tr>{/each}
              {#if item.only_on_source}<tr><td class="code">single device</td><td>Every snapshot is on {item.app.node_name}, the host of this app. Add storage on another device.</td></tr>{/if}
              {#each codes as code}<tr><td class="code">{code}</td><td>{limits[code]?.text ?? ""}</td></tr>{/each}
            </tbody>
          </table>
        </div>
      </section>
    {/if}

    <div class="columns">
      <section class="section" aria-labelledby="snapshots">
        <div class="section-head">
          <h2 id="snapshots">Snapshots</h2>
          <form class="filter" onsubmit={(event) => { event.preventDefault(); readCopies(); }}>
            <label><span class="sr">Storage</span>
              <select bind:value={storage} disabled={reading} onchange={readCopies}>
                <option value="">All storage</option>
                {#each destinations as d (d.id)}<option value={d.id}>{d.name}</option>{/each}
              </select>
            </label>
            <label><span class="sr">Read through</span>
              <select bind:value={via} disabled={reading} onchange={readCopies} title="Device that reads the repository">
                <option value="">Any device</option>
                {#each nodes.filter((n) => n.approved_at) as node (node.id)}<option value={node.id}>via {node.name}</option>{/each}
              </select>
            </label>
          </form>
        </div>
        {#if copiesFailure}<ErrorPanel view={errorView(copiesFailure)} />{/if}
        {#if header?.truncated}<p class="callout attn">List truncated. Pick one storage to see all of its snapshots.</p>{/if}
        {#each header?.warning_codes ?? [] as warning}<p class="callout attn">Some storage was unreadable, so the list may be incomplete <span class="code">{warning}</span></p>{/each}
        {#if reading && !copies.length}<p class="loading" role="status">Loading snapshots</p>{/if}
        {#if copies.length}
          <div class="scroll">
            <table class="nowrap">
              <thead><tr><th>Time</th><th>Storage</th><th class="num">Size</th><th><span class="sr">Actions</span></th></tr></thead>
              <tbody>
                {#each copies as copy (copy.id)}
                  <tr>
                    <td class="mono"><button type="button" class="link" onclick={() => open("files", copy)} title="Browse files">{when(copy.completed_at, now)}</button></td>
                    <td>{storageNames(copy.destination_ids)}</td>
                    <td class="num mono">{bytes(copy.file_bytes)}</td>
                    <td class="end"><button type="button" class="small" onclick={() => open("restore", copy)} aria-label={`Restore the snapshot of ${when(copy.completed_at, now)}`}>Restore</button></td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {:else if copiesRead && !copiesFailure}
          <div class="empty">
            <strong>No snapshots{storage ? " in this storage" : ""}</strong>
            {#if destinations.length}<button type="button" onclick={() => open("backup")} disabled={!item.app.present}>Back up now</button>{:else}<Link screen="storage">Add storage</Link>{/if}
          </div>
        {/if}
      </section>

      <section class="section" aria-labelledby="paths">
        <h2 id="paths">Paths</h2>
        <AppPaths app={item.app} included={item.settings.include_large ?? []} />
        {#if item.app.definition.containers?.length}
          <div class="scroll">
            <table>
              <thead><tr><th>Container</th><th>Image</th><th>Platform</th><th>State</th></tr></thead>
              <tbody>{#each item.app.definition.containers as container}<tr><td class="mono">{container.name}</td><td class="mono wrap">{container.image}</td><td class="mono">{container.platform || ""}</td><td>{container.state || ""}</td></tr>{/each}</tbody>
            </table>
          </div>
        {/if}
      </section>
    </div>
  {/if}
</div>

{#if item && listing}
  {#if modal === "restore"}<RestoreDialog {item} {copies} start={chosen} {destinations} {nodes} onClose={() => (modal = "")} onFinished={read} />{/if}
  {#if modal === "files"}<FilesDialog {item} {copies} start={chosen} {destinations} {via} onClose={() => (modal = "")} />{/if}
  {#if modal === "schedule" && plan}<EditSchedule {plan} {item} {destinations} onClose={() => (modal = "")} onSaved={() => { modal = ""; read(); }} />{/if}
  {#if modal === "backup" || modal === "drill"}<WorkDialog kind={modal} app={item} {destinations} {nodes} onClose={() => (modal = "")} onAccepted={(op) => go("operation", op)} />{/if}
{/if}

<style>
  .summary {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1.4fr);
    gap: 32px;
  }
  .summary .facts {
    font-size: 14px;
    gap: 8px 24px;
    align-content: start;
  }
  .gap {
    margin-left: 10px;
  }
  .filter {
    display: flex;
    gap: 8px;
  }
  .filter select {
    min-height: 28px;
    padding: 3px 8px;
    font-size: 13px;
    width: auto;
  }
  .columns {
    grid-template-columns: minmax(0, 1fr) minmax(0, 1.1fr);
  }
  @media (max-width: 860px) {
    .summary,
    .columns {
      grid-template-columns: 1fr;
    }
  }
</style>
