<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, download, type ProtectionPlan, type DestinationList, type Node, type Snapshot, type FileHeader } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import { bytes, timestamp } from "../lib/format";
  import Panel from "../lib/ui/Panel.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import WorkDialog from "../lib/WorkDialog.svelte";
  let { id, onApp, onOperation }: { id: string; onApp: (id: string) => void; onOperation: (id: string) => void } = $props();
  let plan = $state<ProtectionPlan | null>(null); let destinations = $state<DestinationList | null>(null); let nodes = $state<Node[]>([]);
  let snapshots = $state<Snapshot[]>([]); let selected = $state<Snapshot | null>(null); let files = $state<FileHeader | null>(null);
  let listedPath = $state("/"); let listing = $state<FileHeader | null>(null);
  let directory = $state("/"); let via = $state(""); let destination = $state("");
  let busy = $state(false); let loaded = $state(false); let browsing = $state(false); let downloading = $state(false);
  let failure = $state<APIError | null>(null); let fileFailure = $state<APIError | null>(null); let downloadFailure = $state<APIError | null>(null);
  let work = $state<"restore" | "drill" | null>(null);
  let request = 0; let controller: AbortController | null = null;
  let app = $derived(plan?.apps.find(item => item.app.id === id));
  async function read() {
    if (busy) return; busy = true; loaded = false; selected = null; files = null; request++;
    try { const result = await api.snapshots(id, { via, destination }); snapshots = result.snapshots ?? []; listing = result; failure = null; loaded = true; }
    catch (error) { failure = detail(error); snapshots = []; }
    finally { busy = false; }
  }
  onMount(() => {
    let active = true;
    (async () => {
      try { const [p, d, n] = await Promise.all([api.protection(), api.destinations(), api.nodes()]); if (!active) return; plan = p; destinations = d; nodes = n; if (id) await read(); }
      catch (error) { if (active) failure = detail(error); }
    })();
    return () => { active = false; request++; controller?.abort(); };
  });
  async function browse(snapshot: Snapshot, path: string) {
    const token = ++request;
    selected = snapshot; directory = path; listedPath = path; files = null; fileFailure = null; browsing = true;
    try {
      const result = await api.files(id, { snapshot: snapshot.id, destination: destination || snapshot.destination_ids[0] || "", via, path });
      if (token === request) files = result;
    } catch (error) { if (token === request) fileFailure = detail(error); }
    finally { if (token === request) browsing = false; }
  }
  async function get(path: string, name: string) {
    if (!selected || downloading) return;
    downloading = true; downloadFailure = null; controller = new AbortController();
    try {
      const result = await download(id, { snapshot: selected.id, destination: files?.destination_id || destination || selected.destination_ids[0] || "", via, path }, controller.signal);
      const url = URL.createObjectURL(result.blob);
      const link = document.createElement("a"); link.href = url; link.download = name + (result.archive ? ".tar" : ""); link.click();
      setTimeout(() => URL.revokeObjectURL(url), 10000);
    } catch (error) { if (!controller.signal.aborted) downloadFailure = detail(error); }
    finally { downloading = false; }
  }
  function child(name: string): string {
    if (files?.path_type && files.path_type !== "directory") return listedPath;
    if (listedPath === "/") return name === "@databases" ? name : "/" + name;
    return listedPath + "/" + name;
  }
</script>
<div class="page">
  <header class="page-head"><div><h1>Snapshots & restore</h1><p class="note">Read the saved copies of an app. Restore the app or get a file.</p></div></header>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if !plan && !failure}<p role="status" class="note">Reading apps and destinations…</p>{/if}
  {#if plan}
    <form class="panel pad columns" onsubmit={(event) => { event.preventDefault(); read(); }}>
      <label>App<select value={id} onchange={(e) => onApp(e.currentTarget.value)} disabled={downloading}><option value="">Select an app</option>{#each plan.apps as item}<option value={item.app.id}>{item.app.definition.name} · {item.app.node_name}</option>{/each}</select></label>
      <label>Read through device<select bind:value={via} disabled={busy || downloading} onchange={() => { selected = null; files = null; loaded = false; snapshots = []; request++; }}><option value="">Automatic · available agent</option>{#each nodes.filter(n => n.approved_at) as node}<option value={node.id}>{node.name} · {node.architecture}</option>{/each}</select></label>
      <label>Destination<select bind:value={destination} disabled={busy || downloading} onchange={() => { selected = null; files = null; loaded = false; snapshots = []; request++; }}><option value="">All accessible destinations</option>{#each destinations?.destinations ?? [] as d}<option value={d.id}>{d.name}</option>{/each}</select></label>
      <div class="actions"><button disabled={!id || busy || downloading}>{busy ? "Reading…" : "Read snapshots"}</button></div>
    </form>
  {/if}
  {#if app}
    {#if listing?.truncated}<p class="callout">The snapshot list is incomplete. Select a destination to narrow it.</p>{/if}
    {#each listing?.warning_codes ?? [] as warning}<p class="callout">Some repositories could not be read: <span class="mono">{warning}</span>. Other copies may be available.</p>{/each}
    <Panel title={app.app.definition.name} note="A saved copy is separate from a tested restore.">
      <div class="scroll"><table><thead><tr><th>Snapshot time</th><th>Source</th><th>Method / size</th><th>Copies</th><th>Actions</th></tr></thead><tbody>
        {#each snapshots as snapshot (snapshot.id)}<tr class:selected={selected?.id === snapshot.id}>
          <td><button class="link" onclick={() => browse(snapshot, "/")} disabled={downloading}>{timestamp(snapshot.completed_at)}</button><small class="mono wrap">{snapshot.id}</small></td><td>{snapshot.source_node_name}<small class="mono">{snapshot.architecture}</small></td><td>{snapshot.method}<small class="mono">{bytes(snapshot.file_bytes)}</small></td><td>{snapshot.destination_ids.map(id => destinations?.destinations.find(d => d.id === id)?.name ?? id).join(", ")}</td>
          <td><button onclick={() => { selected = snapshot; files = null; fileFailure = null; request++; work = "restore"; }} disabled={downloading}>Restore…</button></td>
        </tr>{:else}<tr><td colspan="5">{busy ? "Reading repositories…" : loaded ? "No saved snapshots were found on the selected destinations." : "Read snapshots to list saved copies."}</td></tr>{/each}
      </tbody></table></div>
    </Panel>
  {/if}
  {#if selected}
    <Panel title="Selected snapshot" note={timestamp(selected.completed_at)}>
      {#snippet actions()}<button onclick={() => work = "drill"} disabled={downloading}>Test restore</button><button class="primary" onclick={() => work = "restore"} disabled={downloading}>Restore app…</button>{/snippet}
      <div class="pad stack"><dl class="facts mono"><dt>Snapshot ID</dt><dd>{selected.id}</dd><dt>Saved paths</dt><dd>{(selected.paths ?? []).join(", ") || "None"}</dd><dt>Database dumps</dt><dd>{selected.databases?.join(", ") || "None"}</dd></dl>
        {#if selected.exclusions?.length}<details><summary>Excluded paths · {selected.exclusions.length}</summary><ul>{#each selected.exclusions as excluded}<li><span class="mono">{excluded.source}</span> · {excluded.reason}</li>{/each}</ul></details>{/if}
        <p class="note">A restore creates the saved app. A drill starts an isolated copy to test it.</p>
      </div>
    </Panel>
    <Panel title="Saved files" note="Downloads up to 64 MiB. Directories download as tar archives.">
      <div class="pad stack">
        <form class="actions" onsubmit={(event) => { event.preventDefault(); if (selected) browse(selected, directory); }}>
          <label>Saved path<input class="mono" bind:value={directory} required disabled={browsing || downloading} /></label>
          <button disabled={browsing || downloading}>Open path</button>
          <button type="button" disabled={browsing || downloading || directory === "/"} onclick={() => browse(selected!, directory.includes("/") ? directory.slice(0, directory.lastIndexOf("/")) || "/" : "/")}>Up</button>
          <button type="button" onclick={() => get(listedPath, listedPath.split("/").at(-1) || "snapshot")} disabled={downloading || browsing || !files}>{downloading ? "Downloading…" : "Download this path"}</button>
          {#if downloading}<button type="button" onclick={() => controller?.abort()}>Cancel download</button>{/if}
        </form>
        {#if fileFailure}<ErrorPanel view={errorView(fileFailure)} />{/if}
        {#if downloadFailure}<ErrorPanel view={errorView(downloadFailure)} />{/if}
        {#if files?.truncated}<p class="callout">This directory has more entries than the list can show. Enter a saved path above to open it.</p>{/if}
        {#each files?.warning_codes ?? [] as warning}<p class="callout mono">{warning}</p>{/each}
        {#if browsing}<p role="status" class="note">Reading saved paths…</p>{/if}
        {#if files}<div class="scroll"><table><thead><tr><th>Name</th><th>Type</th><th>Size</th><th>Action</th></tr></thead><tbody>
          {#each files.entries ?? [] as entry}<tr><td class="mono wrap">{#if entry.type === "directory"}<button class="link" onclick={() => browse(selected!, child(entry.name))} disabled={downloading}>{entry.name}/</button>{:else}{entry.name}{/if}{#if entry.target}<small>→ {entry.target}</small>{/if}</td><td>{entry.type}</td><td class="mono">{entry.type === "file" ? bytes(entry.size) : "—"}</td><td>{#if entry.type === "file" || entry.type === "directory"}<button disabled={downloading} onclick={() => get(child(entry.name), entry.name)}>Download</button>{/if}</td></tr>
          {:else}<tr><td colspan="4">This directory is empty.</td></tr>{/each}
        </tbody></table></div>{/if}
        <p class="note">For larger downloads, use <code>synod file get</code> with this app and snapshot ID.</p>
      </div>
    </Panel>
  {/if}
</div>
{#if work && app && selected && destinations}<WorkDialog kind={work} {app} snapshot={selected} destinations={destinations.destinations} {nodes} onClose={() => work = null} onAccepted={onOperation} />{/if}
<style>.link { border:0; padding:0; color:var(--link); text-decoration:underline; text-underline-offset:2px; text-align:left; } .selected { background:var(--head); }</style>
