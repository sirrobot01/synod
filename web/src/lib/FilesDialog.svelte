<script lang="ts">
  import { onMount, untrack } from "svelte";
  import { api, detail, download, type AppPlan, type Destination, type FileHeader, type Snapshot } from "./api";
  import { errorView, type APIError } from "./state";
  import { bytes } from "./format";
  import { when } from "./model";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";
  import Icon from "./ui/Icon.svelte";

  // Opens the saved files of one copy, and downloads a file or a folder from it.
  let { item, copies, start = undefined, destinations, via = "", onClose }: {
    item: AppPlan; copies: Snapshot[]; start?: Snapshot | undefined; destinations: Destination[]; via?: string; onClose: () => void;
  } = $props();

  const now = Date.now();
  let copyID = $state(untrack(() => start?.id ?? copies[0]?.id ?? ""));
  let files = $state<FileHeader | null>(null);
  let listedPath = $state("/");
  let directory = $state("/");
  let browsing = $state(false);
  let downloading = $state(false);
  let fileFailure = $state<APIError | null>(null);
  let downloadFailure = $state<APIError | null>(null);
  let request = 0;
  let controller: AbortController | null = null;
  let copy = $derived(copies.find((c) => c.id === copyID));
  let crumbs = $derived(listedPath === "/" ? [] : listedPath.replace(/^\//, "").split("/"));

  async function browse(target: string) {
    if (!copy) return;
    const token = ++request;
    directory = target; listedPath = target; files = null; fileFailure = null; browsing = true;
    try {
      const result = await api.files(item.app.id, { snapshot: copy.id, destination: copy.destination_ids[0] ?? "", via, path: target });
      if (token === request) files = result;
    } catch (error) {
      if (token === request) fileFailure = detail(error);
    } finally {
      if (token === request) browsing = false;
    }
  }

  async function get(target: string, name: string) {
    if (!copy || downloading) return;
    downloading = true; downloadFailure = null; controller = new AbortController();
    try {
      const result = await download(item.app.id, { snapshot: copy.id, destination: files?.destination_id || copy.destination_ids[0] || "", via, path: target }, controller.signal);
      const url = URL.createObjectURL(result.blob);
      const link = document.createElement("a"); link.href = url; link.download = name + (result.archive ? ".tar" : ""); link.click();
      setTimeout(() => URL.revokeObjectURL(url), 10000);
    } catch (error) {
      if (!controller.signal.aborted) downloadFailure = detail(error);
    } finally {
      downloading = false;
    }
  }

  function child(name: string): string {
    if (files?.path_type && files.path_type !== "directory") return listedPath;
    if (listedPath === "/") return name === "@databases" ? name : "/" + name;
    return listedPath + "/" + name;
  }

  function parent(): string {
    return listedPath.includes("/") ? listedPath.slice(0, listedPath.lastIndexOf("/")) || "/" : "/";
  }

  onMount(() => {
    browse("/");
    return () => { request++; controller?.abort(); };
  });
</script>

<Dialog label={`Files of ${item.app.definition.name}`} title={`Files: ${item.app.definition.name}`} note="Folders download as tar. Limit 64 MiB; use synod file get for more." width={900} onClose={() => { if (!downloading) onClose(); }}>
  <div class="bar">
    <label class="field grow">Snapshot
      <select class="mono" bind:value={copyID} disabled={downloading} onchange={() => browse("/")}>
        {#each copies as c (c.id)}<option value={c.id}>{when(c.completed_at, now)}  {c.destination_ids.map((id) => destinations.find((d) => d.id === id)?.name ?? "unknown").join(", ")}</option>{/each}
      </select>
    </label>
    <form class="go" onsubmit={(event) => { event.preventDefault(); browse(directory); }}>
      <label class="field grow">Path<input class="mono" bind:value={directory} required disabled={browsing || downloading} /></label>
      <button disabled={browsing || downloading}>Open</button>
    </form>
  </div>
  <nav class="crumbs mono" aria-label="Folder">
    <button type="button" class="link" onclick={() => browse("/")} disabled={browsing || downloading} aria-label="All paths">/</button>
    {#each crumbs as part, index}
      {#if index > 0}<span aria-hidden="true">/</span>{/if}
      <button type="button" class="link" disabled={browsing || downloading} onclick={() => browse(listedPath === "/" ? "/" : (crumbs[0] === "@databases" ? "" : "/") + crumbs.slice(0, index + 1).join("/"))}>{part}</button>
    {/each}
  </nav>
  {#if fileFailure}<ErrorPanel view={errorView(fileFailure)} />{/if}
  {#if downloadFailure}<ErrorPanel view={errorView(downloadFailure)} />{/if}
  {#if files?.truncated}<p class="callout attn">List truncated. Type a path to open it.</p>{/if}
  {#each files?.warning_codes ?? [] as warning}<p class="callout attn">Some storage was unreadable <span class="code">{warning}</span></p>{/each}
  {#if browsing}<p class="loading" role="status">Loading</p>{/if}
  {#if files}
    <div class="scroll">
      <table>
        <thead><tr><th>Name</th><th>Type</th><th class="num">Size</th><th><span class="sr">Download</span></th></tr></thead>
        <tbody>
          {#if listedPath !== "/"}
            <tr><td colspan="4"><button type="button" class="link mono" onclick={() => browse(parent())} disabled={downloading}>..</button></td></tr>
          {/if}
          {#each files.entries ?? [] as entry}
            <tr>
              <td class="mono wrap">
                {#if entry.type === "directory"}<button type="button" class="link" onclick={() => browse(child(entry.name))} disabled={downloading}>{entry.name}/</button>{:else}{entry.name}{/if}
                {#if entry.target}<span class="dim"> -&gt; {entry.target}</span>{/if}
              </td>
              <td class="dim">{entry.type}</td>
              <td class="num mono">{entry.type === "file" ? bytes(entry.size) : ""}</td>
              <td class="end">{#if entry.type === "file" || entry.type === "directory"}<button type="button" class="small" disabled={downloading} onclick={() => get(child(entry.name), entry.name)} aria-label={`Download ${entry.name}`} title={`Download ${entry.name}`}><Icon name="download" /></button>{/if}</td>
            </tr>
          {:else}
            <tr><td colspan="4" class="dim">Empty folder.</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
  {#snippet footer()}
    {#if downloading}<button type="button" class="ghost" onclick={() => controller?.abort()}>Cancel download</button>{/if}
    <button type="button" onclick={() => get(listedPath, listedPath.split("/").at(-1) || "copy")} disabled={downloading || browsing || !files}><Icon name="download" />{downloading ? "Downloading" : "Download folder"}</button>
    <button type="button" class="primary" onclick={onClose} disabled={downloading}>Done</button>
  {/snippet}
</Dialog>

<style>
  .bar {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1.4fr);
    gap: 12px;
    align-items: end;
  }
  .go {
    display: flex;
    gap: 8px;
    align-items: flex-end;
  }
  .grow {
    flex: 1;
  }
  .crumbs {
    display: flex;
    flex-wrap: wrap;
    gap: 2px;
    font-size: 13px;
    color: var(--ink-dim);
  }
  @media (max-width: 700px) {
    .bar {
      grid-template-columns: 1fr;
    }
  }
</style>
