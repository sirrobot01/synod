<script lang="ts">
  import type { App } from "./api";
  import { bytes } from "./format";
  import { paths } from "./model";

  // Every discovered path of one app: what a backup does with it, and why.
  let { app, included = [] }: { app: App; included?: string[] } = $props();
  let rows = $derived(paths(app, included));
</script>

{#if app.definition.issues?.length}<p class="callout attn">Discovery incomplete <span class="code">{app.definition.issues.join(", ")}</span></p>{/if}
<div class="scroll">
  <table>
    <caption class="sr">Paths of {app.definition.name}</caption>
    <thead><tr><th>Status</th><th>Path</th><th class="num">Size</th><th>Reason</th></tr></thead>
    <tbody>
      {#each rows as row}
        <tr>
          <td class="status {row.status}" class:chosen={row.chosen}>{row.status}</td>
          <td class="mono path" title={row.label}>{#each row.label.split("/") as part, index}{index ? "/" : ""}<wbr />{part}{/each}</td>
          <td class="num mono">{row.sizeKnown ? bytes(row.size ?? 0) : ""}</td>
          <td class="dim">{row.reason}</td>
        </tr>
      {:else}
        <tr><td colspan="4" class="dim">No paths reported.</td></tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  /* A path breaks only after a slash, and keeps room before the size column takes it. */
  .path {
    min-width: 24ch;
    overflow-wrap: normal;
    word-break: normal;
  }
  .status {
    white-space: nowrap;
    width: 1%;
  }
  .included { color: var(--ok); }
  .dumped { color: var(--active); }
  .excluded { color: var(--attn); }
  .never { color: var(--ink-dim); }
  .chosen { color: var(--attn); }
</style>
