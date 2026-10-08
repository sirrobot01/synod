<script lang="ts">
  import type { App } from "./api";
  import { bytes } from "./format";
  import Mark from "./ui/Mark.svelte";
  let { app, included = [] }: { app: App; included?: string[] } = $props();
  const chosen = (item: { source?: string; status: string }) => item.status === "Large" && !!item.source && included.includes(item.source);
</script>
{#if app.definition.issues?.length}<p class="callout">Discovery needs attention: <span class="mono">{app.definition.issues.join(", ")}</span></p>{/if}
<div class="scroll">
  <table>
    <caption class="sr">Paths for {app.definition.name}</caption>
    <thead><tr><th>Path</th><th>Copy</th><th>Size</th><th>Reason</th></tr></thead>
    <tbody>{#each app.definition.paths ?? [] as item}
      <tr><td class="mono wrap">{item.source || item.volume || item.target || item.kind}{#if item.target && item.target !== item.source}<small class="dim">→ {item.target}</small>{/if}</td>
        <td>{#if chosen(item)}<Mark mark={{ glyph: "✓", shape: "square-open", tone: "attn", label: "Included" }} />{:else}<Mark mark={{ glyph: item.status === "Included" || item.status === "Dumped" ? "✓" : "–", shape: item.status === "Never" ? "square-slash" : "square-open", tone: item.status === "Large" ? "attn" : "neutral", label: item.status }} />{/if}</td>
        <td class="mono">{bytes(item.size_known ? (item.size_bytes ?? 0) : undefined)}</td><td>{chosen(item) ? "You included this Large path." : item.reason}</td></tr>
    {:else}<tr><td colspan="4">No data paths were reported.</td></tr>{/each}</tbody>
  </table>
</div>
