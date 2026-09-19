<script lang="ts">
  import type { Snippet } from "svelte";

  // A panel whose source is expired hatches its header strip only. Hatching stays
  // legible by being rare, so it never repeats down every cell of the panel.
  let {
    title,
    note = "",
    stale = false,
    tone = "",
    foot = undefined,
    actions = undefined,
    children,
  }: {
    title: string;
    note?: string;
    stale?: boolean;
    tone?: string;
    foot?: Snippet;
    actions?: Snippet;
    children: Snippet;
  } = $props();
</script>

<section class="panel" style={tone ? `border-color: var(--${tone})` : undefined}>
  <header class="panel-head" class:hatched={stale}>
    <h2 class="eyebrow">{title}</h2>
    {#if note}<span class="note">{note}</span>{/if}
    {#if actions}<span class="actions">{@render actions()}</span>{/if}
  </header>
  {@render children()}
  {#if foot}
    <div class="panel-foot">{@render foot()}</div>
  {/if}
</section>

<style>
  h2 {
    margin: 0;
    font-weight: 400;
    text-transform: uppercase;
  }
  .actions {
    margin-left: auto;
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
  }
</style>
