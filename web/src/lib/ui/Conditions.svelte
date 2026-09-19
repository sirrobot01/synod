<script lang="ts">
  import { age, viewConditions, type Condition } from "../state";
  import Mark from "./Mark.svelte";

  // Fixed order, so position alone identifies a condition. The third mark is
  // always replication, whether it is true, false, or unknown. Never summed.
  let {
    reported,
    now = new Date(),
    compact = false,
  }: { reported: readonly Condition[]; now?: Date; compact?: boolean } = $props();

  let views = $derived(viewConditions(reported, now));

  // Two counts, never one. A failure and an unknown are different facts, and a
  // condition the server does not report at all is a third.
  let summary = $derived.by(() => {
    const parts: string[] = [];
    const yes = views.filter((view) => view.display === "true").length;
    const no = views.filter((view) => view.display === "false").length;
    const unknown = views.filter((view) => view.display === "unknown").length;
    if (yes > 0) parts.push(`${yes} true`);
    if (no > 0) parts.push(`${no} false`);
    if (unknown > 0) parts.push(`${unknown} unknown`);
    return parts.join(" · ");
  });
</script>

{#if compact}
  <span class="strip">
    {#each views as view (view.name)}
      <Mark
        mark={view.mark}
        shapeOnly
        title={`${view.name} ${view.display}${view.lastKnown ? `, last known ${view.lastKnown}` : ""}`}
      />
    {/each}
    <span class="summary mono">{summary}</span>
  </span>
{:else}
  <ul class="cells">
    {#each views as view (view.name)}
      <li class:hatched={view.freshness === "expired"}>
        <span class="name" class:ageing={view.freshness === "ageing"}>
          <Mark mark={view.mark} text={false} />
          <span class="mono">{view.name}</span>
        </span>
        <span class="meta mono">
          {view.display} · {view.observedRevision > 0 ? `${age(view.ageSeconds)} ago` : "never observed"} ·
          {view.observedRevision > 0 ? `rev ${view.observedRevision}` : "no revision"}
          <!-- An expired value is demoted to history, never deleted. -->
          {#if view.lastKnown}<br /><span class="last">last {view.lastKnown}</span>{/if}
        </span>
        <span class="reason mono" style="color: var(--{view.mark.tone})">{view.reason}</span>
      </li>
    {/each}
  </ul>
{/if}

<style>
  .strip {
    display: flex;
    align-items: center;
    gap: 5px;
    flex-wrap: wrap;
  }
  .summary {
    font-size: 10px;
    color: var(--ink-muted);
    margin-left: 4px;
  }

  .cells {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-wrap: wrap;
    gap: 1px;
    background: var(--line);
  }
  li {
    flex: 1 1 190px;
    background-color: var(--panel);
    padding: 12px 14px;
    display: flex;
    flex-direction: column;
    gap: 7px;
  }
  .name {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    color: var(--ink);
  }
  /* Ageing is drawn, not just stated, so decay is visible before the drop. */
  .ageing {
    text-decoration: underline dashed var(--attn);
    text-underline-offset: 3px;
  }
  .meta {
    font-size: 10px;
    color: var(--ink-muted);
    line-height: 1.55;
  }
  .last {
    text-decoration: line-through;
    text-decoration-color: var(--ink-faint);
  }
  .reason {
    font-size: 11px;
    line-height: 1.5;
  }
</style>
