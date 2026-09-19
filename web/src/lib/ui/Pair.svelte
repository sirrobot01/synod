<script lang="ts">
  import { age, ageSeconds, desiredMark, freshness, observedMark, verdict, verdictView, type Divergence } from "../state";
  import Mark from "./Mark.svelte";

  // Desired above, observed below, each with its own revision. A single collapsed
  // badge would have to pick a winner between intent and observation; this pair
  // never does. Divergence is annotated, not alarmed.
  let {
    value,
    now = new Date(),
    labels = true,
  }: { value: Divergence; now?: Date; labels?: boolean } = $props();

  let result = $derived(verdict(value, now));
  let view = $derived(verdictView(result));
  let seconds = $derived(ageSeconds(value.observedAt, now));
  // An expired observation displays unknown and keeps its last known value.
  let expired = $derived(freshness(seconds) === "expired");
  let observed = $derived(observedMark(expired ? "unknown" : value.observed));
</script>

<div class="pair">
  {#if labels}
    <div class="verdict">
      <span class="label" style="color: var(--{view.tone})">{view.label}</span>
    </div>
  {/if}
  <div class="rows" class:dashed={view.dashed} style="--rule: var(--{result === 'converged' ? 'line' : view.tone})">
    <span class="label">desired</span>
    <Mark mark={desiredMark(value.desired)} shapeOnly title={`desired ${value.desired}`} />
    <span class="mono value dim">{desiredMark(value.desired).glyph} {value.desired}</span>
    <span class="mono rev">rev {value.desiredRevision}</span>

    <span class="label">observed</span>
    <Mark mark={observed} shapeOnly title={`observed ${observed.label}`} />
    <span class="mono value">{observed.glyph} {observed.label}</span>
    <span class="mono rev">rev {value.observedRevision} · {age(seconds)}</span>
  </div>
  {#if expired && value.observed !== "unknown"}
    <p class="note">
      Last seen <span class="mono">{value.observed}</span> {age(seconds)} ago at rev {value.observedRevision}.
      <!-- No failure is claimed and no recovery is started from silence alone. -->
      <strong>The database may still be serving traffic.</strong>
    </p>
  {:else if value.owningOperation}
    <p class="note">Operation <span class="mono">{value.owningOperation}</span> owns this change.</p>
  {/if}
</div>

<style>
  .pair {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .verdict {
    display: flex;
    justify-content: flex-end;
    align-items: baseline;
    gap: 12px;
  }
  .rows {
    display: grid;
    grid-template-columns: 74px 22px 1fr auto;
    row-gap: 6px;
    column-gap: 8px;
    align-items: center;
    border-left: 2px solid var(--rule);
    padding-left: 12px;
  }
  /* An uncertain difference gets a dashed rule: nobody can currently say. */
  .rows.dashed {
    border-left-style: dashed;
  }
  .value {
    font-size: 12px;
    color: var(--ink);
  }
  .rev {
    font-size: 11px;
    color: var(--ink-dim);
  }
  .note {
    margin: 0;
    font-size: 12px;
    color: var(--ink-body);
    line-height: 1.55;
  }
  strong {
    font-weight: 500;
    color: var(--ink);
  }
</style>
