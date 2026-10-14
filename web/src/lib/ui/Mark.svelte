<script lang="ts">
  import type { Mark } from "../state";

  // A mark is a triple: shape, glyph, and a text label in the same cell. It
  // survives greyscale and a colourblind reader, because colour is the last
  // signal added and never the first one read. The drawn shape is decorative to
  // assistive technology; the label is what carries the state.
  // decorative is for a mark whose neighbouring text already states the value.
  // Announcing the label again would read it twice to a screen reader.
  let {
    mark,
    text = true,
    shapeOnly = false,
    decorative = false,
    title = "",
  }: { mark: Mark; text?: boolean; shapeOnly?: boolean; decorative?: boolean; title?: string } = $props();
</script>

<span class="mark {mark.tone}">
  <span class="shape {mark.shape}" aria-hidden="true" title={title || undefined}></span>
  {#if shapeOnly}
    <span class="sr">{title || mark.label}</span>
  {:else}
    {#if text}<span class="text">{mark.label}</span>{:else}<span class="glyph" aria-hidden="true">{mark.glyph}</span>{#if !decorative}<span class="sr">{mark.label}</span>{/if}{/if}
  {/if}
</span>

<style>
  .mark {
    display: inline-flex;
    gap: 8px;
    align-items: center;
    color: var(--c);
  }
  .ok { --c: var(--ok); }
  .attn { --c: var(--attn); }
  .fail { --c: var(--fail); }
  .active { --c: var(--active); }
  .unc { --c: var(--unc); }
  .neutral { --c: var(--neutral); }
  .dim { --c: var(--dim); }
  .shape {
    width: 12px;
    height: 12px;
    flex-shrink: 0;
    display: inline-block;
  }
  .glyph {
    font-family: "IBM Plex Mono", ui-monospace, monospace;
    font-size: 12px;
    width: 14px;
    text-align: center;
    flex-shrink: 0;
  }
  .text {
    font-size: 13px;
    font-weight: 500;
  }

  /* Outline is an intention, solid is an observation, dashed is uncertainty. */
  .circle { border-radius: 50%; background: var(--c); }
  .circle-open { border-radius: 50%; border: 1.5px solid var(--c); }
  .circle-dashed { border-radius: 50%; border: 1.5px dashed var(--c); }
  /* A half fill reads as partial even in greyscale. */
  .circle-half {
    border-radius: 50%;
    border: 1.5px solid var(--c);
    background: linear-gradient(90deg, var(--c) 0 50%, transparent 50% 100%);
  }
  .square { background: var(--c); }
  .square-open { border: 1.5px solid var(--c); }
  .square-half {
    border: 1.5px solid var(--c);
    background: linear-gradient(180deg, var(--c) 0 50%, transparent 50% 100%);
  }
  .square-slash {
    border: 1.5px solid var(--c);
    background: linear-gradient(135deg, transparent 43%, var(--c) 43% 57%, transparent 57%);
  }
  .square-open-slash {
    border: 1.5px dotted var(--c);
    background: linear-gradient(135deg, transparent 45%, var(--c) 45% 55%, transparent 55%);
  }
  .triangle {
    clip-path: polygon(50% 4%, 100% 96%, 0 96%);
    background: var(--c);
  }
  /* A diamond is reserved for known-bad, so failure has a unique silhouette. */
  .diamond { width: 11px; height: 11px; transform: rotate(45deg); background: var(--c); }
  .diamond-open { width: 11px; height: 11px; transform: rotate(45deg); border: 1.5px solid var(--c); }
  .diamond-hatched {
    width: 11px;
    height: 11px;
    transform: rotate(45deg);
    border: 1.5px solid var(--c);
    background: repeating-linear-gradient(45deg, var(--c) 0 2px, transparent 2px 4px);
  }

  .sr {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
  }
</style>
