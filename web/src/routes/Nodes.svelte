<script lang="ts">
  import { gaps, nodes, unknown as unknownMark, ok as okMark } from "../lib/mock";
  import { age, freshness } from "../lib/state";
  import { follow, path } from "../lib/router.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Mocked from "../lib/ui/Mocked.svelte";

  let { id, onOpen }: { id: string; onOpen: (id: string) => void } = $props();

  // An address may name one node. Naming one narrows the list rather than opening
  // a different screen, so the address still survives a reload.
  let shown = $derived(id === "" ? nodes : nodes.filter((node) => node.name === id || node.id === id));
</script>

<div class="screen">
  <header>
    <div class="titles">
      <h1>Nodes</h1>
      <p class="note">
        {nodes.length} enrolled hosts · capacity is reserved durably, so requested and reserved are
        shown apart.
      </p>
    </div>
    <div class="actions">
      {#if id !== ""}
        <a class="back mono" href={path("nodes")} onclick={(event) => follow(event, () => onOpen(""))}>All nodes</a>
      {/if}
      <button type="button" disabled>Enrollment tokens</button>
    </div>
  </header>

  <Mocked gap={gaps.nodes} />

  {#each shown as node (node.id)}
    {@const expired = freshness(node.seenAgeSeconds) === "expired"}
    {@const ageing = freshness(node.seenAgeSeconds) === "ageing"}
    <article class="card" class:hatched={expired} class:strong={expired}>
      <div class="columns">
        <div class="identity">
          <div class="name-row">
            <Mark mark={expired ? unknownMark("unknown") : okMark("reporting")} shapeOnly title={expired ? "unknown" : "reporting"} />
            <span class="mono name">{node.name}</span>
            <span class="badge mono" style="border-color: var(--{node.stateTone}); color: var(--{node.stateTone})">
              {node.state}
            </span>
          </div>
          <dl class="facts mono">
            <dt>address</dt><dd>{node.address}</dd>
            <dt>node_id</dt><dd>{node.id}</dd>
            <dt>agent</dt><dd>{node.agent}</dd>
            <dt>last_seen</dt>
            <dd class:expired class:ageing>
              {age(node.seenAgeSeconds)} ago{expired ? " · limit 60s" : ""}
            </dd>
          </dl>
          <div class="labels">
            {#each node.labels as label (label)}
              <span class="chip mono">{label}</span>
            {/each}
          </div>
        </div>

        <div class="capacity">
          <span class="label">Capacity · reserved / total</span>
          {#each node.capacity as entry (entry.name)}
            <div class="bar">
              <div class="bar-head mono">
                <span class="key">{entry.name}</span>
                <span class="value">{entry.value}</span>
                <span class="note-inline">{entry.note}</span>
              </div>
              <div class="track">
                <div class="fill" style="width: {entry.share}%; background: var(--{entry.tone})"></div>
              </div>
            </div>
          {/each}
        </div>

        <div class="placement">
          <span class="label">Placement · {node.placements.length}</span>
          <div class="placements">
            {#each node.placements as placement (placement.name)}
              <div class="placed mono">
                <Mark mark={placement.mark} text={false} />
                <span class="who">{placement.name}</span>
                <span class="role">{placement.role}</span>
              </div>
            {/each}
          </div>
          <div class="buttons">
            <button type="button" disabled>{node.cordonLabel}</button>
            <button type="button" disabled>Drain…</button>
          </div>
          <p class="note">{node.actionNote}</p>
        </div>
      </div>
    </article>
  {:else}
    <p class="note">No node has that name.</p>
  {/each}
</div>

<style>
  .screen {
    display: flex;
    flex-direction: column;
    gap: 22px;
    max-width: 1240px;
  }
  header {
    display: flex;
    align-items: flex-end;
    gap: 16px;
    flex-wrap: wrap;
  }
  .titles {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  header .note {
    font-size: 13px;
  }
  .actions {
    margin-left: auto;
    display: flex;
    gap: 8px;
    align-items: center;
  }
  .back {
    font-size: 11px;
  }

  .card {
    border: 1px solid var(--line);
    background-color: var(--panel);
    padding: 18px;
  }
  /* An expired source gets a stronger edge as well as a hatch, so the state does
     not rest on the hatch alone. */
  .card.strong {
    border-color: var(--line-strong);
  }
  .columns {
    display: grid;
    grid-template-columns: minmax(0, 1.05fr) minmax(0, 1.25fr) minmax(0, 1.1fr);
    gap: 20px;
  }
  .identity,
  .capacity,
  .placement {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .name-row {
    display: flex;
    align-items: center;
    gap: 9px;
    flex-wrap: wrap;
  }
  .name {
    font-size: 14px;
    font-weight: 600;
  }
  .badge {
    font-size: 10px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    border: 1px solid;
    padding: 2px 7px;
  }
  .facts {
    display: grid;
    grid-template-columns: auto 1fr;
    row-gap: 4px;
    column-gap: 12px;
    margin: 0;
    font-size: 11px;
  }
  .facts dt {
    color: var(--ink-faint);
  }
  .facts dd {
    margin: 0;
    color: var(--ink-body);
  }
  /* Ageing and expired are drawn on the value itself, not stated beside it. */
  .facts dd.ageing {
    text-decoration: underline dashed var(--attn);
    text-underline-offset: 3px;
  }
  .facts dd.expired {
    text-decoration: underline dashed var(--ink-muted);
    text-underline-offset: 3px;
  }
  .labels {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
  }
  .chip {
    font-size: 10px;
    border: 1px solid var(--line-strong);
    color: var(--ink-muted);
    padding: 3px 7px;
  }

  .bar {
    display: flex;
    flex-direction: column;
    gap: 5px;
  }
  .bar-head {
    display: flex;
    gap: 8px;
    font-size: 11px;
  }
  .bar-head .key {
    color: var(--ink-muted);
    width: 52px;
  }
  .bar-head .value {
    color: var(--ink);
  }
  .bar-head .note-inline {
    margin-left: auto;
    color: var(--ink-faint);
  }
  .track {
    height: 6px;
    background: var(--line);
  }
  .fill {
    height: 6px;
  }

  .placements {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .placed {
    display: flex;
    gap: 8px;
    align-items: center;
    font-size: 11px;
  }
  .placed .who {
    color: var(--ink);
  }
  .placed .role {
    margin-left: auto;
    color: var(--ink-dim);
  }
  .buttons {
    margin-top: auto;
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
  }
  .placement .note {
    font-size: 11px;
  }
</style>
