<script lang="ts">
  import { activeOperations, clusters, decisions, freshnessCounts, gaps, healthPlanes } from "../lib/mock";
  import { age, desiredMark, freshness, observedMark, verdict, verdictView } from "../lib/state";
  import { follow, path, type ScreenName } from "../lib/router.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Mocked from "../lib/ui/Mocked.svelte";
  import Panel from "../lib/ui/Panel.svelte";

  let { go }: { go: (screen: ScreenName, id?: string) => void } = $props();

  const now = new Date();
  const fleet = clusters(now);

  /** row reads one cluster into the three planes the design keeps apart. */
  function pair(cluster: (typeof fleet)[number]) {
    const value = {
      desired: cluster.desired,
      desiredRevision: cluster.desiredRevision,
      observed: cluster.observed,
      observedRevision: cluster.observedRevision,
      observedAt: new Date(now.getTime() - cluster.observedAgeSeconds * 1000).toISOString(),
      ...(cluster.owningOperation ? { owningOperation: cluster.owningOperation } : {}),
    };
    const result = verdict(value, now);
    const expired = freshness(cluster.observedAgeSeconds) === "expired";
    return {
      result,
      view: verdictView(result),
      desiredMark: desiredMark(cluster.desired),
      observedMark: observedMark(expired ? "unknown" : cluster.observed),
      expired,
    };
  }
</script>

<div class="screen">
  <header>
    <h1>Fleet</h1>
    <p class="note">
      {fleet.length} clusters · 4 nodes · management, database, and backup health are reported
      separately and never combined.
    </p>
  </header>

  <Mocked gap={gaps.collections} />

  <!-- Three planes. They fail separately, so they are never averaged together. -->
  <div class="planes">
    {#each healthPlanes as plane (plane.title)}
      <section class="plane">
        <div class="plane-head">
          <span class="eyebrow">{plane.title}</span>
          <span class="source mono">{plane.source}</span>
        </div>
        <p class="note">{plane.what}</p>
        <div class="plane-rows">
          {#each plane.rows as row (row.label)}
            <div class="plane-row" class:hatched={row.stale}>
              <Mark mark={row.mark} text={false} decorative />
              <span class="mono grow">{row.label}</span>
              <span class="mono meta">{row.meta}</span>
            </div>
          {/each}
        </div>
      </section>
    {/each}
  </div>

  <div class="columns">
    <Panel title="Needs a decision" note="ordered by what is unrecoverable if ignored, not by severity colour">
      {#each decisions as decision (decision.title)}
        <a
          class="row top decision"
          href={path(decision.go.screen, decision.go.id)}
          onclick={(event) => follow(event, () => go(decision.go.screen, decision.go.id))}
        >
          <Mark mark={decision.mark} text={false} />
          <span class="grow">
            <span class="mono title">{decision.title}</span>
            <span class="detail">{decision.detail}</span>
          </span>
          <span class="mono action">{decision.action}</span>
        </a>
      {/each}
    </Panel>

    <div class="stack">
      <Panel title={`Active operations · ${activeOperations.length}`}>
        {#each activeOperations as operation (operation.id)}
          <a
            class="row"
            href={path("operation", operation.id)}
            onclick={(event) => follow(event, () => go("operation", operation.id))}
          >
            <Mark mark={operation.mark} text={false} />
            <span class="grow">
              <span class="mono title">{operation.id}</span>
              <span class="mono meta">{operation.what}</span>
            </span>
            <span class="mono action">{operation.step}</span>
          </a>
        {/each}
      </Panel>

      <Panel title="Observation freshness">
        <div class="freshness">
          {#each freshnessCounts as bucket (bucket.label)}
            <div class="bucket">
              <span class="mono name">{bucket.label}</span>
              <span class="track" class:dashed={bucket.dashed}>
                <span class="fill" style="width: {bucket.share}%; background: var(--{bucket.tone})"></span>
              </span>
              <span class="mono count">{bucket.count}</span>
            </div>
          {/each}
          <p class="note">
            Expired observations are counted, not hidden. An expired report is not a failure and
            does not change desired state.
          </p>
        </div>
      </Panel>
    </div>
  </div>

  <Panel title="All clusters">
    <div class="scroll">
      <div class="grid fleet">
        <div class="head">Cluster</div>
        <div class="head">Desired / observed</div>
        <div class="head">Management</div>
        <div class="head">Database</div>
        <div class="head">Backup</div>
        <div class="head">Operation</div>
        {#each fleet as cluster (cluster.id)}
          {@const state = pair(cluster)}
          <div class="cell stack">
            <a
              class="mono name"
              href={path("cluster", cluster.id)}
              onclick={(event) => follow(event, () => go("cluster", cluster.id))}>{cluster.name}</a
            >
            <span class="mono engine">{cluster.engine}</span>
          </div>
          <!-- Desired above, observed below. Never collapsed into one badge. -->
          <div
            class="cell stack pair"
            class:ruled={state.result !== "converged"}
            class:dashed={state.view.dashed}
            style="--rule: var(--{state.view.tone})"
          >
            <span class="mono desired">
              {state.desiredMark.glyph} {cluster.desired} · rev {cluster.desiredRevision}
            </span>
            <span class="mono observed">
              {state.observedMark.glyph} {state.observedMark.label} · {age(cluster.observedAgeSeconds)}
            </span>
          </div>
          <div class="cell" style="color: var(--{cluster.management.mark.tone})">
            <Mark mark={cluster.management.mark} text={false} />
            <span class="mono">{cluster.management.text}</span>
          </div>
          <div class="cell" style="color: var(--{cluster.database.mark.tone})">
            <Mark mark={cluster.database.mark} text={false} />
            <span class="mono">{cluster.database.text}</span>
          </div>
          <div class="cell" style="color: var(--{cluster.backup.mark.tone})">
            <Mark mark={cluster.backup.mark} text={false} />
            <span class="mono">{cluster.backup.text}</span>
          </div>
          <div class="cell" style="color: var(--{cluster.operation.mark.tone})">
            <Mark mark={cluster.operation.mark} text={false} />
            <span class="mono">{cluster.operation.text}</span>
          </div>
        {/each}
      </div>
    </div>
    {#snippet foot()}
      No row is summarised into a single dot. Management, database, and backup are three
      independent facts, and the operation that owns a difference is named beside them.
    {/snippet}
  </Panel>
</div>

<style>
  .screen {
    display: flex;
    flex-direction: column;
    gap: 26px;
    max-width: 1240px;
  }
  header {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  header .note {
    font-size: 13px;
  }

  .planes {
    display: flex;
    flex-wrap: wrap;
    gap: 1px;
    background: var(--line);
    border: 1px solid var(--line);
  }
  .plane {
    flex: 1 1 258px;
    background-color: var(--panel);
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .plane-head {
    display: flex;
    align-items: baseline;
    gap: 8px;
  }
  .plane-head .eyebrow {
    text-transform: uppercase;
  }
  .source {
    font-size: 10px;
    color: var(--ink-faint);
    margin-left: auto;
  }
  .plane-rows {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .plane-row {
    display: flex;
    align-items: center;
    gap: 9px;
    font-size: 12px;
    padding: 5px 7px;
    margin: 0 -7px;
  }
  .grow {
    flex: 1;
    min-width: 0;
  }
  .meta {
    font-size: 11px;
    color: var(--ink-dim);
  }

  .columns {
    display: grid;
    grid-template-columns: minmax(0, 1.45fr) minmax(0, 1fr);
    gap: 20px;
    align-items: start;
  }
  .stack {
    display: flex;
    flex-direction: column;
    gap: 20px;
  }
  .row {
    text-decoration: none;
    color: inherit;
  }
  .row .grow {
    display: flex;
    flex-direction: column;
    gap: 3px;
  }
  .title {
    font-size: 12px;
    color: var(--ink);
  }
  .detail {
    font-size: 12px;
    color: var(--ink-soft);
    line-height: 1.5;
  }
  .action {
    font-size: 11px;
    color: var(--ink-muted);
    white-space: nowrap;
  }
  .decision:hover .title,
  .row:hover .title {
    text-decoration: underline;
  }

  .freshness {
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 9px;
  }
  .bucket {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .bucket .name {
    font-size: 11px;
    color: var(--ink-muted);
    width: 92px;
  }
  .track {
    flex: 1;
    height: 6px;
    background: var(--line);
  }
  /* An expired bucket carries a broken track, so the count is not colour alone. */
  .track.dashed {
    background: repeating-linear-gradient(90deg, var(--line-strong) 0 3px, transparent 3px 6px);
  }
  .fill {
    display: block;
    height: 6px;
  }
  .count {
    font-size: 11px;
    width: 26px;
    text-align: right;
  }

  .grid.fleet {
    grid-template-columns:
      minmax(160px, 1.1fr) minmax(180px, 1fr) minmax(130px, 0.8fr)
      minmax(140px, 0.9fr) minmax(140px, 0.9fr) minmax(170px, 1fr);
    min-width: 980px;
  }
  .name {
    font-size: 12px;
    font-weight: 500;
  }
  .engine {
    font-size: 10px;
    color: var(--ink-faint);
  }
  .pair.ruled {
    border-left: 2px solid var(--rule);
    padding-left: 10px;
  }
  .pair.ruled.dashed {
    border-left-style: dashed;
  }
  .desired {
    color: var(--ink-muted);
  }
  .observed {
    color: var(--ink);
  }
</style>
