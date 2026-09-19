<script lang="ts">
  import { backupPolicy, backups, clusters, gaps, recoveryRange } from "../lib/mock";
  import { viewConditions } from "../lib/state";
  import Mark from "../lib/ui/Mark.svelte";
  import Mocked from "../lib/ui/Mocked.svelte";
  import Panel from "../lib/ui/Panel.svelte";

  const now = new Date();
  // BackupCurrent is a cluster condition, so it is read through the same state
  // system as every other condition rather than written down again here.
  const current = viewConditions(clusters(now)[0]?.conditions ?? [], now).find(
    (view) => view.name === "BackupCurrent",
  );
  const total = recoveryRange.spans.reduce((sum, span) => sum + span.share, 0);
</script>

<div class="screen">
  <header>
    <div class="titles">
      <h1>Backups</h1>
      <p class="note">
        A backup that exists is not a backup that restores. Verification state is reported
        separately from artifact state.
      </p>
    </div>
    <div class="actions">
      <button type="button" disabled>Back up now</button>
      <button type="button" disabled>Restore…</button>
    </div>
  </header>

  <Mocked gap={gaps.backups} />

  <div class="cards">
    <section class="card">
      <span class="label">Policy · pg-core</span>
      <dl class="facts mono">
        {#each backupPolicy as entry (entry.key)}
          <dt>{entry.key}</dt><dd>{entry.value}</dd>
        {/each}
      </dl>
    </section>

    <section class="card">
      <span class="label">Recovery range</span>
      <div class="range">
        <!-- A gap is drawn as a broken span, because recovery is refused inside it. -->
        {#each recoveryRange.spans as span, index (index)}
          <span
            class="span"
            class:gap={span.gap}
            style="flex: {span.share}"
            title={span.gap ? "gap — no recovery offered" : "recoverable"}
          ></span>
        {/each}
      </div>
      <div class="range-ends mono">
        <span>{recoveryRange.from}</span>
        <span>{recoveryRange.to}</span>
      </div>
      <p class="gap-note mono">▲ {recoveryRange.gapNote}</p>
      <p class="note">{recoveryRange.detail}</p>
      <span class="sr">The range covers {total} parts, of which one is a gap.</span>
    </section>

    <section class="card">
      <span class="label">BackupCurrent · cluster condition</span>
      {#if current}
        <div class="condition" class:hatched={current.freshness === "expired"}>
          <Mark mark={current.mark} text={false} />
          <span class="mono">{current.display}</span>
        </div>
        <p class="mono condition-meta">
          {current.display} · rev {current.observedRevision}<br />reason: {current.reason}
        </p>
      {/if}
      <p class="note">
        The last backup succeeded. Whether it is still there cannot currently be observed — so this
        is unknown, not true.
      </p>
    </section>
  </div>

  <Panel title="Artifacts">
    <div class="scroll">
      <div class="grid backups">
        <div class="head">Backup</div>
        <div class="head">Kind</div>
        <div class="head">Scope</div>
        <div class="head">Size</div>
        <div class="head">Verification</div>
        <div class="head">Taken</div>
        {#each backups as backup (backup.id)}
          <div class="cell stack">
            <span class="mono id">{backup.id}</span>
            <span class="mono source">{backup.source}</span>
          </div>
          <div class="cell">{backup.kind}</div>
          <div class="cell">{backup.scope}</div>
          <div class="cell">{backup.size}</div>
          <div class="cell" style="color: var(--{backup.mark.tone})">
            <Mark mark={backup.mark} text={false} decorative />
            <span class="mono">{backup.verification}</span>
          </div>
          <div class="cell taken">{backup.taken}</div>
        {/each}
      </div>
    </div>
    {#snippet foot()}
      Verification is manual in v0.1, so an unverified artifact says
      <span class="mono">never verified</span> rather than showing a neutral dash. Restore always
      targets a new cluster unless an overwrite is explicitly validated through a plan.
    {/snippet}
  </Panel>
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
  }

  .cards {
    display: flex;
    flex-wrap: wrap;
    gap: 1px;
    background: var(--line);
    border: 1px solid var(--line);
  }
  .card {
    flex: 1 1 300px;
    background-color: var(--panel);
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .facts {
    display: grid;
    grid-template-columns: auto 1fr;
    row-gap: 5px;
    column-gap: 12px;
    margin: 0;
    font-size: 11px;
  }
  .facts dt {
    color: var(--ink-faint);
  }
  .facts dd {
    margin: 0;
    color: var(--ink);
  }

  .range {
    height: 8px;
    display: flex;
    gap: 2px;
  }
  .span {
    background: var(--ok);
  }
  .span.gap {
    background: repeating-linear-gradient(90deg, var(--ink-muted) 0 3px, transparent 3px 6px);
  }
  .range-ends {
    display: flex;
    justify-content: space-between;
    font-size: 10px;
    color: var(--ink-dim);
  }
  .gap-note {
    margin: 0;
    font-size: 11px;
    color: var(--attn);
    line-height: 1.5;
  }

  .condition {
    display: flex;
    align-items: center;
    gap: 9px;
    padding: 6px 8px;
    margin: 0 -8px;
    font-size: 12px;
  }
  .condition-meta {
    margin: 0;
    font-size: 11px;
    color: var(--ink-body);
    line-height: 1.55;
  }

  .grid.backups {
    grid-template-columns:
      minmax(170px, 1.1fr) minmax(110px, 0.7fr) minmax(140px, 0.8fr)
      minmax(120px, 0.7fr) minmax(190px, 1.1fr) minmax(110px, 0.7fr);
    min-width: 960px;
  }
  .id {
    font-size: 12px;
  }
  .source {
    font-size: 10px;
    color: var(--ink-faint);
  }
  .taken {
    color: var(--ink-muted);
  }
</style>
