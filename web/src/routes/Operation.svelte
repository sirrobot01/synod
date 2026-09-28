<script lang="ts">
  import { api, detail, type Operation } from "../lib/api";
  import { accepted, age, ageSeconds, canCancel, canRetry, errorView, finished, operationMark, type APIError } from "../lib/state";
  import { gaps, residual, steps, attention as attentionMark } from "../lib/mock";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Mocked from "../lib/ui/Mocked.svelte";
  import Panel from "../lib/ui/Panel.svelte";

  let { id }: { id: string } = $props();

  let operation = $state<Operation | null>(null);
  let failure = $state<APIError | null>(null);
  let readAt = $state<Date | null>(null);
  let reading = $state(false);

  async function read() {
    // The address can change while a request is open. An answer for the previous
    // address must never be drawn under this one.
    const wanted = id;
    reading = true;
    try {
      const answer = await api.operation(wanted);
      if (wanted !== id) return;
      operation = answer;
      failure = null;
      readAt = new Date();
    } catch (error) {
      // The previous answer stays on the screen. Blanking it would hide the last
      // thing that was true, and a failed read proves nothing about the operation.
      if (wanted === id) failure = detail(error);
    } finally {
      if (wanted === id) reading = false;
    }
  }

  $effect(() => {
    // A changed address is a different operation, so drop the previous answer.
    operation = null;
    failure = null;
    readAt = null;
    read();
  });

  // An operation that no owner can change never changes again. Deriving this
  // keeps the poll below from restarting on every answer.
  let settled = $derived(operation !== null && finished(operation.status));

  $effect(() => {
    if (settled) return;
    const timer = setInterval(read, 5000);
    return () => clearInterval(timer);
  });

  let mark = $derived(operation === null ? null : operationMark(operation.status, operation.plan_version));
</script>

<div class="screen">
  <h1 class="mono">{id}</h1>

  {#if failure}
    <ErrorPanel view={errorView(failure)} />
  {/if}

  {#if operation && mark}
    {@const current = operation}
    <section class="panel detail" style="border-color: var(--{mark.tone})">
      <div class="strip">
        <Mark {mark} text={false} decorative />
        <span class="mono status" style="color: var(--{mark.tone})">{mark.label}</span>
        <span class="mono who">{current.id}</span>
        <span class="mono kind">{current.kind}</span>
      </div>

      <div class="body">
        {#if accepted(current.status, current.plan_version)}
          <!-- Acceptance is its own state, with its own weight. It says nothing
               about any app yet. -->
          <p class="claim">
            The controller has durably recorded this request. Nothing has been provisioned yet, and
            no claim is made about app <span class="mono">{current.app_id}</span>.
          </p>
        {:else if current.status === "waiting"}
          <p class="claim">
            This is <strong>not a queue and not a spinner.</strong> The outcome is genuinely unknown
            until it is inspected. Retry is deliberately not offered; the path forward is
            inspection with the same idempotency key.
          </p>
        {/if}

        <dl class="facts mono">
          <dt>status</dt><dd>{current.status}</dd>
          <dt>plan_version</dt>
          <dd>{current.plan_version === 0 ? "0 — no executable plan yet" : current.plan_version}</dd>
          <dt>desired_revision</dt><dd>{current.desired_revision}</dd>
          <dt>attempts</dt><dd>{current.attempts}</dd>
          <dt>failure_code</dt>
          <dd style={current.failure_code ? "color: var(--fail)" : undefined}>{current.failure_code ?? "none"}</dd>
          <dt>cancellation</dt><dd>{current.cancellation_requested_at ?? "not requested"}</dd>
          <dt>created_at</dt><dd>{current.created_at}</dd>
          <dt>updated_at</dt><dd>{current.updated_at}</dd>
          <dt>finished_at</dt><dd>{current.finished_at ?? "not finished"}</dd>
          <dt>app_id</dt><dd>{current.app_id}</dd>
        </dl>

        <div class="actions">
          <button type="button" onclick={read} disabled={reading}>{reading ? "Reading" : "Read again"}</button>
          <!-- The state system decides whether the status permits an action, and
               the API serves neither route, so neither button can be offered. -->
          <button type="button" disabled>
            {canRetry(current.status) ? "Retry — no API route" : "Retry — not permitted"}
          </button>
          <button type="button" disabled>
            {canCancel(current.status, current.cancellation_requested_at !== null)
              ? "Cancel — no API route"
              : "Cancel — not permitted"}
          </button>
          <span class="note">
            This status
            {canCancel(current.status, current.cancellation_requested_at !== null) ? "permits" : "does not permit"}
            cancellation, and
            {canRetry(current.status) ? "permits" : "does not permit"}
            a retry. Gap: <span class="mono">{gaps.actions}</span>.
            {#if !settled}This screen reads again every 5 seconds.{/if}
            {#if readAt}Read {age(ageSeconds(readAt.toISOString(), new Date()))} ago.{/if}
          </span>
        </div>
      </div>
    </section>
  {:else if !failure}
    <p class="note">Reading the operation.</p>
  {/if}

  <Panel title={`Steps · ${steps.length}`}>
    <div class="mock-strip"><Mocked gap={gaps.steps} /></div>
    <div class="steps">
      {#each steps as step (step.ordinal)}
        <div class="step" style="color: var(--{step.tone === 'dim' ? 'ink-faint' : 'ink'})">
          <span class="mono glyph" style="color: var(--{step.tone})" aria-hidden="true">{step.glyph}</span>
          <span class="mono ordinal">{step.ordinal}</span>
          <span class="mono name">{step.name}</span>
          <span class="mono meta">{step.meta}</span>
        </div>
        {#if step.note}
          <p class="step-note mono">{step.note}</p>
        {/if}
      {/each}
    </div>
    {#snippet foot()}
      A step count is always shown beside progress. There is no progress bar without one.
    {/snippet}
  </Panel>

  <!-- Residual state belongs to an operation that stopped after doing work. A
       pending or running one has left nothing behind, and saying otherwise would
       claim an effect that never happened. -->
  {#if operation && (operation.status === "failed" || operation.status === "cancelled")}
  <section class="panel residual">
    <div class="residual-head hatched">
      <Mark mark={attentionMark("manual intervention")} text={false} />
      <span class="mono">
        Residual resources · {residual.length} · manual intervention required
      </span>
    </div>
    <div class="mock-strip"><Mocked gap={gaps.steps} /></div>
    <p class="residual-body">
      An operation that ends without cleaning up records what it left behind. Nothing here is
      deleted automatically — a nonempty volume is never removed as generic failure cleanup.
    </p>
    <div class="grid residual-grid">
      {#each residual as item (item.id)}
        <div class="cell res-kind">{item.kind}</div>
        <div class="cell id">{item.id}</div>
        <div class="cell where">{item.where}</div>
        <div class="cell disposition">{item.disposition}</div>
      {/each}
    </div>
  </section>
  {/if}
</div>

<style>
  .screen {
    display: flex;
    flex-direction: column;
    gap: 22px;
    max-width: 1240px;
  }
  h1 {
    font-size: 20px;
  }

  .detail {
    display: flex;
    flex-direction: column;
  }
  .strip {
    padding: 14px 16px;
    border-bottom: 1px solid var(--line);
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }
  .status {
    font-size: 12px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }
  .who {
    font-size: 12px;
    color: var(--ink);
  }
  .kind {
    margin-left: auto;
    font-size: 11px;
    color: var(--ink-dim);
  }
  .body {
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 16px;
  }
  .claim {
    margin: 0;
    font-size: 13px;
    line-height: 1.6;
    color: var(--ink-body);
  }
  .claim strong {
    font-weight: 500;
    color: var(--ink);
  }
  .facts {
    display: grid;
    grid-template-columns: auto 1fr;
    row-gap: 5px;
    column-gap: 14px;
    margin: 0;
    font-size: 11px;
  }
  .facts dt {
    color: var(--ink-faint);
  }
  .facts dd {
    margin: 0;
    color: var(--ink);
    overflow-wrap: anywhere;
  }
  .actions {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    align-items: center;
    border-top: 1px solid var(--line);
    padding-top: 14px;
  }
  .actions .note {
    flex: 1;
    min-width: 220px;
    font-size: 11px;
  }

  .mock-strip {
    padding: 12px 16px 0;
  }
  .steps {
    padding: 12px 16px 16px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .step {
    display: flex;
    align-items: baseline;
    gap: 8px;
  }
  .glyph {
    font-size: 12px;
    width: 14px;
    text-align: center;
    flex-shrink: 0;
  }
  .ordinal {
    font-size: 11px;
    color: var(--ink-faint);
    width: 16px;
  }
  .step .name {
    flex: 1;
    min-width: 0;
    font-size: 11px;
  }
  .step .meta {
    font-size: 10px;
    color: var(--ink-dim);
  }
  .step-note {
    margin: 2px 0 6px 26px;
    padding: 6px 10px;
    border-left: 2px solid var(--fail);
    font-size: 11px;
    color: var(--danger-ink);
    line-height: 1.55;
  }

  .residual {
    border-color: var(--attn-line);
    display: flex;
    flex-direction: column;
  }
  .residual-head {
    padding: 12px 16px;
    display: flex;
    align-items: center;
    gap: 9px;
    font-size: 11px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--attn);
    border-bottom: 1px solid var(--line);
  }
  .residual-body {
    margin: 0;
    padding: 12px 16px;
    font-size: 12px;
    color: var(--ink-body);
    line-height: 1.55;
  }
  .grid.residual-grid {
    grid-template-columns: 90px minmax(140px, 1fr) minmax(90px, 0.6fr) minmax(200px, 1.2fr);
    border-top: 1px solid var(--line);
  }
  .residual-grid .res-kind {
    color: var(--ink-faint);
    font-size: 10px;
  }
  .residual-grid .id {
    color: var(--ink);
  }
  .residual-grid .where,
  .residual-grid .disposition {
    color: var(--ink-muted);
  }
</style>
