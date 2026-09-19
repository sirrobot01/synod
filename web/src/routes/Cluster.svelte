<script lang="ts">
  import { api, detail, type Cluster } from "../lib/api";
  import { age, ageSeconds, errorView, type APIError } from "../lib/state";
  import {
    conditionsFor,
    conflictChanged,
    deletePlan,
    endpoints,
    gaps,
    instances,
    logicals,
    refreshedDeletePlan,
    upgradePlan,
    attention as attentionMark,
    bad as badMark,
  } from "../lib/mock";
  import { follow, path } from "../lib/router.svelte";
  import Conditions from "../lib/ui/Conditions.svelte";
  import Dialog from "../lib/ui/Dialog.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Mocked from "../lib/ui/Mocked.svelte";
  import Pair from "../lib/ui/Pair.svelte";
  import Panel from "../lib/ui/Panel.svelte";

  let { id, onOpenTopology }: { id: string; onOpenTopology: (id: string) => void } = $props();

  let cluster = $state<Cluster | null>(null);
  let failure = $state<APIError | null>(null);
  let readAt = $state<Date | null>(null);
  let reading = $state(false);
  const now = new Date();

  // The destructive flow. The API serves no plan route, so confirming here never
  // sends a request; it demonstrates the refresh that a revision conflict forces.
  let plan = $state<"delete" | "upgrade" | null>(null);
  let conflicted = $state(false);
  let typed = $state("");

  async function read() {
    const wanted = id;
    reading = true;
    try {
      const answer = await api.cluster(wanted);
      if (wanted !== id) return;
      cluster = answer;
      failure = null;
      readAt = new Date();
    } catch (error) {
      // A failed read proves nothing about the database, so the last answer stays.
      if (wanted === id) failure = detail(error);
    } finally {
      if (wanted === id) reading = false;
    }
  }

  $effect(() => {
    cluster = null;
    failure = null;
    readAt = null;
    read();
  });

  // Desired state against observed state. The API carries no observation time, so
  // the row write time stands in for one and the screen says so below.
  let divergence = $derived(
    cluster === null
      ? null
      : {
          desired: cluster.desired_lifecycle,
          desiredRevision: cluster.desired_revision,
          observed: cluster.observed_lifecycle,
          observedRevision: cluster.observed_revision,
          observedAt: cluster.updated_at,
        },
  );

  let shown = $derived(plan === "delete" ? (conflicted ? refreshedDeletePlan : deletePlan) : upgradePlan);
  let named = $derived(cluster?.spec.name ?? id);
  let confirmable = $derived(typed.trim() === named);

  function close() {
    plan = null;
    conflicted = false;
    typed = "";
  }
</script>

<div class="screen">
  <header>
    <div class="titles">
      <div class="name-row">
        <h1 class="mono">{named}</h1>
        {#if cluster}
          <span class="mono subtitle">{cluster.id} · {cluster.spec.engine}</span>
        {/if}
      </div>
      {#if divergence}
        <Pair value={divergence} {now} />
      {/if}
    </div>
    <div class="actions">
      <a class="mono link" href={path("topology", id)} onclick={(event) => follow(event, () => onOpenTopology(id))}>
        Topology
      </a>
      <button type="button" onclick={read} disabled={reading}>{reading ? "Reading" : "Read again"}</button>
      <button type="button" class="quiet" onclick={() => (plan = "upgrade")}>Upgrade…</button>
      <button type="button" class="quiet" onclick={() => (plan = "delete")}>Delete…</button>
    </div>
  </header>

  {#if failure}
    <ErrorPanel view={errorView(failure)} />
  {/if}

  {#if cluster}
    <p class="note">
      The API returns no observation time, so the verdict above measures freshness from the row
      write time <span class="mono">{cluster.updated_at}</span>. That is a write, not an
      observation. Gap: <span class="mono">{gaps.observedAt}</span>.
      {#if readAt}Read {age(ageSeconds(readAt.toISOString(), now))} ago.{/if}
    </p>
  {:else if !failure}
    <p class="note">Reading the cluster.</p>
  {/if}

  <Panel title="Conditions" note="five independent values, each with reason · observed_at · observed_revision">
    <div class="mock-strip"><Mocked gap={gaps.conditions} /></div>
    <Conditions reported={conditionsFor(named, now)} {now} />
    {#snippet foot()}
      This cluster is not green, not red, and not amber. Each condition is its own fact, and the
      screen never averages them into one.
    {/snippet}
  </Panel>

  <div class="columns">
    <Panel title={`Instances · ${instances.length}`}>
      <div class="mock-strip"><Mocked gap={gaps.instances} /></div>
      {#each instances as instance (instance.name)}
        <div class="instance" class:hatched={instance.stale}>
          <div class="instance-head">
            <Mark mark={instance.mark} text={false} />
            <span class="mono name">{instance.name}</span>
            <span class="badge mono" style="border-color: var(--{instance.roleTone}); color: var(--{instance.roleTone})">
              {instance.role}
            </span>
            <span class="mono node">{instance.node}</span>
          </div>
          <dl class="facts mono">
            <dt>observed_role</dt><dd>{instance.observedRole}</dd>
            <dt>volume</dt><dd>{instance.volume}</dd>
            <dt>image</dt><dd>{instance.image}</dd>
            <dt>ports</dt><dd>{instance.ports}</dd>
            <dt>cpu / mem</dt><dd>{instance.usage}</dd>
          </dl>
        </div>
      {/each}
      {#snippet foot()}
        Roles are the engine's own — Synod reports what Patroni says, it does not elect. Routers,
        poolers, and Sentinels are service components and are listed separately.
      {/snippet}
    </Panel>

    <div class="stack">
      <Panel title="Endpoints">
        <div class="mock-strip"><Mocked gap={gaps.instances} /></div>
        {#each endpoints as endpoint (endpoint.purpose)}
          <div class="endpoint">
            <div class="endpoint-head">
              <Mark mark={endpoint.mark} text={false} />
              <span class="mono purpose">{endpoint.purpose}</span>
              <span class="chip mono">{endpoint.mode}</span>
            </div>
            <p class="mono address">{endpoint.address}</p>
            <p class="mono profile">{endpoint.profile}</p>
          </div>
        {/each}
        {#snippet foot()}
          Client mode is stated because it changes application behaviour:
          <span class="mono">single-host</span>, <span class="mono">sentinel-aware</span>, and
          <span class="mono">cluster-aware</span> clients are not interchangeable. Credentials never
          appear here.
        {/snippet}
      </Panel>

      <Panel title="Logical databases &amp; roles">
        <div class="mock-strip"><Mocked gap={gaps.instances} /></div>
        {#each logicals as logical (logical.name)}
          <div class="row">
            <span class="mono kind">{logical.kind}</span>
            <span class="mono grow">{logical.name}</span>
            <span class="mono meta">{logical.meta}</span>
          </div>
        {/each}
        {#snippet foot()}
          Creating either one creates an operation on this cluster — the list shows a pending mark
          until that operation reports.
        {/snippet}
      </Panel>
    </div>
  </div>
</div>

{#if plan}
  <Dialog
    label="Confirm destructive action"
    title={shown.title}
    mark={attentionMark("destructive")}
    placement="centre"
    danger
    onClose={close}
  >
    <div class="mock-strip"><Mocked gap={gaps.plans} /></div>

    {#if conflicted}
      <!-- The previous plan token was rejected, not silently approved. -->
      <div class="conflict hatched">
        <div class="conflict-head">
          <Mark mark={badMark("conflict")} text={false} />
          <span class="mono">revision_conflict · HTTP 412 · plan refreshed</span>
        </div>
        <p class="conflict-body">
          The cluster changed while this dialog was open, and the effects of this plan changed with
          it. The previous plan token was rejected, not silently approved. Read the refreshed plan
          and confirm again.
        </p>
        <p class="mono conflict-changed">{conflictChanged}</p>
      </div>
    {/if}

    <section class="block">
      <span class="label">Exact target</span>
      <dl class="facts mono">
        {#each shown.facts as fact (fact.key)}
          <dt>{fact.key}</dt>
          <dd style={fact.tone ? `color: var(--${fact.tone})` : undefined}>{fact.value}</dd>
        {/each}
      </dl>
    </section>

    <section class="block">
      <span class="label">What happens, in order</span>
      {#each shown.steps as step (step.ordinal)}
        <div class="plan-step">
          <span class="mono ordinal">{step.ordinal}</span>
          <span class="text">{step.text}</span>
          <span class="mono meta">{step.meta}</span>
        </div>
      {/each}
    </section>

    <section class="irreversible">
      <div class="irreversible-head">
        <Mark mark={attentionMark("irreversible")} text={false} />
        <span class="mono">{shown.irreversibleTitle}</span>
      </div>
      <p class="irreversible-body">{shown.irreversibleBody}</p>
      {#each shown.irreversibleFacts as fact (fact)}
        <p class="mono irreversible-fact">{fact}</p>
      {/each}
    </section>

    <section class="block">
      <label for="confirm-name" class="confirm-label">
        Type the cluster name to confirm this plan. The typed name is bound to plan token
        <span class="mono">{shown.token}</span>.
      </label>
      <input id="confirm-name" bind:value={typed} placeholder={named} />
      <span class="hint mono">an expired plan is rejected, never approved</span>
    </section>

    <div class="buttons">
      <button
        type="button"
        class="destructive"
        disabled={!confirmable}
        onclick={() => (conflicted = !conflicted)}
      >
        {shown.confirmLabel}
      </button>
      <button type="button" onclick={close}>Cancel</button>
      <span class="note">
        Confirming returns 202 and an operation ID. It does not mean the action finished. The API
        serves no plan route yet, so this button refreshes the plan rather than sending one.
      </span>
    </div>
  </Dialog>
{/if}

<style>
  .screen {
    display: flex;
    flex-direction: column;
    gap: 24px;
    max-width: 1240px;
  }
  header {
    display: flex;
    flex-wrap: wrap;
    gap: 20px;
    align-items: flex-start;
  }
  .titles {
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .name-row {
    display: flex;
    align-items: baseline;
    gap: 12px;
    flex-wrap: wrap;
  }
  .subtitle {
    font-size: 11px;
    color: var(--ink-faint);
  }
  .actions {
    margin-left: auto;
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    align-items: center;
  }
  .link {
    font-size: 11px;
  }

  .mock-strip {
    padding: 12px 16px 0;
  }

  .columns {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
    gap: 20px;
    align-items: start;
  }
  .stack {
    display: flex;
    flex-direction: column;
    gap: 20px;
  }

  .instance,
  .endpoint {
    padding: 13px 16px;
    border-top: 1px solid var(--line);
  }
  .instance-head,
  .endpoint-head {
    display: flex;
    align-items: center;
    gap: 9px;
    flex-wrap: wrap;
  }
  .name,
  .purpose {
    font-size: 12px;
    font-weight: 500;
  }
  .badge {
    font-size: 10px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    border: 1px solid;
    padding: 2px 7px;
  }
  .node {
    margin-left: auto;
    font-size: 11px;
    color: var(--ink-dim);
  }
  .facts {
    display: grid;
    grid-template-columns: auto 1fr;
    row-gap: 3px;
    column-gap: 12px;
    margin: 8px 0 0;
    font-size: 10px;
  }
  .facts dt {
    color: var(--ink-faint);
  }
  .facts dd {
    margin: 0;
    color: var(--ink-body);
  }
  .address {
    margin: 6px 0 0;
    font-size: 11px;
    color: var(--ink-body);
    word-break: break-all;
  }
  .profile {
    margin: 4px 0 0;
    font-size: 10px;
    color: var(--ink-dim);
    line-height: 1.5;
  }
  .chip {
    font-size: 10px;
    border: 1px solid var(--line-strong);
    color: var(--ink-muted);
    padding: 2px 6px;
  }
  .kind {
    width: 68px;
    flex-shrink: 0;
    font-size: 11px;
    color: var(--ink-muted);
  }
  .grow {
    flex: 1;
    min-width: 0;
    font-size: 12px;
  }
  .meta {
    font-size: 11px;
    color: var(--ink-dim);
  }

  .conflict {
    border: 1px solid var(--fail);
    padding: 14px;
    display: flex;
    flex-direction: column;
    gap: 9px;
  }
  .conflict-head {
    display: flex;
    align-items: center;
    gap: 9px;
    font-size: 11px;
    color: var(--fail);
  }
  .conflict-body {
    margin: 0;
    font-size: 12px;
    color: var(--ink);
    line-height: 1.55;
  }
  .conflict-changed {
    margin: 0;
    font-size: 10px;
    color: var(--ink-muted);
  }

  .block {
    display: flex;
    flex-direction: column;
    gap: 9px;
  }
  .block .facts {
    font-size: 11px;
    row-gap: 5px;
    column-gap: 14px;
    margin: 0;
  }
  .plan-step {
    display: flex;
    gap: 10px;
    align-items: baseline;
  }
  .ordinal {
    font-size: 11px;
    color: var(--ink-faint);
    width: 16px;
  }
  .plan-step .text {
    flex: 1;
    font-size: 12px;
    color: var(--ink-body);
    line-height: 1.55;
  }
  .plan-step .meta {
    font-size: 10px;
    white-space: nowrap;
  }

  .irreversible {
    border: 1px solid var(--danger-line);
    padding: 14px;
    display: flex;
    flex-direction: column;
    gap: 9px;
  }
  .irreversible-head {
    display: flex;
    align-items: center;
    gap: 9px;
    font-size: 11px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--fail);
  }
  .irreversible-body {
    margin: 0;
    font-size: 12px;
    color: var(--ink);
    line-height: 1.6;
  }
  .irreversible-fact {
    margin: 0;
    font-size: 11px;
    color: var(--ink-body);
    line-height: 1.55;
  }

  .confirm-label {
    font-size: 12px;
    color: var(--ink-body);
    line-height: 1.5;
  }
  .hint {
    font-size: 10px;
    color: var(--ink-faint);
  }
  .buttons {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    align-items: center;
  }
  .buttons .note {
    flex: 1;
    min-width: 200px;
    font-size: 11px;
  }
</style>
