<script lang="ts">
  import { api, detail, Failure, type Cluster, type ClusterMember } from "../lib/api";
  import { age, ageSeconds, errorView, type APIError } from "../lib/state";
  import {
    conditionsFor,
    gaps,
    logicals,
    upgradePlan,
    attention as attentionMark,
  } from "../lib/mock";
  import { follow, path } from "../lib/router.svelte";
  import ClusterActions from "../lib/ClusterActions.svelte";
  import ClusterConnection from "../lib/ClusterConnection.svelte";
  import Conditions from "../lib/ui/Conditions.svelte";
  import Dialog from "../lib/ui/Dialog.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Mocked from "../lib/ui/Mocked.svelte";
  import Pair from "../lib/ui/Pair.svelte";
  import Panel from "../lib/ui/Panel.svelte";

  let { id, actorID, onOpenTopology, onOpenOperation }: { id: string; actorID: string; onOpenTopology: (id: string) => void; onOpenOperation: (id: string) => void } = $props();

  let cluster = $state<Cluster | null>(null);
  let failure = $state<APIError | null>(null);
  let readAt = $state<Date | null>(null);
  let reading = $state(false);
  let members = $state<ClusterMember[] | null>(null);
  let memberFailure = $state<APIError | null>(null);
  const now = new Date();
  let readGeneration = 0;

  // Upgrade remains a labelled preview until its public API is available.
  let plan = $state<"upgrade" | null>(null);
  let typed = $state("");

  async function read() {
    const wanted = id;
    const generation = ++readGeneration;
    reading = true;
    members = null;
    memberFailure = null;
    void readMembers(wanted, generation);
    try {
      const answer = await api.cluster(wanted);
      if (wanted !== id || generation !== readGeneration) return;
      if (answer.id !== wanted) throw new Failure(200, { code: "invalid_response", message: "The API returned a different cluster.", requestID: "" });
      cluster = answer;
      failure = null;
      readAt = new Date();
    } catch (error) {
      // Keep the last answer on a transient failure. A missing resource clears it.
      if (wanted === id && generation === readGeneration) {
        failure = detail(error);
        if (error instanceof Failure && error.status === 404) cluster = null;
      }
    } finally {
      if (wanted === id && generation === readGeneration) reading = false;
    }
  }

  async function readMembers(target: string, generation: number) {
    try {
      const answer = await api.clusterMembers(target);
      if (target === id && generation === readGeneration) members = answer;
    } catch (error) {
      if (target === id && generation === readGeneration) memberFailure = detail(error);
    }
  }

  $effect(() => {
    cluster = null;
    failure = null;
    readAt = null;
    void read();
    return () => { readGeneration++; };
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

  const shown = upgradePlan;
  let named = $derived(cluster?.spec.name ?? id);

  function close() {
    plan = null;
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
    </div>
  </header>

  <ClusterActions {id} {actorID} {cluster} {members} {onOpenOperation} onRefresh={read} />

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
    <Panel title={`Stored members${members ? ` · ${members.length}` : ""}`} note="Declared roles and stored membership state">
      {#if memberFailure}
        <ErrorPanel view={errorView(memberFailure)} />
      {:else if members === null}
        <p class="member-note">Reading stored members.</p>
      {:else if members.length === 0}
        <p class="member-note">No stored members. This does not prove that no native process exists.</p>
      {:else}
        {#each members as member (member.id)}
          <div class="instance">
            <dl class="facts mono">
              <dt>member_id</dt><dd>{member.id}</dd>
              <dt>declared_role</dt><dd>{member.role}</dd>
              <dt>stored_state</dt><dd>{member.state}</dd>
            </dl>
          </div>
        {/each}
      {/if}
      {#snippet foot()}
        These records contain no native observation time. Current role, replication link, and resource use are unavailable.
        Active means a checked operation proved membership earlier. Residual means a failed operation left this member recorded.
      {/snippet}
    </Panel>

    <div class="stack">
      <ClusterConnection {id} />

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
        disabled
        title="Upgrade is not available yet"
      >
        Upgrade unavailable
      </button>
      <button type="button" onclick={close}>Cancel</button>
      <span class="note">
        This is an example upgrade plan. Upgrade is not available yet. This preview does not submit work.
      </span>
    </div>
  </Dialog>
{/if}

<style>
  .member-note { padding: 12px 14px; font-size: 12px; color: var(--ink-body); }
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

  .instance {
    padding: 13px 16px;
    border-top: 1px solid var(--line);
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
