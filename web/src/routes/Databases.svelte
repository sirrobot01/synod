<script lang="ts">
  import { api, detail, type Admission } from "../lib/api";
  import { age, errorView, freshness, desiredMark, observedMark, verdict, verdictView, type APIError } from "../lib/state";
  import { clusters, gaps, topologyOptions } from "../lib/mock";
  import { follow, path } from "../lib/router.svelte";
  import Conditions from "../lib/ui/Conditions.svelte";
  import Dialog from "../lib/ui/Dialog.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Mocked from "../lib/ui/Mocked.svelte";
  import Panel from "../lib/ui/Panel.svelte";

  let { onOpen, onOpenOperation }: { onOpen: (id: string) => void; onOpenOperation: (id: string) => void } = $props();

  const now = new Date();
  const fleet = clusters(now);

  let creating = $state(false);
  let lookup = $state("");
  let name = $state("");
  let engine = $state<"postgres" | "redis">("postgres");
  let version = $state("17.11");
  let persistence = $state<"none" | "rdb" | "aof" | "rdb+aof">("rdb");
  let busy = $state(false);
  let failure = $state<APIError | null>(null);
  let admission = $state<Admission | null>(null);
  // The idempotency key belongs to one request body. It survives a failed attempt,
  // so a retry replays the accepted work instead of admitting a second database.
  // Any edit to the body discards it, because a new body needs a new key.
  let key = $state("");

  // This server admits a standalone topology only. The other shapes are drawn, so
  // an operator can see what the product will offer, and refused, so no request
  // can carry a topology the server would answer unsupported_spec for.
  const admitted = "standalone";
  let topologies = $derived(topologyOptions[engine]);

  /**
   * requestKey makes one idempotency key. crypto.randomUUID needs a secure
   * context, and an installation reached over plain HTTP has none, so the key is
   * built from getRandomValues instead. That call works in every context.
   */
  function requestKey(): string {
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  }

  function edited() {
    key = "";
    admission = null;
  }

  function open(event: SubmitEvent) {
    event.preventDefault();
    const id = lookup.trim();
    if (id !== "") onOpen(id);
  }

  function close() {
    creating = false;
    admission = null;
    failure = null;
  }

  async function create(event: SubmitEvent) {
    event.preventDefault();
    if (key === "") key = requestKey();
    busy = true;
    failure = null;
    const spec =
      engine === "postgres"
        ? { schema_version: 1, name: name.trim(), engine, postgresql: { version: version.trim(), topology: admitted }, redis: null }
        : { schema_version: 1, name: name.trim(), engine, postgresql: null, redis: { version: version.trim(), topology: admitted, persistence } };
    try {
      admission = await api.createCluster(spec, key);
      // The work is accepted, so this body is spent. A later create needs a new key.
      key = "";
    } catch (error) {
      // The key stays, so pressing the button again replays the same request.
      failure = detail(error);
    } finally {
      busy = false;
    }
  }
</script>

<div class="screen">
  <header>
    <div class="titles">
      <h1>Clusters</h1>
      <p class="note">
        PostgreSQL and Redis. Every mutation here returns an operation, never an instant result.
      </p>
    </div>
    <button type="button" class="primary" onclick={() => (creating = true)}>Create cluster</button>
  </header>

  <Panel title="Open a cluster" note="this reads the API">
    <form class="lookup" onsubmit={open}>
      <label for="cluster-id" class="label">Cluster ID</label>
      <input id="cluster-id" bind:value={lookup} placeholder="cl_31d0aa47" />
      <button type="submit">Open</button>
    </form>
  </Panel>

  <Mocked gap={gaps.collections} />

  <Panel title="All clusters">
    <div class="scroll">
      <div class="grid clusters">
        <div class="head">Cluster</div>
        <div class="head">Desired / observed</div>
        <div class="head">Topology</div>
        <div class="head">Size</div>
        <div class="head">Conditions</div>
        <div class="head">Updated</div>
        {#each fleet as cluster (cluster.id)}
          {@const value = {
            desired: cluster.desired,
            desiredRevision: cluster.desiredRevision,
            observed: cluster.observed,
            observedRevision: cluster.observedRevision,
            observedAt: new Date(now.getTime() - cluster.observedAgeSeconds * 1000).toISOString(),
            ...(cluster.owningOperation ? { owningOperation: cluster.owningOperation } : {}),
          }}
          {@const result = verdict(value, now)}
          {@const view = verdictView(result)}
          {@const expired = freshness(cluster.observedAgeSeconds) === "expired"}
          {@const seen = observedMark(expired ? "unknown" : cluster.observed)}
          <div class="cell stack">
            <a
              class="mono name"
              href={path("cluster", cluster.id)}
              onclick={(event) => follow(event, () => onOpen(cluster.id))}>{cluster.name}</a
            >
            <span class="mono id">{cluster.id}</span>
          </div>
          <div
            class="cell stack pair"
            class:ruled={result !== "converged"}
            class:dashed={view.dashed}
            style="--rule: var(--{view.tone})"
          >
            <span class="mono desired">
              {desiredMark(cluster.desired).glyph} {cluster.desired} · rev {cluster.desiredRevision}
            </span>
            <span class="mono observed">{seen.glyph} {seen.label} · {age(cluster.observedAgeSeconds)}</span>
          </div>
          <div class="cell">{cluster.topology}</div>
          <div class="cell">{cluster.size}</div>
          <div class="cell"><Conditions reported={cluster.conditions} {now} compact /></div>
          <div class="cell updated">{age(cluster.observedAgeSeconds)}</div>
        {/each}
      </div>
    </div>
    {#snippet foot()}
      The condition strip uses fixed positions — Ready, Reachable, Replicated, BackupCurrent,
      EndpointReady — so the third mark is always replication whether it is true, false, or
      unknown. Hovering names it; the cluster screen states reason, observation time, and revision
      in text.
    {/snippet}
  </Panel>
</div>

{#if creating}
  <Dialog
    label="Create cluster"
    title="Create cluster"
    note={admission ? "step 2 of 2 · acceptance" : "step 1 of 2 · specification"}
    onClose={close}
  >
    {#if admission}
      {@const accepted = admission}
      <!-- Acceptance is its own state. It makes no claim about any database. -->
      <div class="accepted" role="status">
        <p class="headline mono">202 accepted — not created yet</p>
        <p class="body">
          The request is durably recorded.{accepted.replayed ? " It replayed an earlier request." : ""}
          No container, volume, or database exists yet. Nothing about
          <span class="mono">{name.trim()}</span> is claimed until its operation reports.
        </p>
        <div class="facts mono">
          <span class="key">operation_id</span><span>{accepted.operation_id}</span>
          <span class="key">cluster_id</span><span>{accepted.cluster_id}</span>
          <span class="key">desired_revision</span><span>{accepted.desired_revision}</span>
          <span class="key">accepted_at</span><span>{accepted.accepted_at || "not reported"}</span>
          <span class="key">replayed</span><span>{accepted.replayed}</span>
        </div>
        <p class="note">
          The operation ID is written into the address, so a reload returns to this operation
          rather than to an empty list. It is not kept in browser storage.
        </p>
      </div>
      <div class="buttons">
        <a
          class="mono follow"
          href={path("operation", accepted.operation_id)}
          onclick={(event) => follow(event, () => onOpenOperation(accepted.operation_id))}
        >
          Follow {accepted.operation_id}
        </a>
        <button type="button" onclick={close}>Back to clusters</button>
      </div>
    {:else}
      <form class="create" onsubmit={create}>
        <div class="field">
          <label for="name" class="label">Name — scoped to this installation</label>
          <input id="name" bind:value={name} oninput={edited} placeholder="pg-orders" required />
          <span class="hint mono">lower case letters, digits, and hyphens · must start with a letter</span>
        </div>

        <fieldset>
          <legend class="label">Engine &amp; version</legend>
          <div class="engines">
            <button
              type="button"
              class="choice"
              class:picked={engine === "postgres"}
              aria-pressed={engine === "postgres"}
              onclick={() => {
                engine = "postgres";
                version = "17.11";
                edited();
              }}
            >
              <span class="mono choice-name">PostgreSQL</span>
            </button>
            <button
              type="button"
              class="choice"
              class:picked={engine === "redis"}
              aria-pressed={engine === "redis"}
              onclick={() => {
                engine = "redis";
                version = "8.2.7";
                edited();
              }}
            >
              <span class="mono choice-name">Redis</span>
            </button>
          </div>
          <div class="field">
            <label for="version" class="label">Version</label>
            <input id="version" bind:value={version} oninput={edited} required />
            <span class="hint">
              This server accepts a fixed set of versions. It answers
              <span class="mono">unsupported_spec</span> for any other version, before any work
              starts. The design offers only qualified versions; the API serves no catalogue to
              offer from, so this stays a field. Gap: <span class="mono">{gaps.versions}</span>.
            </span>
          </div>
          {#if engine === "redis"}
            <div class="field">
              <label for="persistence" class="label">Persistence</label>
              <select id="persistence" bind:value={persistence} onchange={edited}>
                <option value="none">none</option>
                <option value="rdb">rdb</option>
                <option value="aof">aof</option>
                <option value="rdb+aof">rdb+aof</option>
              </select>
            </div>
          {/if}
        </fieldset>

        <fieldset>
          <legend class="label">Topology</legend>
          <!-- Every shape is drawn, so the client mode each one forces is visible.
               Only the admitted one can be chosen; the rest state why not. -->
          {#each topologies as option (option.key)}
            {@const available = option.key === admitted}
            <div class="topology" class:picked={available} aria-disabled={!available}>
              <div class="topology-head">
                <span class="radio" class:on={available} aria-hidden="true"></span>
                <span class="mono choice-name">{option.label}</span>
                <span class="mono nodes">{option.nodes}</span>
              </div>
              <p class="detail">{option.detail}</p>
              <p class="client mono">{option.client}</p>
              {#if !available}
                <p class="hint mono">not admitted by this server yet</p>
              {/if}
            </div>
          {/each}
        </fieldset>

        {#if failure}
          <ErrorPanel
            view={errorView(failure)}
            footnote={key !== ""
              ? `The request keeps its identity ${key}. Press Create again to repeat the same request. It cannot admit a second cluster.`
              : ""}
          />
        {/if}

        <div class="buttons">
          <button type="submit" class="primary" disabled={busy}>{busy ? "Sending" : "Create — returns 202"}</button>
          <button type="button" onclick={close}>Cancel</button>
          <span class="note">
            Creation is asynchronous: acceptance returns an operation ID, and the operation reports
            what actually happened.
          </span>
        </div>
      </form>
    {/if}
  </Dialog>
{/if}

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
  header button {
    margin-left: auto;
  }

  .lookup {
    display: flex;
    gap: 10px;
    align-items: center;
    flex-wrap: wrap;
    padding: 16px;
  }
  .lookup input {
    min-width: 320px;
  }

  .grid.clusters {
    grid-template-columns:
      minmax(170px, 1.1fr) minmax(180px, 1fr) minmax(150px, 0.9fr)
      minmax(130px, 0.8fr) minmax(180px, 1fr) minmax(110px, 0.6fr);
    min-width: 1000px;
  }
  .name {
    font-size: 12px;
    font-weight: 500;
  }
  .id {
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
  .updated {
    color: var(--ink-muted);
  }

  .create {
    display: flex;
    flex-direction: column;
    gap: 22px;
  }
  .field {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .hint {
    font-size: 11px;
    color: var(--ink-dim);
    line-height: 1.5;
  }
  fieldset {
    border: 1px solid var(--line);
    margin: 0;
    padding: 14px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  legend {
    padding: 0 6px;
  }
  .engines {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
  }
  .choice {
    padding: 11px 14px;
    border: 1px solid var(--line-strong);
    color: var(--ink);
  }
  .choice.picked {
    background: var(--head);
    border-color: var(--ink);
  }
  .choice-name {
    font-size: 12px;
  }
  .topology {
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 11px 12px;
    border: 1px solid var(--line);
  }
  .topology.picked {
    background: var(--head);
    border-color: var(--line-strong);
  }
  .topology[aria-disabled="true"] {
    opacity: 0.62;
  }
  .topology-head {
    display: flex;
    align-items: center;
    gap: 9px;
  }
  .radio {
    width: 12px;
    height: 12px;
    border-radius: 50%;
    border: 1.5px solid var(--ink-faint);
    flex-shrink: 0;
  }
  .radio.on {
    border-color: var(--ink);
    background: radial-gradient(circle, var(--ink) 0 40%, transparent 42%);
  }
  .nodes {
    margin-left: auto;
    font-size: 10px;
    color: var(--ink-dim);
  }
  .detail {
    margin: 0;
    font-size: 11px;
    color: var(--ink-soft);
    line-height: 1.5;
    padding-left: 21px;
  }
  .client {
    margin: 0;
    font-size: 10px;
    color: var(--ink-dim);
    padding-left: 21px;
  }
  .topology .hint {
    padding-left: 21px;
  }

  .buttons {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    align-items: center;
  }
  .buttons .note {
    flex: 1;
    min-width: 180px;
    font-size: 11px;
  }

  .accepted {
    border: 1px solid var(--active);
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .accepted p {
    margin: 0;
  }
  .headline {
    font-size: 12px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--active);
  }
  .accepted .body {
    font-size: 13px;
    line-height: 1.6;
    color: var(--ink-body);
  }
  .facts {
    display: grid;
    grid-template-columns: auto 1fr;
    row-gap: 5px;
    column-gap: 12px;
    font-size: 11px;
  }
  .facts .key {
    color: var(--ink-faint);
  }
  .follow {
    font-size: 12px;
  }
</style>
