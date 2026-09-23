<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type Admission, type Cluster } from "../lib/api";
  import { errorView, type APIError } from "../lib/state";
  import { gaps, topologyOptions } from "../lib/mock";
  import { follow, path } from "../lib/router.svelte";
  import Dialog from "../lib/ui/Dialog.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Panel from "../lib/ui/Panel.svelte";

  let { onOpen, onOpenOperation }: { onOpen: (id: string) => void; onOpenOperation: (id: string) => void } = $props();

  let fleet = $state<Cluster[]>([]);
  let nextCursor = $state("");
  let includeDeleted = $state(false);
  let reading = $state(false);
  let listFailure = $state<APIError | null>(null);
  let generation = 0;

  async function readClusters(reset = false) {
    if (reset) {
      generation++;
      fleet = [];
      nextCursor = "";
    } else if (reading) return;
    const current = generation;
    const cursor = nextCursor;
    const deleted = includeDeleted;
    reading = true;
    listFailure = null;
    try {
      const page = await api.clusters(cursor, deleted);
      if (current !== generation) return;
      const merged = new Map(fleet.map((cluster) => [cluster.id, cluster]));
      for (const cluster of page.items) merged.set(cluster.id, cluster);
      fleet = [...merged.values()];
      nextCursor = page.next_cursor;
    } catch (error) {
      if (current === generation) listFailure = detail(error);
    } finally {
      if (current === generation) reading = false;
    }
  }

  onMount(() => {
    void readClusters(true);
    return () => { generation++; };
  });

  let creating = $state(false);
  let lookup = $state("");
  let name = $state("");
  let engine = $state<"postgres" | "redis">("postgres");
  let version = $state("17.11");
  let replicationPolicy = $state<"asynchronous" | "synchronous">("asynchronous");
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
        ? { schema_version: 1, name: name.trim(), engine, postgresql: { version: version.trim(), topology: admitted, replication_policy: replicationPolicy }, redis: null }
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
      <input id="cluster-id" bind:value={lookup} placeholder="Cluster UUID" />
      <button type="submit">Open</button>
    </form>
  </Panel>

  <Panel title="All clusters" note={`${fleet.length} loaded`}>
    {#snippet actions()}
      <label class="filter"><input type="checkbox" bind:checked={includeDeleted} onchange={() => readClusters(true)} /> Show deleted</label>
      <button type="button" onclick={() => readClusters(true)}>Refresh</button>
    {/snippet}
    {#if reading}<p class="list-message" role="status">Loading clusters…</p>{/if}
    {#if listFailure}
      <div class="list-message">
        <ErrorPanel view={{ ...errorView(listFailure), consequence: listFailure.message }} />
        <button type="button" onclick={() => readClusters()}>Retry list read</button>
      </div>
    {:else if !reading && fleet.length === 0}
      <p class="list-message">No clusters found{includeDeleted ? "." : " with completed deletions excluded."}</p>
    {/if}
    {#if fleet.length > 0}
      <div class="scroll">
        <div class="grid clusters">
          <div class="head">Cluster</div>
          <div class="head">Desired / stored observation</div>
          <div class="head">Configured engine</div>
          <div class="head">Configured topology</div>
          <div class="head">Native observation</div>
          <div class="head">Row updated</div>
          {#each fleet as cluster (cluster.id)}
            {@const configured = cluster.spec.engine === "postgres" ? cluster.spec.postgresql : cluster.spec.redis}
            <div class="cell stack">
              <a class="mono name" href={path("cluster", cluster.id)}
                onclick={(event) => follow(event, () => onOpen(cluster.id))}>{cluster.spec.name}</a>
              <span class="mono id">{cluster.id}</span>
              {#if cluster.deleted_at}<span class="note">Deleted record</span>{/if}
            </div>
            <div class="cell stack">
              <span class="mono desired">Desired: {cluster.desired_lifecycle} · rev {cluster.desired_revision}</span>
              <span class="mono observed">Stored: {cluster.observed_lifecycle} · rev {cluster.observed_revision}</span>
            </div>
            <div class="cell stack">
              <span>{cluster.spec.engine === "postgres" ? "PostgreSQL" : "Redis"} {configured.version}</span>
              {#if cluster.spec.engine === "redis"}<span class="note">Persistence: {cluster.spec.redis.persistence}</span>{:else}<span class="note">Replication: {cluster.spec.postgresql.replication_policy || "asynchronous"}</span>{/if}
            </div>
            <div class="cell">{configured.topology}</div>
            <div class="cell note">Health and observation time unavailable</div>
            <div class="cell updated"><time datetime={cluster.updated_at}>{cluster.updated_at}</time></div>
          {/each}
        </div>
      </div>
    {/if}
    {#if nextCursor !== "" && !listFailure}
      <div class="list-message"><button type="button" disabled={reading} onclick={() => readClusters()}>Load more</button></div>
    {/if}
    {#snippet foot()}
      These are stored lifecycle states, not current database health. Row update time is not an observation time.
      The loaded count covers these pages only. Changes can affect later pages. Refresh to see newer clusters.
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
        <p class="headline mono">202 accepted — inspect the operation</p>
        <p class="body">
          The request is durably recorded.{accepted.replayed ? " It replayed an earlier request." : ""}
          Acceptance does not prove that provisioning is complete. Nothing about
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
          {#if engine === "postgres"}
            <div class="field">
              <label for="replication-policy" class="label">Replication policy</label>
              <select id="replication-policy" bind:value={replicationPolicy} onchange={edited}>
                <option value="asynchronous">Asynchronous</option>
                <option value="synchronous">Synchronous (one replica)</option>
              </select>
              <span class="hint">
                {#if replicationPolicy === "synchronous"}
                  Commits wait for one replica. This cluster starts without replicas; add one before writing.
                  This policy currently requires PostgreSQL 17.11.
                {:else}
                  Commits do not wait for replicas. Recent writes can be lost if the primary is lost.
                {/if}
              </span>
            </div>
          {/if}
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
  .list-message {
    margin: 0;
    padding: 16px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
  }
  .filter {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
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
