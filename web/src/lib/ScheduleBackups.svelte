<script lang="ts">
  import { untrack } from "svelte";
  import { api, detail, uncertain, type AppPlan, type CopySettings, type Destination, type Node, type ProtectionPlan, type ProtectionUpdate } from "./api";
  import { errorView, type APIError } from "./state";
  import { bytes } from "./format";
  import { paths } from "./model";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";
  import AppPaths from "./AppPaths.svelte";
  import ScheduleFields from "./ScheduleFields.svelte";

  // Schedules daily backups for many apps. The reader reviews the paths of each app before the save.
  let { plan, destinations, nodes, onClose, onSaved }: { plan: ProtectionPlan; destinations: Destination[]; nodes: Node[]; onClose: () => void; onSaved: () => void } = $props();

  // The reviewed revision is captured once. A concurrent change needs a new review.
  const revision = untrack(() => plan.revision);
  const approved = (item: AppPlan) => nodes.some((n) => n.id === item.app.node_id && n.approved_at);
  // Apps that can be scheduled come first. Scheduled apps follow for reference.
  const rank = (item: AppPlan) => (item.enabled ? 2 : approved(item) ? 0 : 1);
  const apps = untrack(() => plan.apps.filter((item) => item.app.present).sort((a, b) => rank(a) - rank(b)));
  let chosen = $state<string[]>(untrack(() => apps.filter((item) => !item.enabled && approved(item)).map((item) => item.app.id)));
  let settings = $state<CopySettings>(untrack(() => {
    const value = structuredClone($state.snapshot(plan.defaults));
    delete value.include_large;
    if (value.destinations.length === 0) {
      const shared = destinations.filter((d) => d.location.kind !== "local");
      value.destinations = (shared.length ? shared : destinations).map((d) => d.id);
    }
    return value;
  }));
  let later = $state(false);
  let editing = $state(false);
  let review = $state(false);
  let busy = $state(false);
  let changed = $state(false);
  let failure = $state<APIError | null>(null);
  let submitted = $state<ProtectionUpdate | null>(null);

  let open = $derived(apps.filter((item) => !item.enabled && approved(item)));
  let picked = $derived(apps.filter((item) => chosen.includes(item.app.id)));
  let every = $derived(open.length > 0 && open.every((item) => chosen.includes(item.app.id)));
  let names = $derived(settings.destinations.map((id) => destinations.find((d) => d.id === id)?.name).filter(Boolean).join(", "));
  let incompatible = $derived(picked.some((item) => settings.destinations.some((id) => destinations.some((d) => d.id === id && d.location.kind === "local" && d.node_id !== item.app.node_id))));

  function size(item: AppPlan): string {
    const rows = paths(item.app).filter((r) => r.status === "included");
    if (!rows.length) return "";
    return rows.every((r) => r.sizeKnown) ? bytes(rows.reduce((sum, r) => sum + (r.size ?? 0), 0)) : "unknown";
  }
  function counts(item: AppPlan): string {
    const rows = paths(item.app);
    const n = (status: string) => rows.filter((r) => r.status === status).length;
    const parts = [`${n("included")} path${n("included") === 1 ? "" : "s"}`];
    if (n("dumped")) parts.push(`${n("dumped")} dump${n("dumped") === 1 ? "" : "s"}`);
    const skipped = rows.filter((r) => r.optional).length;
    if (skipped) parts.push(`${skipped} excluded`);
    return parts.join(", ");
  }

  async function save() {
    busy = true;
    const all = later && every;
    submitted ??= { id: crypto.randomUUID(), expected_revision: revision, all, ...(all ? {} : { app_ids: picked.map((item) => item.app.id) }), enabled: true, settings: structuredClone($state.snapshot(settings)) };
    try {
      await api.protect(submitted);
      onSaved();
    } catch (error) {
      failure = detail(error);
      changed = failure.code === "protect_plan_changed";
      if (!uncertain(error)) submitted = null;
    } finally {
      busy = false;
    }
  }
</script>

<Dialog label="Schedule backups" title="Schedule backups" width={760} onClose={() => { if (!busy) onClose(); }}>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if changed}
    <p class="callout attn">The schedule changed in another session. Reload it and choose again.</p>
  {:else if !review}
    <form id="schedule-form" class="stack" onsubmit={(event) => { event.preventDefault(); review = true; }}>
      {#if apps.length === 0}
        <div class="empty"><strong>No apps found</strong><span>Add and approve a device. Its apps appear within a minute.</span></div>
      {:else}
        <fieldset disabled={busy || submitted !== null}>
          <legend class="sr">Apps</legend>
          <div class="scroll list">
            <table>
              <thead><tr><th><span class="sr">Select</span></th><th>App</th><th>Device</th><th>Paths</th><th class="num">Size</th></tr></thead>
              <tbody>
                {#each apps as item (item.app.id)}
                  {@const ready = approved(item)}
                  <tr class:off={item.enabled || !ready}>
                    <td class="box">
                      {#if item.enabled}
                        <input type="checkbox" checked disabled aria-label={`${item.app.definition.name} (scheduled)`} />
                      {:else}
                        <input type="checkbox" bind:group={chosen} value={item.app.id} disabled={!ready} aria-label={item.app.definition.name} />
                      {/if}
                    </td>
                    <td>{item.app.definition.name}</td>
                    <td>{item.app.node_name}</td>
                    <td class="dim">{item.enabled ? "scheduled" : ready ? counts(item) : "device not approved"}</td>
                    <td class="num mono">{size(item)}</td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        </fieldset>
      {/if}
      <div class="settings">
        {#if editing}
          <fieldset class="stack" disabled={busy || submitted !== null}>
            <ScheduleFields bind:settings {destinations} />
          </fieldset>
        {:else}
          <dl class="facts">
            <dt>Storage</dt><dd>{names || "none"}</dd>
            <dt>Daily at</dt><dd class="mono">{settings.at} {settings.time_zone}</dd>
            <dt>Keep</dt><dd class="mono">{settings.retention.daily}d {settings.retention.weekly}w {settings.retention.monthly}m</dd>
            <dt>Method</dt><dd class="mono">{settings.method}</dd>
          </dl>
          <p><button type="button" class="link" onclick={() => (editing = true)}>Change</button></p>
        {/if}
        {#if plan.auto_protect}
          <p class="hint">New apps are scheduled automatically.</p>
        {:else}
          <label class="check"><input type="checkbox" bind:checked={later} disabled={!every || busy || submitted !== null} />Also schedule apps found later</label>
          {#if !every}<p class="hint">Available when every app is selected.</p>{:else if later}<p class="hint">Applies these settings to every app, including scheduled ones.</p>{/if}
        {/if}
      </div>
    </form>
  {:else}
    <p class="note">Paths per app. Host system paths are never copied. Excluded paths can be added per app with Edit schedule.</p>
    <div class="review">
      {#each picked as item (item.app.id)}
        <section class="stack">
          <h3>{item.app.definition.name} <span class="dim">on {item.app.node_name}</span></h3>
          {#if settings.destinations.length > 0 && settings.destinations.every((id) => destinations.some((d) => d.id === id && d.node_id === item.app.node_id))}<p class="callout attn">Every snapshot would be on {item.app.node_name}, the host of the app.</p>{/if}
          <AppPaths app={item.app} />
        </section>
      {/each}
    </div>
    <p class="hint">Offline devices get the schedule when they reconnect. Revision <span class="mono">{revision}</span>.</p>
  {/if}
  {#if incompatible && !changed}<p class="callout attn">A selected local disk belongs to another device and cannot take these apps.</p>{/if}
  {#if submitted}<p class="callout attn">No reply. Retry the same request <span class="code">{submitted.id}</span></p>{/if}
  {#snippet footer()}
    {#if changed}
      <button type="button" class="primary" onclick={onSaved}>Reload schedule</button>
    {:else if review}
      {#if !submitted}<button type="button" class="ghost" disabled={busy} onclick={() => (review = false)}>Back</button>{/if}
      <button type="button" class="primary" disabled={busy || incompatible} onclick={save}>{busy ? "Saving" : submitted ? "Retry same request" : "Save schedule"}</button>
    {:else}
      <button type="button" class="ghost" onclick={onClose}>Cancel</button>
      <button type="submit" form="schedule-form" class="primary" disabled={picked.length === 0 || settings.destinations.length === 0 || incompatible}>Review {picked.length === 1 ? "1 app" : `${picked.length} apps`}</button>
    {/if}
  {/snippet}
</Dialog>

<style>
  .list {
    max-height: 340px;
    overflow: auto;
  }
  .box {
    width: 36px;
  }
  tr.off td {
    color: var(--ink-dim);
  }
  .settings {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .review {
    display: flex;
    flex-direction: column;
    gap: 20px;
    max-height: 460px;
    overflow: auto;
  }
  h3 .dim {
    font-weight: 400;
  }
</style>
