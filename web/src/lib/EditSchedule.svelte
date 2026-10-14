<script lang="ts">
  import { untrack } from "svelte";
  import { api, detail, uncertain, type AppPlan, type ProtectionPlan, type ProtectionUpdate, type CopySettings, type Destination } from "./api";
  import { errorView, type APIError } from "./state";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";
  import AppPaths from "./AppPaths.svelte";
  import ScheduleFields from "./ScheduleFields.svelte";

  // Edits the schedule of one app. The reader reviews the paths before the save.
  let { plan, item, destinations, onClose, onSaved }: { plan: ProtectionPlan; item: AppPlan; destinations: Destination[]; onClose: () => void; onSaved: () => void } = $props();
  // Capture the reviewed revision. A concurrent edit requires another review.
  let revision = $state(untrack(() => plan.revision));
  let settings = $state<CopySettings>(untrack(() => {
    const value = structuredClone($state.snapshot(item.settings));
    value.include_large ??= [];
    return value;
  }));
  // The dialog opens with the schedule on. Clear the box to stop the backups of the app.
  let enabled = $state(true);
  // Large paths and network shares belong to one app.
  let large = $derived([...new Set([...(item.app.definition.paths ?? []).filter((p) => (p.status === "Large" || p.status === "Remote") && p.source).map((p) => p.source!), ...(settings.include_large ?? [])])]);
  let incompatible = $derived(enabled && item.app.present && settings.destinations.some((id) => destinations.some((d) => d.id === id && d.location.kind === "local" && d.node_id !== item.app.node_id)));
  let review = $state(false);
  let busy = $state(false);
  let changed = $state(false);
  let failure = $state<APIError | null>(null);
  let submitted = $state<ProtectionUpdate | null>(null);

  async function save() {
    busy = true;
    submitted ??= { id: crypto.randomUUID(), expected_revision: revision, all: false, app_ids: [item.app.id], enabled, settings: structuredClone($state.snapshot(settings)) };
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

<Dialog label="Edit schedule" title={`Edit schedule: ${item.app.definition.name}`} width={760} onClose={() => { if (!busy) onClose(); }}>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if changed}
    <p class="callout attn">The schedule changed in another session. Reload it before you save.</p>
  {:else}
    <form id="edit-form" class="stack" onsubmit={(event) => { event.preventDefault(); if (review) save(); else review = true; }}>
      <fieldset class="stack" disabled={busy || submitted !== null || review}>
        <legend class="sr">Schedule</legend>
        <label class="check"><input type="checkbox" bind:checked={enabled} />Back up this app daily</label>
        {#if enabled}
          <ScheduleFields bind:settings {destinations} />
          {#if large.length}
            <fieldset class="stack">
              <legend>Excluded paths to include</legend>
              {#each large as path (path)}<label class="check"><input type="checkbox" bind:group={settings.include_large} value={path} /><span class="mono wrap">{path}</span></label>{/each}
            </fieldset>
          {/if}
        {/if}
      </fieldset>
      {#if incompatible}<p class="callout attn">A selected local disk belongs to another device and cannot take this app.</p>{/if}
      {#if review}
        {#if !enabled}<p class="callout attn">Daily backups of {item.app.definition.name} stop. Existing snapshots stay in storage.</p>{/if}
        {#if enabled && settings.destinations.length > 0 && settings.destinations.every((id) => destinations.some((d) => d.id === id && d.node_id === item.app.node_id))}<p class="callout attn">Every snapshot would be on {item.app.node_name}, the host of the app.</p>{/if}
        {#if !item.app.present}<p class="callout attn">Not in the last scan of {item.app.node_name}.</p>{/if}
        {#if enabled}<AppPaths app={item.app} included={settings.include_large ?? []} />{/if}
        <p class="hint">Offline devices get the schedule when they reconnect. Revision <span class="mono">{revision}</span>.</p>
      {/if}
      {#if submitted}<p class="callout attn">No reply. Retry the same request <span class="code">{submitted.id}</span></p>{/if}
    </form>
  {/if}
  {#snippet footer()}
    {#if changed}
      <button type="button" class="primary" onclick={onSaved}>Reload schedule</button>
    {:else}
      {#if review && !submitted}<button type="button" class="ghost" disabled={busy} onclick={() => (review = false)}>Back</button>{/if}
      <button type="submit" form="edit-form" class="primary" disabled={busy || incompatible || (enabled && settings.destinations.length === 0)}>{busy ? "Saving" : submitted ? "Retry same request" : review ? "Save schedule" : "Review"}</button>
    {/if}
  {/snippet}
</Dialog>
