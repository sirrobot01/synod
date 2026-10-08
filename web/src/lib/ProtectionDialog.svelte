<script lang="ts">
  import { untrack } from "svelte";
  import { api, detail, uncertain, type AppPlan, type ProtectionPlan, type ProtectionUpdate, type CopySettings, type Destination } from "./api";
  import { errorView, type APIError } from "./state";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";
  import AppPaths from "./AppPaths.svelte";
  let { plan, apps, destinations, all, onClose, onSaved }: { plan: ProtectionPlan; apps: AppPlan[]; destinations: Destination[]; all: boolean; onClose: () => void; onSaved: () => void } = $props();
  // Capture the reviewed revision. A concurrent edit requires another review.
  let revision = $state(untrack(() => plan.revision));
  let settings = $state<CopySettings>(untrack(() => structuredClone($state.snapshot(all ? plan.defaults : apps[0]?.settings ?? plan.defaults))));
  let enabled = $state(untrack(() => all || (apps[0]?.enabled ?? true)));
  // Large paths belong to one app. The default for all apps never includes them.
  untrack(() => { if (all) delete settings.include_large; else settings.include_large ??= []; });
  let large = $derived(all ? [] : [...new Set([...(apps[0]?.app.definition.paths ?? []).filter(item => item.status === "Large" && item.source).map(item => item.source!), ...(settings.include_large ?? [])])]);
  let incompatible = $derived(enabled && apps.some(item => item.app.present && settings.destinations.some(id => destinations.some(d => d.id === id && d.location.kind === "local" && d.node_id !== item.app.node_id))));
  let review = $state(false);
  let busy = $state(false);
  let changed = $state(false);
  let failure = $state<APIError | null>(null);
  let submitted = $state<ProtectionUpdate | null>(null);
  async function save() {
    busy = true;
    submitted ??= { id: crypto.randomUUID(), expected_revision: revision, all, ...(all ? {} : { app_ids: apps.map(item => item.app.id) }), enabled, settings: structuredClone($state.snapshot(settings)) };
    try { await api.protect(submitted); onSaved(); }
    catch (error) {
      failure = detail(error);
      changed = failure.code === "protect_plan_changed";
      if (!uncertain(error)) submitted = null;
    } finally { busy = false; }
  }
</script>
<Dialog label="Protection plan" title={all ? "Protect all apps" : `Protection · ${apps[0]?.app.definition.name}`} onClose={() => { if (!busy) onClose(); }}>
  <form class="stack" onsubmit={(event) => { event.preventDefault(); if (review) save(); else review = true; }}>
    {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
    {#if changed}
      <p class="callout">The plan changed while you reviewed it. Read the current plan before you confirm again.</p>
      <button type="button" class="primary" onclick={onSaved}>Read current plan</button>
    {:else}
      <fieldset class="stack" disabled={busy || submitted !== null || review}>
        <legend class="label">{review ? "Selected settings" : "Daily backup settings"}</legend>
        <label class="check"><input type="checkbox" bind:checked={enabled} />{all ? "Protect these apps and new apps automatically" : "Protect this app"}</label>
        {#if all}<p class="note">This replaces the settings of present apps on approved devices, including apps that were turned off.</p>{/if}
        <fieldset class="stack"><legend class="label">Destinations</legend>
          {#each destinations as destination (destination.id)}
            <label class="check"><input type="checkbox" bind:group={settings.destinations} value={destination.id} />{destination.name} <span class="mono dim">{destination.location.kind}</span></label>
          {:else}<p>Add a destination first.</p>{/each}
        </fieldset>
        <label>Copy method<select bind:value={settings.method}><option value="auto">Automatic · app keeps running</option><option value="live">Live · copy files while running</option><option value="dump">Dump · native database copies</option><option value="stop">Stop · final copy while stopped</option></select></label>
        {#if settings.method === "stop"}<p class="callout">Stop stops the app briefly for each backup. Use it only if the app needs a stopped copy.</p>{/if}
        <div class="columns"><label>Daily time<input type="time" required bind:value={settings.at} /></label><label>Time zone<input required bind:value={settings.time_zone} placeholder="UTC" /></label></div>
        {#if large.length && enabled}
          <fieldset class="stack"><legend class="label">Large paths</legend>
            <p class="note">Synod skips these by default. Include a path only if the destination has room for it.</p>
            {#each large as path (path)}<label class="check"><input type="checkbox" bind:group={settings.include_large} value={path} /><span class="mono wrap">{path}</span></label>{/each}
          </fieldset>
        {/if}
        <div class="columns"><label>Daily snapshots<input type="number" min="0" max="366" required bind:value={settings.retention.daily} /></label><label>Weekly snapshots<input type="number" min="0" max="104" required bind:value={settings.retention.weekly} /></label><label>Monthly snapshots<input type="number" min="0" max="120" required bind:value={settings.retention.monthly} /></label></div>
      </fieldset>
      {#if incompatible}<p class="callout">A selected local destination belongs to another device. Choose a shared destination or change the selection.</p>{/if}
      {#if review}
        <p>{enabled ? "Check what each app will copy. Host system paths are always excluded. Large paths are excluded unless you include them." : "Future scheduled backups will stop. Saved snapshots remain in the repository."}</p>
        {#each apps as item (item.app.id)}
          <section class="panel"><div class="panel-head"><strong>{item.app.definition.name}</strong><span class="note">{item.app.node_name}</span></div>
            {#if enabled && settings.destinations.length > 0 && settings.destinations.every(id => destinations.some(d => d.id === id && d.node_id === item.app.node_id))}<p class="callout">The only copies will be on the source device. A lost device can also lose these backups.</p>{/if}
            {#if !item.app.present}<p class="callout">This app was not present in the last scan.</p>{/if}
            <AppPaths app={item.app} included={settings.include_large ?? []} />
          </section>
        {/each}
        <p class="note">Plan revision <span class="mono">{revision}</span>. A device that is offline receives the settings when it returns.</p>
      {/if}
      {#if submitted}<p class="callout">The reply was not confirmed. Retry the same request: <span class="mono wrap">{submitted.id}</span>.</p>{/if}
      <div class="actions">
        {#if review && !submitted}<button type="button" disabled={busy} onclick={() => review = false}>Edit settings</button>{/if}
        <button class="primary" disabled={busy || incompatible || (enabled && settings.destinations.length === 0)}>{busy ? "Saving…" : submitted ? "Retry same request" : review ? "Confirm plan" : "Review paths"}</button>
      </div>
    {/if}
  </form>
</Dialog>
