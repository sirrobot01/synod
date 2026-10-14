<script lang="ts">
  import type { CopySettings, Destination } from "./api";

  // The fields of a daily backup schedule, shared by Schedule backups and Edit schedule.
  let { settings = $bindable(), destinations }: { settings: CopySettings; destinations: Destination[] } = $props();
</script>

<fieldset class="stack">
  <legend>Storage</legend>
  <div class="targets">
    {#each destinations as destination (destination.id)}
      <label class="check"><input type="checkbox" bind:group={settings.destinations} value={destination.id} />{destination.name}</label>
    {:else}
      <p class="note">No storage. Add storage first.</p>
    {/each}
  </div>
</fieldset>
<div class="grid">
  <label class="field">Daily at<input class="mono" type="time" required bind:value={settings.at} /></label>
  <label class="field">Time zone<input class="mono" required bind:value={settings.time_zone} placeholder="UTC" /></label>
  <label class="field">Method
    <select bind:value={settings.method}>
      <option value="auto">auto</option>
      <option value="live">live</option>
      <option value="dump">dump</option>
      <option value="stop">stop</option>
    </select>
  </label>
  <label class="field">Keep daily<input class="mono" type="number" min="0" max="366" required bind:value={settings.retention.daily} /></label>
  <label class="field">Keep weekly<input class="mono" type="number" min="0" max="104" required bind:value={settings.retention.weekly} /></label>
  <label class="field">Keep monthly<input class="mono" type="number" min="0" max="120" required bind:value={settings.retention.monthly} /></label>
</div>
<p class="hint">
  {#if settings.method === "auto"}auto: dump databases, copy files while the app runs.
  {:else if settings.method === "live"}live: copy files while the app runs, no dumps.
  {:else if settings.method === "dump"}dump: native database dumps only.
  {:else}stop: stop the app for the final pass. The app is down during that pass.{/if}
</p>

<style>
  .targets {
    display: flex;
    flex-wrap: wrap;
    gap: 8px 20px;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 12px;
  }
  @media (max-width: 600px) {
    .grid {
      grid-template-columns: 1fr 1fr;
    }
  }
</style>
