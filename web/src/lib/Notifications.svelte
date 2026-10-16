<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, uncertain, type NotificationStatus, type NotificationUpdate } from "./api";
  import { errorView, type APIError } from "./state";
  import ErrorPanel from "./ErrorPanel.svelte";
  let status = $state<NotificationStatus | null>(null);
  let failure = $state<APIError | null>(null); let busy = $state(false);
  let url = $state(""); let overdue = $state(24); let missing = $state(168); let full = $state(90);
  let submitted = $state<NotificationUpdate | null>(null);
  let testRequest = $state<{ id: string; expected_revision: number } | null>(null);
  let message = $state(""); let changed = $state(false);
  async function read() {
    busy = true;
    try { status = await api.notifications(); overdue = status.settings.overdue_grace_hours; missing = status.settings.missing_after_hours; full = status.settings.full_percent; changed = false; failure = null; }
    catch (error) { failure = detail(error); } finally { busy = false; }
  }
  onMount(read);
  async function save(disable = false) {
    if (!status || busy) return; busy = true; message = "";
    submitted ??= { id: crypto.randomUUID(), expected_revision: status.revision, settings: { url: disable ? "" : url, overdue_grace_hours: disable ? status.settings.overdue_grace_hours : overdue, missing_after_hours: disable ? status.settings.missing_after_hours : missing, full_percent: disable ? status.settings.full_percent : full } };
    try { await api.configureNotifications(submitted); message = submitted.settings.url === "" ? "Notifications off." : "Saved."; submitted = null; url = ""; await read(); }
    catch (error) { failure = detail(error); changed = failure.code === "notification_settings_changed"; if (!uncertain(error)) submitted = null; }
    finally { busy = false; }
  }
  async function test() {
    if (!status || busy) return; busy = true; message = "";
    testRequest ??= { id: crypto.randomUUID(), expected_revision: status.revision };
    try { const result = await api.testNotification(testRequest); message = `Test queued ${result.id}.`; testRequest = null; failure = null; }
    catch (error) { failure = detail(error); changed = failure.code === "notification_settings_changed"; if (!uncertain(error)) testRequest = null; }
    finally { busy = false; }
  }
</script>
<section class="section">
  <div class="section-head"><h2>Notifications</h2><span class="hint">Shoutrrr: ntfy, Gotify, Discord, Slack, email, webhook</span></div>
  <div class="stack">
    {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
    {#if message}<p role="status" class="callout">{message} <a href="/activity">View activity</a></p>{/if}
    {#if status}<p>{status.settings.configured ? `Sending through ${status.settings.service}.` : "Off."} A part-time device stays quiet until the missing-device limit passes.</p>
      {#if changed}<p class="callout attn">Notification settings changed. Read them again before you save.</p><button onclick={() => { submitted = null; testRequest = null; url = ""; read(); }} disabled={busy}>Read current settings</button>{/if}
      <form class="stack" autocomplete="off" onsubmit={(event) => { event.preventDefault(); save(); }}>
        <fieldset class="stack" disabled={busy || changed || submitted !== null || testRequest !== null}><legend class="sr">Notification settings</legend>
          <label class="field">Shoutrrr notification URL<input type="password" required maxlength="8192" bind:value={url} autocomplete="new-password" placeholder="ntfy://…" /></label>
          <p class="hint">Enter the URL on every save. The saved URL is never sent back.</p>
          <div class="grid"><label class="field">Overdue grace (hours)<input type="number" min="1" max="2160" required bind:value={overdue} /></label><label class="field">Missing device (hours)<input type="number" min="1" max="2160" required bind:value={missing} /></label><label class="field">Almost full (% used)<input type="number" min="1" max="100" required bind:value={full} /></label></div>
        </fieldset>
        {#if submitted}<p class="callout attn">The reply was not confirmed. Retry the same request before you change it.</p>{/if}
        <div class="actions">
          <button class="primary" disabled={busy || changed || testRequest !== null}>{submitted ? submitted.settings.url === "" ? "Retry disable request" : "Retry same settings" : "Save notification settings"}</button>
          {#if status.settings.configured}
            <button type="button" onclick={test} disabled={busy || changed || submitted !== null}>{testRequest ? "Retry same test" : "Send test notification"}</button>
            <button type="button" class="quiet" onclick={() => save(true)} disabled={busy || changed || testRequest !== null || submitted !== null}>Turn off</button>
          {/if}
        </div>
      </form>
    {:else if failure}<button onclick={read} disabled={busy}>Retry loading settings</button>
    {:else}<p class="loading">Reading notification settings…</p>{/if}
  </div>
</section>
<style>
  .grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
  @media (max-width: 600px) { .grid { grid-template-columns: 1fr; } }
</style>
