<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, uncertain, type NotificationStatus, type NotificationUpdate } from "./api";
  import { errorView, type APIError } from "./state";
  import Panel from "./ui/Panel.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";
  let status = $state<NotificationStatus | null>(null);
  let failure = $state<APIError | null>(null); let busy = $state(false); let editing = $state(false);
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
    if (!status) return; busy = true; message = "";
    submitted ??= { id: crypto.randomUUID(), expected_revision: status.revision, settings: { url: disable ? "" : url, overdue_grace_hours: overdue, missing_after_hours: missing, full_percent: full } };
    try { await api.configureNotifications(submitted); submitted = null; url = ""; editing = false; message = disable ? "Alerts are disabled." : "Notification settings saved."; await read(); }
    catch (error) { failure = detail(error); changed = failure.code === "notification_settings_changed"; if (!uncertain(error)) submitted = null; }
    finally { busy = false; }
  }
  async function test() {
    if (!status) return; busy = true; message = "";
    testRequest ??= { id: crypto.randomUUID(), expected_revision: status.revision };
    try { const result = await api.testNotification(testRequest); message = `Test queued: ${result.id}. Check Activity for delivery.`; testRequest = null; failure = null; }
    catch (error) { failure = detail(error); changed = failure.code === "notification_settings_changed"; if (!uncertain(error)) testRequest = null; }
    finally { busy = false; }
  }
</script>
<Panel title="Notifications" note="Shoutrrr · ntfy, Gotify, Discord, Slack, email, or webhook">
  <div class="pad stack">
    {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
    {#if message}<p role="status" class="callout">{message} <a href="/operations">View activity</a></p>{/if}
    {#if status}<p>{status.settings.configured ? `Alerts configured through ${status.settings.service}.` : "Alerts are off."} A part-time device is quiet until its missing-device limit passes.</p>
      <dl class="facts mono"><dt>Overdue grace</dt><dd>{status.settings.overdue_grace_hours} hours</dd><dt>Missing device</dt><dd>{status.settings.missing_after_hours} hours</dd><dt>Almost full</dt><dd>{status.settings.full_percent}% used</dd></dl>
      {#if changed}<p class="callout">Notification settings changed. Read them again before you save.</p><button onclick={() => { submitted = null; testRequest = null; url = ""; editing = false; read(); }} disabled={busy}>Read current settings</button>
      {:else if editing}
        <form class="stack" autocomplete="off" onsubmit={(event) => { event.preventDefault(); save(); }}>
          <fieldset class="stack" disabled={busy || submitted !== null || testRequest !== null}><legend class="label">Configure alerts</legend>
            <label>Shoutrrr notification URL<input type="password" required maxlength="8192" bind:value={url} autocomplete="new-password" placeholder="ntfy://…" /></label>
            <p class="note">Enter the URL again when you change these settings. Synod never sends the saved URL back to this page.</p>
            <div class="columns"><label>Overdue grace (hours)<input type="number" min="1" max="2160" required bind:value={overdue} /></label><label>Missing device (hours)<input type="number" min="1" max="2160" required bind:value={missing} /></label><label>Almost full (% used)<input type="number" min="1" max="100" required bind:value={full} /></label></div>
          </fieldset>
          {#if submitted}<p class="callout">The reply was not confirmed. Retry the same request before you change it.</p>{/if}
          <div class="actions"><button disabled={busy}>{submitted ? "Retry same settings" : "Save notification settings"}</button><button type="button" disabled={busy || submitted !== null} onclick={() => { editing = false; url = ""; }}>Cancel</button></div>
        </form>
      {:else}<div class="actions"><button onclick={() => editing = true} disabled={busy || submitted !== null || testRequest !== null}>Configure alerts</button>
        {#if status.settings.configured}<button onclick={test} disabled={busy || submitted !== null}>{testRequest ? "Retry same test" : "Send test notification"}</button><button class="quiet" onclick={() => save(true)} disabled={busy || testRequest !== null}>{submitted ? "Retry disable request" : "Disable alerts"}</button>{/if}
      </div>{/if}
    {:else if !failure}<p class="note">Reading notification settings…</p>{/if}
  </div>
</Panel>
