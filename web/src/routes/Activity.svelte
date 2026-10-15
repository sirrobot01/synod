<script lang="ts">
  import { onMount } from "svelte";
  import { api, detail, type DestinationList, type Node, type NotificationStatus, type Operation, type ProtectionPlan } from "../lib/api";
  import { errorView, finished, operationMark, type APIError } from "../lib/state";
  import { bytes } from "../lib/format";
  import { activity, failureText, kinds, nodeName, took, when } from "../lib/model";
  import { go } from "../lib/router.svelte";
  import { toast } from "../lib/toast.svelte";
  import Link from "../lib/ui/Link.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Icon from "../lib/ui/Icon.svelte";
  import IconButton from "../lib/ui/IconButton.svelte";
  import Dialog from "../lib/ui/Dialog.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";

  const size = 50;

  let operations = $state<Operation[]>([]);
  // The cursor of each page: the ID that the page starts after. The first page has none.
  let cursors = $state<string[]>([]);
  let older = $state(false);
  let alerts = $state<NotificationStatus | null>(null);
  let plan = $state<ProtectionPlan | null>(null);
  let listing = $state<DestinationList | null>(null);
  let nodes = $state<Node[]>([]);
  let failure = $state<APIError | null>(null);
  let alertFailure = $state<APIError | null>(null);
  let loaded = $state(false);
  let busy = $state(false);
  let filter = $state("");
  let kind = $state("");
  let now = $state(Date.now());
  let removing = $state<Operation | "finished" | null>(null);
  let removeFailure = $state<APIError | null>(null);
  let removeBusy = $state(false);

  let names = $derived({
    app: (id: string) => plan?.apps.find((a) => a.app.id === id)?.app.definition.name ?? "app",
    node: (id: string) => nodeName(nodes, id),
    storage: (id?: string) => listing?.destinations.find((d) => d.id === id)?.name ?? "",
  });
  let rows = $derived(operations.map((op) => ({ op, row: activity(op, names) })));
  let shown = $derived(rows.filter(({ op, row }) => (!kind || op.kind === kind) && `${row.kind} ${row.subject} ${row.target} ${row.result} ${op.id}`.toLowerCase().includes(filter.toLowerCase())));
  let page = $derived(cursors.length + 1);

  async function readPage() {
    // One extra record says whether an older page exists.
    const before = cursors.at(-1);
    const answer = await api.operations({ limit: size + 1, ...(before ? { before } : {}) });
    older = answer.length > size;
    operations = answer.slice(0, size);
  }

  async function read() {
    if (busy) return;
    busy = true;
    const results = await Promise.allSettled([readPage(), api.notifications(), api.protection(), api.nodes(), api.destinations()]);
    if (results[0].status === "fulfilled") { loaded = true; failure = null; } else failure = detail(results[0].reason);
    if (results[1].status === "fulfilled") { alerts = results[1].value; alertFailure = null; } else alertFailure = detail(results[1].reason);
    if (results[2].status === "fulfilled") plan = results[2].value;
    if (results[3].status === "fulfilled") nodes = results[3].value;
    if (results[4].status === "fulfilled") listing = results[4].value;
    now = Date.now();
    busy = false;
  }

  async function turn(direction: "older" | "newer") {
    if (direction === "older") {
      const last = operations.at(-1);
      if (!last) return;
      cursors = [...cursors, last.id];
    } else {
      cursors = cursors.slice(0, -1);
    }
    busy = true;
    try {
      await readPage();
      failure = null;
    } catch (error) {
      failure = detail(error);
    } finally {
      busy = false;
    }
  }

  async function remove() {
    if (!removing) return;
    removeBusy = true;
    try {
      if (removing === "finished") {
        const answer = await api.clearOperations();
        toast.show(answer.removed === 1 ? "Removed 1 finished run from Activity" : `Removed ${answer.removed} finished runs from Activity`);
        cursors = [];
      } else {
        await api.removeOperation(removing.id);
        toast.show("Removed from Activity");
      }
      removing = null;
      removeFailure = null;
      await read();
    } catch (error) {
      removeFailure = detail(error);
    } finally {
      removeBusy = false;
    }
  }

  // The whole row opens the run. A click on a link or a button inside the row keeps its own meaning.
  function open(event: MouseEvent, op: Operation) {
    if ((event.target as HTMLElement).closest("a, button") || event.defaultPrevented) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey) {
      window.open(`/activity/${encodeURIComponent(op.id)}`, "_blank", "noopener");
      return;
    }
    go("operation", op.id);
  }

  onMount(() => {
    read();
    // Only the newest page refreshes by itself, so a page never shifts under the reader.
    const timer = setInterval(() => { if (cursors.length === 0 && !removing) read(); }, 10000);
    return () => clearInterval(timer);
  });
</script>

<div class="page">
  <header class="page-head">
    <div>
      <h1>Activity</h1>
      <p class="sub">Backups, restores, restore tests, checks, and cleanups</p>
    </div>
    <div class="actions">
      <button type="button" onclick={() => { removing = "finished"; removeFailure = null; }} disabled={!loaded || operations.every((op) => !finished(op.status))}><Icon name="clear" />Clear finished</button>
      <IconButton icon="refresh" label="Refresh" onclick={read} disabled={busy} />
    </div>
  </header>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  <section class="section" aria-label="Runs">
    <div class="tools">
      <label class="search"><span class="sr">Filter this page</span><Icon name="search" /><input type="search" bind:value={filter} placeholder="Filter by app, target, or result" /></label>
      <label class="select"><span class="sr">Kind</span><Icon name="filter" />
        <select bind:value={kind}>
          <option value="">All kinds</option>
          {#each Object.entries(kinds) as [value, name] (value)}<option {value}>{name}</option>{/each}
        </select>
      </label>
      <nav class="pager" aria-label="Pages">
        <span class="note">Page <span class="mono">{page}</span></span>
        <IconButton icon="left" label="Newer runs" onclick={() => turn("newer")} disabled={busy || cursors.length === 0} />
        <IconButton icon="right" label="Older runs" onclick={() => turn("older")} disabled={busy || !older} />
      </nav>
    </div>
    {#if !loaded && !failure}<p class="loading" role="status">Loading</p>{/if}
    {#if loaded}
      <div class="scroll">
        <table class="runs">
          <colgroup><col class="c-time" /><col class="c-kind" /><col /><col /><col class="c-result" /><col class="c-num" /><col class="c-num" /><col class="c-by" /><col class="c-act" /></colgroup>
          <thead><tr><th>Started</th><th>Kind</th><th>App</th><th>Target</th><th>Result</th><th class="num">Took</th><th class="num">Size</th><th>By</th><th class="end"><span class="sr">Actions</span></th></tr></thead>
          <tbody>
            {#each shown as { op, row } (op.id)}
              <!-- The first cell holds the real link for the keyboard. The row click is a larger target for the pointer. -->
              <!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_noninteractive_element_interactions -->
              <tr class="link" onclick={(event) => open(event, op)}>
                <td class="mono"><Link screen="operation" id={op.id} label={`${row.kind}: ${row.subject}, ${when(op.created_at, now)}`}>{when(op.created_at, now)}</Link></td>
                <td>{row.kind}</td>
                <td class="truncate" title={row.subject}>{row.subject}</td>
                <td class="truncate" title={row.target}>{row.target}</td>
                <td><span class="result"><Mark mark={operationMark(op.status)} shapeOnly /><span class={row.tone}>{row.result}</span></span>{#if op.status === "failed"}<small title={failureText(op.failure_code)} class="code">{op.failure_code}</small>{/if}</td>
                <td class="num mono">{row.took !== undefined ? took(row.took) : ""}</td>
                <td class="num mono">{op.result?.file_bytes !== undefined ? bytes(op.result.file_bytes) : ""}</td>
                <td class="dim truncate">{op.actor_id.startsWith("schedule:") ? "schedule" : op.actor_id}</td>
                <td class="end">
                  {#if finished(op.status)}
                    <IconButton icon="trash" tone="danger" label={`Remove ${row.kind} of ${row.subject} from Activity`} onclick={() => { removing = op; removeFailure = null; }} />
                  {/if}
                </td>
              </tr>
            {:else}
              <tr><td colspan="9" class="dim">{operations.length ? "No runs on this page match." : cursors.length ? "No older runs." : "No runs yet. Backups, restores, and checks appear here."}</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>
  <section class="section" aria-labelledby="alerts">
    <div class="section-head"><h2 id="alerts">Notifications sent</h2><Link screen="settings" id="notifications">Notification settings</Link></div>
    {#if alertFailure}<ErrorPanel view={errorView(alertFailure)} />{/if}
    <div class="scroll">
      <table>
        <colgroup><col class="c-time" /><col /><col class="c-result" /><col class="c-num" /></colgroup>
        <thead><tr><th>Created</th><th>Message</th><th>Delivery</th><th class="num">Attempts</th></tr></thead>
        <tbody>
          {#each alerts?.alerts ?? [] as alert (alert.id)}
            <tr>
              <td class="mono">{when(alert.created_at, now)}</td>
              <td>{alert.message}{#if alert.operation_id}<span class="gap"><Link screen="operation" id={alert.operation_id}>Run</Link></span>{/if}</td>
              <td><span class:ok={alert.state === "sent"} class:fail={alert.state === "failed"}>{alert.state}</span>{#if alert.failure_code}<small class="code">{alert.failure_code}</small>{/if}</td>
              <td class="num mono">{alert.attempts}</td>
            </tr>
          {:else}
            <tr><td colspan="4" class="dim">{alerts ? "None sent." : "Loading"}</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>
</div>

{#if removing}
  <Dialog label="Remove from Activity" title={removing === "finished" ? "Clear finished runs" : "Remove from Activity"} width={480} onClose={() => { if (!removeBusy) removing = null; }}>
    <p>{removing === "finished" ? "Hides every finished run from Activity. Running and queued work stays." : "Hides this run from Activity."} App status and restore-test dates keep their history.</p>
    {#if removeFailure}<ErrorPanel view={errorView(removeFailure)} />{/if}
    {#snippet footer()}
      <button type="button" class="ghost" onclick={() => (removing = null)} disabled={removeBusy}>Cancel</button>
      <button type="button" class="destructive" onclick={remove} disabled={removeBusy}><Icon name="trash" />{removeBusy ? "Removing" : removing === "finished" ? "Clear finished" : "Remove"}</button>
    {/snippet}
  </Dialog>
{/if}

<style>
  .tools {
    display: grid;
    grid-template-columns: minmax(0, 360px) minmax(0, 200px) 1fr;
    gap: 8px;
    align-items: center;
  }
  .pager {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 4px;
  }
  .gap {
    margin-left: 8px;
  }
  .runs small.code {
    white-space: nowrap;
  }
  .pager .note {
    margin-right: 8px;
  }
  .result {
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }
  .ok { color: var(--ok); }
  .fail { color: var(--fail); }
  .active { color: var(--active); }
  .unc { color: var(--unc); }
  .neutral { color: var(--ink-muted); }
  .runs :global(.c-act) {
    width: 48px;
  }
  @media (max-width: 700px) {
    .tools {
      grid-template-columns: 1fr 1fr;
    }
    .search {
      grid-column: 1 / -1;
    }
  }
</style>
