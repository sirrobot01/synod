<script lang="ts">
  import { api, detail, type Operation } from "../lib/api";
  import { errorView, operationMark, type APIError } from "../lib/state";
  import { follow, path } from "../lib/router.svelte";
  import ErrorPanel from "../lib/ErrorPanel.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  let { onOpen }: { onOpen: (id: string) => void } = $props();
  let operations = $state<Operation[]>([]);
  let failure = $state<APIError | null>(null);
  let loaded = $state(false);
  let busy = $state(false);
  async function read() {
    busy = true;
    try { operations = await api.operations(); loaded = true; failure = null; }
    catch (error) { failure = detail(error); }
    finally { busy = false; }
  }
  $effect(() => { read(); });
</script>
<header><h1>Operations</h1><p class="note">The 100 most recent operations.</p><button onclick={read} disabled={busy}>Refresh</button></header>
{#if failure}<ErrorPanel view={errorView(failure)} />{/if}
{#each operations as operation (operation.id)}
  <a class="panel row" href={path("operation", operation.id)} onclick={(event) => follow(event, () => onOpen(operation.id))}>
    <Mark mark={operationMark(operation.status)} />
    <span>{operation.kind}</span><span class="mono">{operation.id}</span><span>{operation.created_at}</span>
  </a>
{:else}
  {#if loaded}<p class="note">No operations have been recorded yet.</p>{/if}
{/each}
<style>
 header { display:flex; align-items:start; flex-direction:column; gap:10px; margin-bottom:22px; }
 .row { display:flex; flex-wrap:wrap; gap:16px; padding:16px; margin-bottom:12px; }
</style>
