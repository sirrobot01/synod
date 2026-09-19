<script lang="ts">
  import { gaps, operations } from "../lib/mock";
  import { accepted, operationMark } from "../lib/state";
  import { follow, path } from "../lib/router.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Mocked from "../lib/ui/Mocked.svelte";
  import Panel from "../lib/ui/Panel.svelte";

  let { onOpen }: { onOpen: (id: string) => void } = $props();

  let lookup = $state("");

  function open(event: SubmitEvent) {
    event.preventDefault();
    const id = lookup.trim();
    if (id !== "") onOpen(id);
  }
</script>

<div class="screen">
  <header>
    <h1>Operations</h1>
    <p class="note">
      Durable, resumable, inspectable. Cancel and retry appear only when the API allows them.
    </p>
  </header>

  <Panel title="Open an operation" note="this reads the API">
    <form class="lookup" onsubmit={open}>
      <label for="operation-id" class="label">Operation ID</label>
      <input id="operation-id" bind:value={lookup} placeholder="op_7c14f2a9" />
      <button type="submit">Open</button>
    </form>
  </Panel>

  <Mocked gap={gaps.collections} />

  <Panel title={`Recent · ${operations.length}`}>
    {#each operations as operation (operation.id)}
      {@const mark = operationMark(operation.status, operation.planVersion)}
      <a
        class="row"
        href={path("operation", operation.id)}
        onclick={(event) => follow(event, () => onOpen(operation.id))}
        style="color: var(--{mark.tone})"
      >
        <Mark {mark} text={false} decorative />
        <span class="grow">
          <span class="mono status">
            {mark.glyph} {mark.label}
            <!-- Acceptance is its own state, so it is named rather than folded
                 into pending. It makes no claim about any database. -->
            {#if accepted(operation.status, operation.planVersion)}· no executable plan yet{/if}
          </span>
          <span class="mono what">{operation.what}</span>
        </span>
        <span class="ends mono">
          <span class="id">{operation.id}</span>
          <span class="age">{operation.age}</span>
        </span>
      </a>
    {/each}
    {#snippet foot()}
      Nothing here animates. A spinner would promise progress that an operation with an unresolved
      effect cannot promise.
    {/snippet}
  </Panel>
</div>

<style>
  .screen {
    display: flex;
    flex-direction: column;
    gap: 22px;
    max-width: 1240px;
  }
  header {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  header .note {
    font-size: 13px;
  }
  .lookup {
    display: flex;
    gap: 10px;
    align-items: center;
    flex-wrap: wrap;
    padding: 16px;
  }
  .lookup input {
    min-width: 280px;
  }
  .row {
    text-decoration: none;
  }
  .grow {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 3px;
  }
  .status {
    font-size: 12px;
  }
  .what {
    font-size: 11px;
    color: var(--ink-dim);
  }
  .row:hover .status {
    text-decoration: underline;
  }
  .ends {
    display: flex;
    flex-direction: column;
    gap: 3px;
    align-items: flex-end;
    font-size: 10px;
    color: var(--ink-faint);
    white-space: nowrap;
  }
</style>
