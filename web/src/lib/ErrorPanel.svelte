<script lang="ts">
  import type { Snippet } from "svelte";
  import type { Mark as MarkValue } from "./state";
  import Mark from "./ui/Mark.svelte";

  // An error is anchored to the form that caused it, never raised as a toast. The
  // stable code is shown verbatim, because that is the string an operator greps
  // for, and the request identity is never hidden.
  interface Props {
    view: { code: string; consequence: string; requestID: string; operationID?: string; mark: MarkValue };
    actions?: Snippet;
    footnote?: string;
  }
  let { view, actions = undefined, footnote = "" }: Props = $props();
</script>

<div class="failure" style="border-color: var(--{view.mark.tone})" role="alert">
  <p class="head">
    <Mark mark={view.mark} text={false} />
    <span class="code mono" style="color: var(--{view.mark.tone})">{view.code}</span>
  </p>
  <p class="consequence">{view.consequence}</p>
  {#if view.requestID || view.operationID}
    <div class="facts mono">
      {#if view.requestID}
        <span class="key">request_id</span><span>{view.requestID}</span>
      {/if}
      {#if view.operationID}
        <span class="key">operation_id</span><span>{view.operationID}</span>
      {/if}
    </div>
  {/if}
  {#if actions}
    <div class="actions">{@render actions()}</div>
  {/if}
  {#if footnote}
    <p class="footnote">{footnote}</p>
  {/if}
</div>

<style>
  .failure {
    border: 1px solid var(--fail);
    background: var(--panel);
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .failure p {
    margin: 0;
  }
  .head {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .code {
    font-size: 12px;
    font-weight: 500;
  }
  .consequence {
    font-size: 13px;
    line-height: 1.6;
    color: var(--ink);
  }
  .facts {
    display: grid;
    grid-template-columns: auto 1fr;
    row-gap: 4px;
    column-gap: 12px;
    font-size: 11px;
    color: var(--ink-body);
  }
  .key {
    color: var(--ink-faint);
  }
  .actions {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
  }
  .footnote {
    font-size: 11px;
    color: var(--ink-dim);
    line-height: 1.5;
  }
</style>
