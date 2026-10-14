<script lang="ts">
  import type { Snippet } from "svelte";
  import type { Mark as MarkValue } from "./state";
  import { path } from "./router.svelte";
  import Mark from "./ui/Mark.svelte";

  // An error is anchored to the form that caused it, never raised as a toast. The
  // sentence comes first. The stable code and the request identity stay visible in
  // small type, because that is what a support request needs.
  interface Props {
    view: { code: string; consequence: string; requestID: string; operationID?: string; mark: MarkValue };
    actions?: Snippet;
    footnote?: string;
  }
  let { view, actions = undefined, footnote = "" }: Props = $props();
</script>

<div class="failure {view.mark.tone}" role="alert">
  <div class="head">
    <Mark mark={view.mark} text={false} />
    <p class="consequence">{view.consequence}</p>
  </div>
  <p class="support">
    <span class="code">{view.code}</span>
    {#if view.requestID}<span class="code">request {view.requestID}</span>{/if}
    {#if view.operationID}<a class="code" href={path("operation", view.operationID)}>operation {view.operationID}</a>{/if}
  </p>
  {#if actions}
    <div class="actions">{@render actions()}</div>
  {/if}
  {#if footnote}
    <p class="footnote">{footnote}</p>
  {/if}
</div>

<style>
  .failure {
    border: 1px solid var(--error-color, var(--fail));
    background: var(--panel);
    padding: 12px 14px;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .failure.ok { --error-color: var(--ok); }
  .failure.attn { --error-color: var(--attn); }
  .failure.fail { --error-color: var(--fail); }
  .failure.active { --error-color: var(--active); }
  .failure.unc { --error-color: var(--unc); }
  .failure.neutral { --error-color: var(--neutral); }
  .failure.dim { --error-color: var(--dim); }
  .head {
    display: flex;
    align-items: flex-start;
    gap: 10px;
  }
  .consequence {
    font-size: 13px;
    line-height: 1.5;
    color: var(--ink);
  }
  .support {
    display: flex;
    flex-wrap: wrap;
    gap: 4px 14px;
    padding-left: 34px;
  }
  .footnote {
    font-size: 12px;
    color: var(--ink-dim);
  }
</style>
