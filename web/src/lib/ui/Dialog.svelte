<script lang="ts">
  import type { Snippet } from "svelte";
  import Mark from "./Mark.svelte";
  import type { Mark as MarkValue } from "../state";

  // Esc closes any dialog, which the keyboard contract in the design requires.
  // Focus enters the dialog on open and returns to the control that opened it,
  // so a keyboard reader is never left on a page behind an overlay.
  let {
    label,
    title,
    note = "",
    mark = undefined,
    placement = "side",
    danger = false,
    onClose,
    children,
  }: {
    label: string;
    title: string;
    note?: string;
    mark?: MarkValue;
    placement?: "side" | "centre";
    danger?: boolean;
    onClose: () => void;
    children: Snippet;
  } = $props();

  let panel = $state<HTMLElement | null>(null);

  $effect(() => {
    const opener = document.activeElement as HTMLElement | null;
    panel?.focus();
    return () => opener?.focus();
  });

  function key(event: KeyboardEvent) {
    if (event.key === "Escape") onClose();
  }
</script>

<svelte:window onkeydown={key} />

<div class="scrim {placement}" role="presentation" onclick={(event) => event.target === event.currentTarget && onClose()}>
  <div
    class="sheet {placement}"
    class:danger
    role="dialog"
    aria-modal="true"
    aria-label={label}
    tabindex="-1"
    bind:this={panel}
  >
    <header>
      {#if mark}<Mark {mark} text={false} />{/if}
      <span class="title mono" class:danger>{title}</span>
      {#if note}<span class="note mono">{note}</span>{/if}
      <button type="button" onclick={onClose} aria-label="Close">Esc</button>
    </header>
    <div class="body">
      {@render children()}
    </div>
  </div>
</div>

<style>
  .scrim {
    position: fixed;
    inset: 0;
    background: rgba(6, 8, 10, 0.74);
    display: flex;
    z-index: 40;
  }
  .scrim.side {
    justify-content: flex-end;
  }
  .scrim.centre {
    align-items: flex-start;
    justify-content: center;
    padding: 48px 20px;
    overflow-y: auto;
  }
  .sheet {
    background: var(--nav);
    display: flex;
    flex-direction: column;
    border: 1px solid var(--line);
  }
  .sheet:focus {
    outline: none;
  }
  .sheet.side {
    width: min(620px, 100%);
    height: 100%;
    overflow-y: auto;
    border: none;
    border-left: 1px solid var(--line);
  }
  .sheet.centre {
    width: min(680px, 100%);
  }
  .sheet.danger {
    border-color: var(--danger-line);
  }
  header {
    padding: 16px 20px;
    border-bottom: 1px solid var(--line);
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }
  .title {
    font-size: 13px;
    font-weight: 600;
  }
  .title.danger {
    font-size: 12px;
    letter-spacing: 0.06em;
    color: var(--fail);
  }
  header .note {
    font-size: 11px;
    color: var(--ink-faint);
  }
  header button {
    margin-left: auto;
  }
  .body {
    padding: 22px;
    display: flex;
    flex-direction: column;
    gap: 20px;
  }
</style>
