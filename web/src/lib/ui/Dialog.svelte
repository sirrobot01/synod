<script lang="ts">
  import type { Snippet } from "svelte";

  // Esc closes any dialog, which the keyboard contract in the design requires.
  // Focus enters the dialog on open and returns to the control that opened it,
  // so a keyboard reader is never left on a page behind an overlay.
  let {
    label,
    title,
    note = "",
    width = 640,
    danger = false,
    head = undefined,
    footer = undefined,
    onClose,
    children,
  }: {
    label: string;
    title: string;
    note?: string;
    width?: number;
    danger?: boolean;
    head?: Snippet;
    footer?: Snippet;
    onClose: () => void;
    children: Snippet;
  } = $props();

  const id = `dialog-${Math.random().toString(36).slice(2, 10)}`;
  let panel = $state<HTMLElement | null>(null);

  $effect(() => {
    const opener = document.activeElement as HTMLElement | null;
    panel?.focus();
    document.body.classList.add("modal-open");
    return () => {
      document.body.classList.remove("modal-open");
      opener?.focus();
    };
  });

  function key(event: KeyboardEvent) {
    if (event.key === "Escape") {
      event.preventDefault();
      onClose();
      return;
    }
    if (event.key !== "Tab" || !panel) return;
    const controls = Array.from(panel.querySelectorAll<HTMLElement>(
      'a[href], button, input, select, textarea, summary, [tabindex]',
    )).filter((control) => control.tabIndex >= 0 && !control.matches(":disabled, [hidden]"));
    const first = controls[0];
    const last = controls.at(-1);
    if (!first || !last) {
      event.preventDefault();
      panel.focus();
    } else if (event.shiftKey && (document.activeElement === first || !controls.includes(document.activeElement as HTMLElement))) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && (document.activeElement === last || !controls.includes(document.activeElement as HTMLElement))) {
      event.preventDefault();
      first.focus();
    }
  }
</script>

<svelte:window onkeydown={key} />

<div class="scrim" role="presentation" onclick={(event) => event.target === event.currentTarget && onClose()}>
  <div
    class="sheet"
    class:danger
    class:w480={width <= 480}
    class:w720={width >= 720 && width < 900}
    class:w900={width >= 900}
    role="dialog"
    aria-modal="true"
    aria-label={label}
    aria-describedby={note ? `${id}-note` : undefined}
    tabindex="-1"
    bind:this={panel}
  >
    <header>
      <div class="titles">
        <h2 id={`${id}-title`}>{title}</h2>
        {#if note}<p class="note" id={`${id}-note`}>{note}</p>{/if}
        {#if head}{@render head()}{/if}
      </div>
      <button type="button" class="ghost small" onclick={onClose} aria-label="Close" aria-keyshortcuts="Escape">Esc</button>
    </header>
    <div class="body">
      {@render children()}
    </div>
    {#if footer}<footer>{@render footer()}</footer>{/if}
  </div>
</div>

<style>
  .scrim {
    position: fixed;
    inset: 0;
    background: rgba(6, 8, 10, 0.74);
    display: flex;
    align-items: flex-start;
    justify-content: center;
    padding: 56px 20px;
    overflow-y: auto;
    z-index: 40;
  }
  .sheet {
    width: min(640px, 100%);
    background: var(--panel);
    border: 1px solid var(--line-strong);
    display: flex;
    flex-direction: column;
    margin: auto 0;
  }
  .sheet.w480 { width: min(480px, 100%); }
  .sheet.w720 { width: min(760px, 100%); }
  .sheet.w900 { width: min(960px, 100%); }
  .sheet:focus {
    outline: none;
  }
  .sheet.danger {
    border-color: var(--danger-line);
  }
  header {
    padding: 16px 20px;
    border-bottom: 1px solid var(--line);
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
  }
  .titles {
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 0;
  }
  h2 {
    font-size: 20px;
  }
  .body {
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 16px;
    min-width: 0;
  }
  footer {
    padding: 12px 20px;
    border-top: 1px solid var(--line);
    background: var(--nav);
    display: flex;
    justify-content: flex-end;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
  }
  @media (max-width: 600px) {
    .scrim { padding: 0; }
    .sheet { min-height: 100%; border: 0; }
    .body { flex: 1; }
    header, .body, footer { padding-left: 16px; padding-right: 16px; }
  }
  :global(body.modal-open) {
    overflow: hidden;
  }
</style>
