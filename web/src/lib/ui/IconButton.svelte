<script lang="ts">
  import Icon, { type IconName } from "./Icon.svelte";

  // An icon-only button. The label is its accessible name and its tooltip.
  let { icon, label, onclick, disabled = false, tone = "", shortcut = undefined }: { icon: IconName; label: string; onclick: (event: MouseEvent) => void; disabled?: boolean; tone?: "" | "danger"; shortcut?: string } = $props();
</script>

<button type="button" class="icon-button" class:danger={tone === "danger"} aria-label={label} title={label} aria-keyshortcuts={shortcut} {disabled} {onclick}><Icon name={icon} /></button>

<style>
  .icon-button {
    width: 32px;
    min-height: 32px;
    height: 32px;
    padding: 0;
    border-color: transparent;
    color: var(--ink-muted);
  }
  .icon-button:hover:not(:disabled) {
    color: var(--ink);
    background: var(--head);
    border-color: var(--line);
  }
  .icon-button.danger:hover:not(:disabled) {
    color: var(--danger-ink);
    border-color: var(--danger-line);
  }
  .icon-button:disabled {
    border-color: transparent;
    opacity: 0.5;
  }
</style>
