<script lang="ts">
  // A mocked region says so in the interface. A reader must never mistake a
  // placeholder for an observation. Every mock has a gap recorded against
  // specification 15 before it appears here.
  let { gap }: { gap: string } = $props();
</script>

<p class="mock hatched" role="note">
  <span class="tag mono">MOCK</span>
  <span>Placeholder data. The API does not serve this yet. Gap: <span class="mono">{gap}</span>.</span>
</p>

<style>
  .mock {
    display: flex;
    gap: 10px;
    align-items: baseline;
    border: 1px dashed var(--line-strong);
    padding: 9px 12px;
    margin: 0;
    color: var(--ink-dim);
    font-size: 12px;
    line-height: 1.5;
  }
  .tag {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.12em;
    color: var(--ink-muted);
    flex-shrink: 0;
  }
</style>
