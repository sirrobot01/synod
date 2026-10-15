<script lang="ts">
  import { untrack } from "svelte";
  import { api, detail, type Node } from "./api";
  import { errorView, type APIError } from "./state";
  import { deviceNamePattern, label } from "./model";
  import { toast } from "./toast.svelte";
  import Dialog from "./ui/Dialog.svelte";
  import Icon from "./ui/Icon.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";

  // Renames one device. Apps, schedules, and snapshots keep pointing at the same device ID.
  let { node, onClose, onRenamed }: { node: Node; onClose: () => void; onRenamed: (node: Node) => void } = $props();
  let name = $state(untrack(() => node.name));
  let busy = $state(false);
  let failure = $state<APIError | null>(null);

  async function save() {
    busy = true;
    try {
      const renamed = await api.renameNode(node.id, name.trim());
      toast.show(`Renamed ${label(node)} to ${label(renamed)}`);
      onRenamed(renamed);
    } catch (error) {
      failure = detail(error);
      if (failure.code === "name_taken") failure = { ...failure, message: `Another device is named ${name.trim()}. Choose another name.` };
    } finally {
      busy = false;
    }
  }
</script>

<Dialog label="Rename device" title={`Rename ${label(node)}`} width={480} onClose={() => { if (!busy) onClose(); }}>
  <form id="rename-form" class="stack" onsubmit={(event) => { event.preventDefault(); save(); }}>
    {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
    <label class="field">Name<input class="mono" required maxlength="63" pattern={deviceNamePattern} bind:value={name} autocomplete="off" /></label>
    <p class="hint">1 to 63 letters, digits, hyphens, underscores, or dots. Not starting with a hyphen.</p>
    <p class="hint">ID <span class="mono">{node.id}</span></p>
  </form>
  {#snippet footer()}
    <button type="button" class="ghost" onclick={onClose} disabled={busy}>Cancel</button>
    <button type="submit" form="rename-form" class="primary" disabled={busy || name.trim() === "" || name.trim() === node.name}><Icon name="check" />{busy ? "Renaming" : "Rename"}</button>
  {/snippet}
</Dialog>
