<script lang="ts">
  import type { Snippet } from "svelte";
  import { follow, go, path, type ScreenName } from "../router.svelte";

  // A link keeps a real address, so it opens in a new tab and survives a reload.
  let { screen, id = "", query = "", class: kind = "", label = undefined, children }: { screen: ScreenName; id?: string; query?: string; class?: string; label?: string; children: Snippet } = $props();
</script>

<a class={kind} href={path(screen, id, query)} aria-label={label} onclick={(event) => follow(event, () => go(screen, id, query))}>{@render children()}</a>
