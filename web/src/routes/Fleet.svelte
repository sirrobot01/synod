<script lang="ts">
  import { follow, path, type ScreenName } from "../lib/router.svelte";
  let { go }: { go: (screen: ScreenName, id?: string) => void } = $props();
</script>

<header>
  <h1>Dashboard</h1>
  <p class="note">Back up Docker apps. Restore them on another device.</p>
</header>
<section class="panel">
  <h2>Set up your devices</h2>
  <p>Join each device with <code>synod agent join TOKEN</code>. Then approve it in Nodes.</p>
  <a href={path("nodes")} onclick={(event) => follow(event, () => go("nodes"))}>View devices</a>
</section>
<p class="note">Backup and restore are not available in this development build yet.</p>

<style>
  header { margin-bottom: 24px; }
  .panel { padding: 20px; margin-bottom: 20px; }
  h2 { font-size: 16px; }
  p { line-height: 1.6; }
</style>
