<script lang="ts">
  import { onMount } from "svelte";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";
  import { api, detail, type Node } from "./api";
  import { encodeJoinToken, newSecret } from "./jointoken";
  import { errorView, type APIError } from "./state";

  let { onClose, onChanged }: { onClose: () => void; onChanged: () => void } = $props();

  const lifetime = 24 * 3600;
  let token = $state("");
  let failure = $state<APIError | null>(null);
  let known: Set<string> | null = null;
  let joined = $state<Node | null>(null);
  let busy = $state(false);
  let copied = $state("");

  let compose = $derived(`services:
  synod-agent:
    image: ghcr.io/sirrobot01/synod:latest
    container_name: synod-agent
    restart: unless-stopped
    privileged: true
    pid: host
    network_mode: host
    environment:
      SYNOD_JOIN_TOKEN: "${token}"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - /var/lib/docker:/var/lib/docker
      - /var/lib/synod:/var/lib/synod
      # Add each directory with app files, at the same path.
      - /srv:/srv
`);
  let command = $derived(`curl -fsSL https://synod.biodun.dev/install.sh | sh\nsynod agent join '${token}'`);

  async function create() {
    try {
      const [installation, nodes] = await Promise.all([api.installation(), api.nodes()]);
      known = new Set(nodes.map((node) => node.id));
      const secret = newSecret();
      await api.registerJoinToken(secret, lifetime, crypto.randomUUID());
      token = encodeJoinToken({ address: installation.join_address, installation_id: installation.installation_id, fingerprint: installation.api_ca_fingerprint, secret });
    } catch (error) {
      failure = detail(error);
    }
  }

  // A device that joined after the dialog opened is the new one.
  async function watch() {
    if (!known || joined?.approved_at) return;
    try {
      const nodes = await api.nodes();
      joined = nodes.find((node) => !known!.has(node.id)) ?? null;
    } catch {
      // The next poll tries again. The token stays valid.
    }
  }

  async function approve() {
    if (!joined) return;
    busy = true;
    try {
      joined = await api.approveNode(joined.id);
      failure = null;
      onChanged();
    } catch (error) {
      failure = detail(error);
    } finally {
      busy = false;
    }
  }

  async function copy(name: string, text: string) {
    await navigator.clipboard.writeText(text);
    copied = name;
  }

  onMount(() => {
    create();
    const timer = setInterval(watch, 3000);
    return () => clearInterval(timer);
  });
</script>

<Dialog label="Add device" title="Add device" note="Linux device with Docker" onClose={() => { if (!busy) onClose(); }}>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if token === ""}
    {#if !failure}<p class="note">Creating a join token.</p>{/if}
  {:else if joined}
    <section>
      <h2>{joined.name || joined.id}</h2>
      {#if joined.approved_at}
        <p class="note">The device is approved. Synod finds its apps within one minute.</p>
        <button type="button" class="primary" onclick={onClose}>Done</button>
      {:else}
        <p class="note">{joined.architecture ? `The device joined (${joined.architecture}). Approve it to back up its apps.` : "The device joined. Waiting for its first report."}</p>
        <button type="button" class="primary" onclick={approve} disabled={busy || !joined.architecture}>{busy ? "Approving" : "Approve"}</button>
      {/if}
    </section>
  {:else}
    <section>
      <h2>Docker Compose</h2>
      <p class="note">On the device, save this as <code>compose.yaml</code> and run <code>docker compose up -d</code>. Mount each directory with app files at the same path.</p>
      <pre class="mono">{compose}</pre>
      <button type="button" onclick={() => copy("compose", compose)}>{copied === "compose" ? "Copied" : "Copy Compose file"}</button>
    </section>
    <section>
      <h2>Without Docker</h2>
      <p class="note">Run these commands as root on the device.</p>
      <pre class="mono">{command}</pre>
      <button type="button" onclick={() => copy("command", command)}>{copied === "command" ? "Copied" : "Copy commands"}</button>
    </section>
    <p class="note">The token joins one device and is valid for 24 hours. Waiting for the device to join.</p>
  {/if}
</Dialog>

<style>
  section { display: grid; gap: 8px; margin-bottom: 20px; }
  section button { justify-self: start; }
  h2 { font-size: 14px; margin: 0; }
  pre { margin: 0; padding: 10px; border: 1px solid var(--line); font-size: 11px; white-space: pre-wrap; overflow-wrap: anywhere; }
</style>
