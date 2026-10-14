<script lang="ts">
  import { onMount } from "svelte";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";
  import Mark from "./ui/Mark.svelte";
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
  let tab = $state<"compose" | "service">("compose");

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
  let snippet = $derived(tab === "compose" ? compose : command);

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

  async function copy() {
    await navigator.clipboard.writeText(snippet);
    copied = tab;
  }

  onMount(() => {
    create();
    const timer = setInterval(watch, 3000);
    return () => clearInterval(timer);
  });
</script>

<Dialog label="Add device" title="Add device" onClose={() => { if (!busy) onClose(); }}>
  {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
  {#if joined}
    <div class="joined">
      <Mark mark={joined.approved_at ? { glyph: "✓", shape: "circle", tone: "ok", label: "Approved" } : { glyph: "!", shape: "square-half", tone: "attn", label: "Not approved" }} shapeOnly />
      <div class="stack tight">
        <strong>{joined.name || joined.id} {joined.approved_at ? "approved" : "joined"}</strong>
        {#if joined.architecture}
          <dl class="facts"><dt>Architecture</dt><dd class="mono">{joined.architecture}</dd><dt>Docker</dt><dd class="mono">{joined.runtime_version || "unknown"}</dd><dt>Agent</dt><dd class="mono">{joined.agent_version || "unknown"}</dd></dl>
        {:else}
          <p class="loading">Waiting for the first report.</p>
        {/if}
        {#if joined.approved_at}<p class="note">Apps appear within a minute.</p>{/if}
      </div>
    </div>
  {:else}
    <div class="tabs" role="tablist" aria-label="How to install the agent">
      <button type="button" role="tab" id="tab-compose" aria-controls="panel-install" aria-selected={tab === "compose"} class:on={tab === "compose"} onclick={() => (tab = "compose")}>With Docker Compose</button>
      <button type="button" role="tab" id="tab-service" aria-controls="panel-install" aria-selected={tab === "service"} class:on={tab === "service"} onclick={() => (tab = "service")}>As a Linux service</button>
    </div>
    <div class="stack" role="tabpanel" id="panel-install" aria-labelledby={`tab-${tab}`}>
      {#if token === ""}
        {#if !failure}<p class="loading">Creating a join token</p>{/if}
      {:else}
        <ol>
          {#if tab === "compose"}
            <li>Save as <code>compose.yaml</code> on the host. Mount each app data path at the same path.</li>
            <li>Run <code>docker compose up -d</code>.</li>
          {:else}
            <li>Run as root on the host.</li>
          {/if}
        </ol>
        <pre class="mono snippet">{snippet}</pre>
        <div class="row">
          <button type="button" onclick={copy}>{copied === tab ? "Copied" : tab === "compose" ? "Copy compose.yaml" : "Copy commands"}</button>
          <span class="hint">The token joins one device and expires in 24 h.</span>
        </div>
        <p class="waiting">Waiting for the device to connect</p>
      {/if}
    </div>
  {/if}
  {#snippet footer()}
    {#if joined?.approved_at}
      <button type="button" class="primary" onclick={onClose}>Done</button>
    {:else if joined}
      <button type="button" class="ghost" onclick={onClose} disabled={busy}>Later</button>
      <button type="button" class="primary" onclick={approve} disabled={busy || !joined.architecture}>{busy ? "Approving" : "Approve"}</button>
    {:else}
      <button type="button" class="ghost" onclick={onClose}>Close</button>
    {/if}
  {/snippet}
</Dialog>

<style>
  .tabs {
    display: flex;
    border-bottom: 1px solid var(--line);
    margin-top: -8px;
  }
  .tabs button {
    border: 0;
    border-bottom: 2px solid transparent;
    background: none;
    color: var(--ink-muted);
    padding: 0 14px;
  }
  .tabs button.on {
    color: var(--ink);
    border-bottom-color: var(--ink);
  }
  ol {
    margin: 0;
    padding-left: 18px;
    color: var(--ink-body);
    display: flex;
    flex-direction: column;
    gap: 4px;
    font-size: 13px;
  }
  .snippet {
    padding: 12px 14px;
    background: var(--canvas);
    border: 1px solid var(--line);
    color: var(--ink-body);
    max-height: 260px;
    overflow: auto;
    /* A YAML file keeps its indentation. A long line scrolls instead of wrapping. */
    white-space: pre;
    overflow-wrap: normal;
  }
  .row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    flex-wrap: wrap;
  }
  .waiting {
    padding: 10px 12px;
    border: 1px dashed var(--line-strong);
    background-image: var(--hatch);
    color: var(--ink-body);
    font-size: 13px;
  }
  .joined {
    display: flex;
    gap: 12px;
    align-items: flex-start;
    padding: 14px;
    border: 1px solid var(--line-strong);
  }
  .tight {
    gap: 8px;
  }
</style>
