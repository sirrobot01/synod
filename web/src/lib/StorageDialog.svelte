<script lang="ts">
  import { api, detail, uncertain, type Destination, type DestinationEntry, type Node } from "./api";
  import { errorView, type APIError } from "./state";
  import { storageTypes } from "./model";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";

  let { nodes, onClose, onSaved }: { nodes: Node[]; onClose: () => void; onSaved: () => void } = $props();

  let kind = $state<Destination["location"]["kind"]>("device");
  let name = $state(""); let nodeID = $state(""); let directory = $state(""); let password = $state("");
  let host = $state(""); let port = $state(22); let user = $state(""); let knownHosts = $state(""); let sshKey = $state("");
  let endpoint = $state(""); let bucket = $state(""); let prefix = $state(""); let region = $state("");
  let accessKey = $state(""); let secretKey = $state(""); let sessionToken = $state("");
  let savedPassword = $state(false); let busy = $state(false);
  // A request whose reply was lost stays as it was, so its retry cannot add a second storage.
  let submitted = $state<DestinationEntry | null>(null);
  let failure = $state<APIError | null>(null);
  let approved = $derived(nodes.filter((n) => n.approved_at));
  let owner = $derived(approved.find((n) => n.id === nodeID));

  async function save() {
    busy = true;
    if (!submitted) {
      const id = crypto.randomUUID();
      const location: Destination["location"] = { kind };
      const credentials: DestinationEntry["credentials"] = { password };
      if (kind !== "s3") location.path = directory;
      if (kind === "device") location.device = { node_id: nodeID, destination_id: id };
      if (kind === "sftp") { location.sftp = { host, port, user, known_hosts: knownHosts }; credentials.ssh_key = sshKey; }
      if (kind === "s3") { location.s3 = { endpoint, bucket, prefix, region }; credentials.access_key_id = accessKey; credentials.secret_access_key = secretKey; if (sessionToken) credentials.session_token = sessionToken; }
      submitted = { destination: { id, name, location, ...(kind === "device" || kind === "local" ? { node_id: nodeID } : {}) }, credentials };
    }
    try {
      await api.addDestination(submitted);
      password = sshKey = accessKey = secretKey = sessionToken = "";
      submitted = null;
      onSaved();
    } catch (error) {
      failure = detail(error);
      if (!uncertain(error)) submitted = null;
    } finally {
      busy = false;
    }
  }
</script>

<Dialog label="Add storage" title="Add storage" width={720} onClose={() => { if (!busy) onClose(); }}>
  <form id="storage-form" class="stack" onsubmit={(event) => { event.preventDefault(); save(); }} autocomplete="off">
    {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
    <fieldset class="stack" disabled={busy || submitted !== null}>
      <legend class="sr">Type of backup storage</legend>
      <div class="types">
        {#each storageTypes as type (type.kind)}
          <button type="button" class="pick" aria-pressed={kind === type.kind} onclick={() => (kind = type.kind)}>
            <span class="pick-head"><strong>{type.title}</strong>{#if type.kind === "device"}<span class="dim">recommended</span>{/if}</span>
            <span class="note">{type.text}</span>
          </button>
        {/each}
      </div>
      <div class="fields">
        {#if kind === "device" || kind === "local"}
          <label class="field">{kind === "device" ? "Device that stores the snapshots" : "Device with the disk"}
            <select required bind:value={nodeID}>
              <option value="" disabled>{approved.length ? "Choose a device" : "No approved device"}</option>
              {#each approved as node (node.id)}<option value={node.id}>{node.name || node.id} · {node.architecture}</option>{/each}
            </select>
          </label>
        {/if}
        {#if kind === "sftp"}
          <label class="field">Host<input class="mono" required bind:value={host} placeholder="nas.home.arpa" /></label>
          <label class="field">Port<input type="number" min="1" max="65535" required bind:value={port} /></label>
          <label class="field">SSH user<input required bind:value={user} /></label>
        {/if}
        {#if kind === "s3"}
          <label class="field">Endpoint<input type="url" required bind:value={endpoint} placeholder="https://s3.us-west-000.backblazeb2.com" /></label>
          <label class="field">Bucket<input required bind:value={bucket} /></label>
          <label class="field">Prefix (optional)<input bind:value={prefix} placeholder="synod" /></label>
          <label class="field">Region (optional)<input bind:value={region} /></label>
          <label class="field">Access key ID<input required type="password" bind:value={accessKey} /></label>
          <label class="field">Secret access key<input required type="password" bind:value={secretKey} /></label>
          <label class="field full">Session token (optional)<input type="password" bind:value={sessionToken} /></label>
        {/if}
        {#if kind !== "s3"}
          <label class="field">Path<input class="mono" required bind:value={directory} placeholder="/srv/synod-backups" pattern="/.*" title="An absolute path, such as /srv/synod-backups" /></label>
        {/if}
        <label class="field" class:full={kind !== "device" && kind !== "local" && kind !== "sftp"}>Name<input required maxlength="128" bind:value={name} placeholder={owner ? `${owner.name}-disk` : kind === "s3" ? "b2-homelab" : "nas"} /></label>
        {#if kind === "sftp"}
          <label class="field full">known_hosts line<textarea required bind:value={knownHosts} placeholder="nas.home.arpa ssh-ed25519 AAAA…"></textarea></label>
          <label class="field full">SSH private key<textarea required bind:value={sshKey} spellcheck="false"></textarea></label>
        {/if}
      </div>
      {#if kind === "local"}<p class="callout attn">Only {owner?.name || "this device"} can read or write this storage. It is lost with that device.</p>{/if}
      <details>
        <summary>Custom repository password</summary>
        <div class="stack inset">
          <label class="field">Custom password (optional)<input type="password" bind:value={password} autocomplete="new-password" /></label>
          {#if password}
            <p class="callout attn">This password is not in the recovery kit. Without it the repository cannot be read. For an existing Synod repository, enter its password.</p>
            <label class="check"><input type="checkbox" required bind:checked={savedPassword} />I saved this password with the recovery kit.</label>
          {/if}
        </div>
      </details>
    </fieldset>
    {#if !password}<p class="hint">Encrypted with the repository password in the recovery kit.</p>{/if}
    {#if submitted}<p class="callout attn">No reply. Retry the same request before you add other storage.</p>{/if}
  </form>
  {#snippet footer()}
    <button type="button" class="ghost" onclick={onClose} disabled={busy}>Cancel</button>
    <button type="submit" form="storage-form" class="primary" disabled={busy || (password !== "" && !savedPassword)}>{busy ? "Adding" : submitted ? "Retry same request" : "Add storage"}</button>
  {/snippet}
</Dialog>

<style>
  .types {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;
  }
  .pick-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 8px;
  }
  .pick-head .dim {
    font-size: 12px;
  }
  .fields {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 12px;
  }
  .full {
    grid-column: 1 / -1;
  }
  .inset {
    margin-top: 12px;
  }
  @media (max-width: 600px) {
    .types,
    .fields {
      grid-template-columns: 1fr;
    }
  }
</style>
