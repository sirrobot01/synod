<script lang="ts">
  import { api, detail, uncertain, type Destination, type DestinationEntry, type Node } from "./api";
  import { errorView, type APIError } from "./state";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";
  let { nodes, onClose, onSaved }: { nodes: Node[]; onClose: () => void; onSaved: () => void } = $props();
  let kind = $state<Destination["location"]["kind"]>("device");
  let name = $state(""); let nodeID = $state(""); let directory = $state(""); let password = $state("");
  let host = $state(""); let port = $state(22); let user = $state(""); let knownHosts = $state(""); let sshKey = $state("");
  let endpoint = $state(""); let bucket = $state(""); let prefix = $state(""); let region = $state("");
  let accessKey = $state(""); let secretKey = $state(""); let sessionToken = $state("");
  let savedPassword = $state(false); let busy = $state(false);
  let submitted = $state<DestinationEntry | null>(null);
  let failure = $state<APIError | null>(null);
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
    try { await api.addDestination(submitted); password = sshKey = accessKey = secretKey = sessionToken = ""; submitted = null; onSaved(); }
    catch (error) { failure = detail(error); if (!uncertain(error)) submitted = null; }
    finally { busy = false; }
  }
</script>
<Dialog label="Add destination" title="Add destination" onClose={() => { if (!busy) onClose(); }}>
  <form class="stack" onsubmit={(event) => { event.preventDefault(); save(); }} autocomplete="off">
    {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
    <fieldset class="stack" disabled={busy || submitted !== null}>
      <legend class="label">Repository</legend>
      <label>Name<input required maxlength="128" bind:value={name} placeholder="Backups on odin" /></label>
      <label>Destination type<select bind:value={kind}><option value="device">Synod device</option><option value="sftp">SFTP server</option><option value="s3">S3-compatible bucket</option><option value="local">Local path</option></select></label>
      {#if kind === "device" || kind === "local"}<label>{kind === "local" ? "Device that owns this path" : "Destination device"}<select required bind:value={nodeID}><option value="" disabled>Select an approved device</option>{#each nodes.filter(n => n.approved_at) as node}<option value={node.id}>{node.name || node.id}</option>{/each}</select></label>{/if}
      {#if kind === "local"}<p class="note">Only this device can use this local repository. To share it with other agents, choose Synod device.</p>{/if}
      {#if kind !== "s3"}<label>Absolute directory path<input required bind:value={directory} placeholder="/srv/synod-backups" pattern="/.*" /></label>{/if}
      {#if kind === "sftp"}
        <div class="columns"><label>Host<input required bind:value={host} /></label><label>Port<input type="number" min="1" max="65535" required bind:value={port} /></label></div>
        <label>SSH user<input required bind:value={user} /></label><label>Trusted known_hosts entry<textarea required bind:value={knownHosts}></textarea></label><label>SSH private key<textarea required bind:value={sshKey} spellcheck="false"></textarea></label>
      {:else if kind === "s3"}
        <label>Endpoint<input type="url" required bind:value={endpoint} placeholder="https://s3.example.com" /></label><label>Bucket<input required bind:value={bucket} /></label>
        <div class="columns"><label>Prefix (optional)<input bind:value={prefix} /></label><label>Region (optional)<input bind:value={region} /></label></div>
        <label>Access key ID<input required type="password" bind:value={accessKey} /></label><label>Secret access key<input required type="password" bind:value={secretKey} /></label><label>Session token (optional)<input type="password" bind:value={sessionToken} /></label>
      {/if}
      <label>Repository password (optional)<input type="password" bind:value={password} autocomplete="new-password" /></label>
      {#if password}
        <p class="callout">This custom password is not in your recovery kit. Without it, nobody can read this repository, including Synod. Use the existing password when you connect an existing repository.</p>
        <label class="check"><input type="checkbox" required bind:checked={savedPassword} />I saved this custom password with my recovery kit, outside this device.</label>
      {:else}
        <p class="note">Synod uses the password in the recovery kit that <span class="mono">synod server init</span> printed. Keep that kit outside your Synod devices.</p>
      {/if}
    </fieldset>
    {#if submitted}<p class="callout">The reply was not confirmed. Retry the same destination request before you add another.</p>{/if}
    <button class="primary" disabled={busy || (password !== "" && !savedPassword)}>{busy ? "Saving…" : submitted ? "Retry same request" : "Add destination"}</button>
  </form>
</Dialog>
