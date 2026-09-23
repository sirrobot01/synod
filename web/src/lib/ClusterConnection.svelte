<script lang="ts">
  import { api, detail, type CredentialInspection, type CredentialMetadata, type EndpointInspection } from "./api";
  import { errorView, type APIError } from "./state";
  import ErrorPanel from "./ErrorPanel.svelte";
  import Panel from "./ui/Panel.svelte";

  let { id }: { id: string } = $props();

  let endpoints = $state<EndpointInspection | null>(null);
  let credentials = $state<CredentialInspection | null>(null);
  let endpointFailure = $state<APIError | null>(null);
  let credentialFailure = $state<APIError | null>(null);
  let reading = $state(false);
  let selected = $state<CredentialMetadata | null>(null);
  let password = $state<string | null>(null);
  let revealing = $state(false);
  let revealFailure = $state<APIError | null>(null);
  let metadataGeneration = 0;
  let revealGeneration = 0;

  function hideSecret() {
    revealGeneration++;
    password = null;
    selected = null;
    revealing = false;
    revealFailure = null;
  }

  async function read(target: string) {
    const generation = ++metadataGeneration;
    hideSecret();
    endpoints = null;
    credentials = null;
    endpointFailure = null;
    credentialFailure = null;
    reading = true;
    const [endpointResult, credentialResult] = await Promise.allSettled([
      api.clusterEndpoints(target),
      api.clusterCredentials(target),
    ]);
    if (generation !== metadataGeneration || target !== id) return;
    if (endpointResult.status === "fulfilled") endpoints = endpointResult.value;
    else endpointFailure = detail(endpointResult.reason);
    if (credentialResult.status === "fulfilled") credentials = credentialResult.value;
    else credentialFailure = detail(credentialResult.reason);
    reading = false;
  }

  async function reveal(credential: CredentialMetadata) {
    hideSecret();
    const target = id;
    const generation = revealGeneration;
    selected = credential;
    revealing = true;
    try {
      const answer = await api.revealClusterCredential(target, credential.purpose);
      if (generation !== revealGeneration || target !== id) return;
      const returned = answer.credential;
      if (returned.cluster_id !== target || returned.credential_id !== credential.credential_id ||
          returned.purpose !== credential.purpose || returned.version !== credential.version ||
          returned.username !== credential.username) {
        revealFailure = { code: "invalid_response", message: "Credential metadata changed.", requestID: "" };
        return;
      }
      password = answer.password;
    } catch (error) {
      if (generation === revealGeneration && target === id) revealFailure = detail(error);
    } finally {
      if (generation === revealGeneration && target === id) revealing = false;
    }
  }

  $effect(() => {
    const target = id;
    void read(target);
    return () => {
      metadataGeneration++;
      hideSecret();
    };
  });
</script>

<svelte:window onpagehide={hideSecret} />

<div class="connection">
  <Panel title="Endpoints" note="declared connection metadata">
    {#snippet actions()}
      <button type="button" onclick={() => read(id)} disabled={reading}>
        {reading ? "Reading metadata" : "Refresh connection metadata"}
      </button>
    {/snippet}
    {#if endpointFailure}
      <div class="block"><ErrorPanel view={errorView(endpointFailure)} /></div>
    {:else if endpoints}
      {#if endpoints.unavailable_reason}
        <p class="message">Endpoints unavailable: <span class="mono">{endpoints.unavailable_reason}</span>.</p>
      {/if}
      {#if endpoints.endpoints.length === 0}
        <p class="message">No declared endpoint address is available.</p>
      {/if}
      {#each endpoints.endpoints as endpoint (endpoint.id)}
        <div class="endpoint">
          <div class="row">
            <span class="mono purpose">{endpoints.engine}</span>
            <span class="chip mono">{endpoint.client_mode}</span>
          </div>
          <p class="address mono">{endpoint.host.includes(":") ? `[${endpoint.host}]` : endpoint.host}:{endpoint.port}</p>
          <dl class="facts mono">
            <dt>availability profile</dt><dd>{endpoint.availability_profile}</dd>
            <dt>database</dt><dd>{endpoint.database || "Not applicable"}</dd>
            <dt>transport</dt><dd>{endpoint.transport}</dd>
            <dt>TLS mode</dt><dd>{endpoint.tls_mode}</dd>
          </dl>
          {#if endpoint.tls_mode === "disabled"}
            <p class="warning">TLS is disabled. Database traffic is plaintext.</p>
          {/if}
        </div>
      {/each}
    {:else}
      <p class="message">Reading endpoint metadata.</p>
    {/if}
    {#snippet foot()}
      Declared addresses do not prove reachability or database health. Use the stated client mode
      and availability profile when configuring an application.
    {/snippet}
  </Panel>

  <Panel title="Credentials" note="metadata by default">
    <p class="message">Reveal is an explicit, audited request for the stated purpose and version.</p>
    {#if credentialFailure}
      <div class="block"><ErrorPanel view={errorView(credentialFailure)} /></div>
    {:else if credentials}
      {#if credentials.credentials.length === 0}
        <p class="message">No credential metadata is available.</p>
      {/if}
      {#each credentials.credentials as credential (credential.credential_id)}
        <div class="credential">
          <div class="row">
            <span class="mono purpose">{credential.purpose}</span>
            <span class="chip mono">version {credential.version}</span>
          </div>
          <dl class="facts mono">
            <dt>username</dt><dd>{credential.username}</dd>
            <dt>credential ID</dt><dd>{credential.credential_id}</dd>
          </dl>
          <div class="buttons">
            <button type="button" disabled={revealing || reading} onclick={() => reveal(credential)}>
              Reveal {credential.purpose} — audited
            </button>
            {#if selected?.credential_id === credential.credential_id}
              <button type="button" onclick={hideSecret}>Hide {credential.purpose} credential</button>
            {/if}
          </div>
          {#if selected?.credential_id === credential.credential_id}
            <div class="reveal" aria-busy={revealing}>
              {#if revealing}
                <p class="message">Requesting audited access.</p>
              {:else if revealFailure}
                <ErrorPanel view={{ ...errorView(revealFailure), consequence: "No credential is displayed. Refresh metadata before another explicit reveal." }} />
              {:else if password !== null}
                <label class="password-label" for={`credential-password-${credential.credential_id}`}>
                  {credential.purpose} password · version {credential.version}
                </label>
                <input id={`credential-password-${credential.credential_id}`} class="mono password" type="text"
                  value={password} readonly autocomplete="off" spellcheck={false} />
              {/if}
            </div>
          {/if}
        </div>
      {/each}
    {:else}
      <p class="message">Reading credential metadata.</p>
    {/if}
    {#snippet foot()}
      Revealed values stay in this panel until hidden or navigation changes. Synod does not copy
      them automatically or add them to a connection URL.
    {/snippet}
  </Panel>
</div>

<style>
  .connection { display: flex; flex-direction: column; gap: 20px; min-width: 0; }
  .endpoint, .credential { padding: 13px 16px; border-top: 1px solid var(--line); }
  .row { display: flex; align-items: center; gap: 9px; flex-wrap: wrap; }
  .purpose { font-size: 12px; font-weight: 500; }
  .chip { font-size: 10px; border: 1px solid var(--line-strong); color: var(--ink-muted); padding: 2px 6px; }
  .address { margin: 8px 0 0; font-size: 11px; color: var(--ink-body); overflow-wrap: anywhere; }
  .facts { display: grid; grid-template-columns: auto minmax(0, 1fr); row-gap: 4px; column-gap: 12px; margin: 9px 0 0; font-size: 10px; }
  .facts dt { color: var(--ink-faint); }
  .facts dd { margin: 0; color: var(--ink-body); overflow-wrap: anywhere; }
  .message { margin: 0; padding: 12px 16px; color: var(--ink-dim); font-size: 11px; line-height: 1.6; }
  .warning { margin: 10px 0 0; color: var(--attn); font-size: 11px; line-height: 1.5; }
  .block { padding: 12px 16px; }
  .buttons { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
  .reveal { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; }
  .reveal .message { padding: 0; }
  .password-label { font-size: 11px; color: var(--ink-body); }
  .password { width: 100%; box-sizing: border-box; font-size: 11px; }
</style>
