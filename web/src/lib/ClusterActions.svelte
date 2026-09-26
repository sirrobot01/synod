<script lang="ts">
  import { onDestroy } from "svelte";
  import { api, detail, Failure, type ActionConfirmation, type ActionPlan, type Admission, type Cluster, type ClusterAction, type ClusterMember, type ScaleDownPlan, type SetupRecoveryPlan } from "./api";
  import { errorView, type APIError } from "./state";
  import { follow, path } from "./router.svelte";
  import Dialog from "./ui/Dialog.svelte";
  import ErrorPanel from "./ErrorPanel.svelte";
  import Panel from "./ui/Panel.svelte";

  let { id, actorID, cluster, members = null, onOpenOperation, onRefresh }: {
    id: string;
    actorID: string;
    cluster: Cluster | null;
    members?: ClusterMember[] | null;
    onOpenOperation: (id: string) => void;
    onRefresh?: () => void;
  } = $props();

  type Mutation = ClusterAction | "scale_up" | "scale_down";
  type Preview = ActionPlan | ScaleDownPlan | SetupRecoveryPlan;
  type ReviewedAction = Exclude<Mutation, "start" | "stop" | "scale_up">;
  const reviews = {
    delete: { action: "delete", label: "cluster deletion", confirm: "Confirm delete" },
    scale_down: { action: "scale_down", label: "replica removal", confirm: "Confirm replica removal" },
    "recover-setup": { action: "recover_setup", label: "setup recovery", confirm: "Confirm setup recovery" },
  } as const;

  interface Attempt {
    version: 1;
    actorID: string;
    targetID: string;
    targetName: string;
    action: Mutation;
    expectedRevision: number;
    key: string;
    confirmation?: ActionConfirmation;
    plan?: Preview;
    admission?: Admission;
    dispatchID: string;
  }

  let storageKey = $derived(`synod.lifecycle:${JSON.stringify([actorID, id])}`);
  let loadedKey = $state("");
  let attempt = $state<Attempt | null>(null);
  let blocked = $state(false);
  let failure = $state<APIError | null>(null);
  let inFlight = $state<Record<string, boolean>>({});
  let dialog = $state(false);
  let dialogAction = $state<ReviewedAction>("delete");
  let plan = $state<Preview | null>(null);
  let typedName = $state("");
  let previewLoading = $state(false);
  let previewGeneration = 0;
  let alive = true;
  onDestroy(() => { alive = false; previewGeneration++; });
  let current = $derived(loadedKey === storageKey);
  let sending = $derived(Boolean(inFlight[storageKey]));
  let unresolved = $derived(current && attempt !== null && !attempt.admission);
  let canCreate = $derived(current && !blocked && !sending && !unresolved && cluster?.id === id && Number.isSafeInteger(cluster.desired_revision) && cluster.desired_revision > 0 && cluster.desired_lifecycle !== "deleted");
  let standalonePostgres = $derived(cluster?.spec.engine === "postgres" && cluster.spec.postgresql.version === "17.11" && cluster.spec.postgresql.topology === "standalone");
  let replicas = $derived(members?.filter((member) => member.role === "replica") ?? []);
  let canScale = $derived(canCreate && cluster !== null && cluster?.spec.engine === "redis" && cluster.spec.redis.persistence !== "none" && replicas.length <= 1 &&
    cluster.desired_lifecycle === "running" && members !== null && [0, 1].includes(cluster.desired_replicas));
  let residualReplica = $derived(members?.some((member) => member.role === "replica" && member.state === "residual") ?? false);
  let canAdd = $derived(canScale && (cluster?.desired_replicas === 0 || residualReplica));
  let canRemove = $derived(canScale && members?.some((member) => member.role === "replica"));
  let canRecover = $derived(canCreate && standalonePostgres && cluster?.desired_replicas === 0 && cluster.desired_lifecycle === "running" && members?.length === 1 && replicas.length === 0);
  let shownPlan = $derived(unresolved && attempt?.plan ? attempt.plan : plan);

  // Stored attempts contain request identity only. A missing admission always
  // restores as uncertain, including a page closed while the request was in flight.
  function validStored(value: unknown, actor: string, target: string): value is Attempt {
    if (!value || typeof value !== "object") return false;
    const record = value as Attempt;
    if (Object.keys(record).some((key) => !["version", "actorID", "targetID", "targetName", "action", "expectedRevision", "key", "confirmation", "plan", "admission", "dispatchID"].includes(key))) return false;
    if (record.version !== 1 || record.actorID !== actor || record.targetID !== target ||
      typeof record.targetName !== "string" || !/^[a-z][a-z0-9-]{0,62}$/.test(record.targetName) ||
      !["start", "stop", "delete", "scale_up", "scale_down", "recover-setup"].includes(record.action) ||
      !Number.isSafeInteger(record.expectedRevision) || record.expectedRevision <= 0 ||
      typeof record.key !== "string" || !/^[!-~]{1,128}$/.test(record.key)) return false;
    if (record.action in reviews) {
      const preview = record.plan;
      const confirmation = record.confirmation;
      if (record.action === "scale_down" && (!preview || preview.action !== "scale_down" || typeof preview.member_id !== "string")) return false;
      if (record.action === "delete" && preview && ("member_id" in preview || "resource_id" in preview)) return false;
      if (record.action === "scale_down" && preview && "resource_id" in preview) return false;
      if (record.action === "recover-setup" &&
        (!preview || !("member_id" in preview) || !("resource_id" in preview) || preview.retained_volumes?.length !== 1)) return false;
      if (!preview || !confirmation ||
        Object.keys(preview).some((key) => !["cluster_id", "cluster_name", "action", "expected_revision", "plan_token", "data_disposition", "retained_volumes", "downtime", "expires_at", "member_id", "resource_id"].includes(key)) ||
        Object.keys(confirmation).some((key) => !["plan_token", "confirm", "data_disposition"].includes(key)) || preview.cluster_id !== target || preview.cluster_name !== record.targetName ||
        preview.action !== reviews[record.action as ReviewedAction].action || preview.expected_revision !== record.expectedRevision ||
        preview.data_disposition !== "retain" || typeof preview.plan_token !== "string" || !preview.plan_token ||
        typeof preview.downtime !== "string" || !preview.downtime ||
        typeof preview.expires_at !== "string" || !Number.isFinite(Date.parse(preview.expires_at)) ||
        !Array.isArray(preview.retained_volumes) || new Set(preview.retained_volumes).size !== preview.retained_volumes.length || !preview.retained_volumes.every((volume) => typeof volume === "string" && volume.length > 0) ||
        confirmation.plan_token !== preview.plan_token || confirmation.confirm !== true || confirmation.data_disposition !== "retain") return false;
    } else if (record.plan !== undefined || record.confirmation !== undefined) return false;
    const accepted = record.admission;
    const identifiers = [record.targetID, record.dispatchID, ...(record.plan ? [record.plan.plan_token, ...record.plan.retained_volumes, ...("member_id" in record.plan ? [record.plan.member_id] : []), ...("resource_id" in record.plan ? [record.plan.resource_id] : [])] : []), ...(accepted ? [accepted.operation_id] : [])];
    if (!identifiers.every((value) => typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value) && value !== "00000000-0000-0000-0000-000000000000")) return false;
    const dates = [...(record.plan ? [record.plan.expires_at] : []), ...(accepted ? [accepted.accepted_at] : [])];
    if (!dates.every((value) => typeof value === "string" && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?(?:Z|[+-]\d\d:\d\d)$/.test(value) && Number.isFinite(Date.parse(value)) && !value.startsWith("0001-") && new Date(`${value.slice(0, 19)}Z`).toISOString().slice(0, 19) === value.slice(0, 19))) return false;
    if (accepted && Object.keys(accepted).some((key) => !["cluster_id", "operation_id", "desired_revision", "accepted_at", "replayed"].includes(key))) return false;
    return accepted === undefined || (accepted !== null && accepted.cluster_id === target &&
      typeof accepted.operation_id === "string" && accepted.operation_id.length > 0 &&
      Number.isSafeInteger(accepted.desired_revision) && accepted.desired_revision > 0 &&
      typeof accepted.accepted_at === "string" && Number.isFinite(Date.parse(accepted.accepted_at)) && typeof accepted.replayed === "boolean");
  }

  // A completion from an unmounted view must not replace a newer request.
  function matchingSaved(key: string, request: Attempt): Attempt | null {
    const saved: unknown = JSON.parse(sessionStorage.getItem(key) ?? "null");
    if (!validStored(saved, request.actorID, request.targetID) || saved.key !== request.key ||
      saved.action !== request.action || saved.targetName !== request.targetName || saved.expectedRevision !== request.expectedRevision ||
      JSON.stringify(saved.confirmation) !== JSON.stringify(request.confirmation) || JSON.stringify(saved.plan) !== JSON.stringify(request.plan)) return null;
    return saved;
  }

  $effect(() => {
    const key = storageKey;
    const actor = actorID;
    const target = id;
    previewGeneration++;
    dialog = false;
    plan = null;
    typedName = "";
    previewLoading = false;
    failure = null;
    attempt = null;
    blocked = false;
    try {
      const saved = sessionStorage.getItem(key);
      if (saved !== null) {
        const value: unknown = JSON.parse(saved);
        if (!validStored(value, actor, target)) throw new Error("invalid stored attempt");
        attempt = value;
      }
    } catch {
      blocked = true;
      failure = { code: "lifecycle_storage_unavailable", message: "The saved lifecycle request cannot be read safely. New actions are blocked. Resolve its outcome in Operations before continuing.", requestID: "" };
    }
    loadedKey = key;
  });

  function closeDialog() {
    previewGeneration++;
    dialog = false;
    previewLoading = false;
    plan = null;
    typedName = "";
  }

  async function preview(action: ReviewedAction) {
    if (!canCreate || (action === "scale_down" && !canRemove) || (action === "recover-setup" && !canRecover)) return;
    dialogAction = action;
    const key = storageKey;
    const target = id;
    const generation = ++previewGeneration;
    dialog = true;
    plan = null;
    typedName = "";
    failure = null;
    previewLoading = true;
    try {
      const result = action === "delete" ? await api.planDelete(target) : action === "scale_down" ? await api.planScaleDown(target) :
        await api.planSetupRecovery(target);
      if (storageKey !== key || generation !== previewGeneration) return;
      plan = result;
    } catch (error) {
      if (storageKey === key && generation === previewGeneration) failure = detail(error);
    } finally {
      if (storageKey === key && generation === previewGeneration) previewLoading = false;
    }
  }

  function openPlan(action: ReviewedAction) {
    dialogAction = action;
    if (unresolved && attempt?.action === action) {
      dialog = true;
      return;
    }
    void preview(action);
  }

  function begin(action: Mutation) {
    if (!canCreate || !cluster || (action === "scale_up" && !canAdd) || (action === "scale_down" && !canRemove) ||
      (action === "recover-setup" && !canRecover)) return;
    const reviewed = action in reviews;
    if (reviewed && (!plan || plan.action !== reviews[action as ReviewedAction].action || typedName !== plan.cluster_name)) return;
    if (reviewed && plan && Date.parse(plan.expires_at) <= Date.now()) {
      plan = null;
      typedName = "";
      failure = { code: "plan_expired", message: "This preview expired. Fetch a new preview and confirm the name again.", requestID: "" };
      return;
    }
    const request: Attempt = {
      version: 1, actorID, targetID: id, targetName: cluster.spec.name,
      action, expectedRevision: cluster.desired_revision, key: crypto.randomUUID(), dispatchID: crypto.randomUUID(),
    };
    if (reviewed && plan) {
      request.targetName = plan.cluster_name;
      request.expectedRevision = plan.expected_revision;
      request.plan = plan;
      request.confirmation = { plan_token: plan.plan_token, confirm: true, data_disposition: "retain" };
    }
    void dispatch(request, storageKey);
  }

  async function dispatch(request: Attempt, key: string, retry = false) {
    if (inFlight[key] || blocked) return;
    if (retry) {
      try {
        const saved = matchingSaved(key, request);
        if (!saved) throw new Error("saved request changed");
        if (saved.admission) {
          attempt = saved;
          failure = null;
          closeDialog();
          onRefresh?.();
          return;
        }
      } catch {
        blocked = true;
        failure = { code: "lifecycle_storage_unavailable", message: "The saved request changed or cannot be read. No retry was sent. Resolve its outcome in Operations.", requestID: "" };
        return;
      }
      request = { ...request, dispatchID: crypto.randomUUID() };
    }
    // Do not send a request whose exact retry identity cannot survive reload.
    try {
      sessionStorage.setItem(key, JSON.stringify(request));
    } catch {
      failure = { code: "lifecycle_storage_unavailable", message: "The lifecycle request could not be saved. No request was sent.", requestID: "" };
      blocked = true;
      return;
    }
    attempt = request;
    failure = null;
    inFlight[key] = true;
    try {
      const admission = request.action === "scale_up" || request.action === "scale_down"
        ? await api.scaleCluster(request.targetID, request.action === "scale_up" ? 1 : 0, request.expectedRevision, request.key, request.confirmation)
        : await api.actOnCluster(request.targetID, request.action, request.expectedRevision, request.key, request.confirmation);
      const accepted = { ...request, admission };
      let persisted = true;
      try {
        const saved = matchingSaved(key, request);
        if (saved && !saved.admission) sessionStorage.setItem(key, JSON.stringify({ ...accepted, dispatchID: saved.dispatchID }));
      } catch { persisted = false; }
      if (!alive || storageKey !== key) return;
      attempt = accepted;
      closeDialog();
      if (!persisted) failure = { code: "lifecycle_storage_unavailable", message: "The operation was accepted, but its link could not be saved. Keep the operation ID. The original request remains safe to retry.", requestID: "" };
      onRefresh?.();
    } catch (error) {
      const problem = detail(error);
      const uncertain = !(error instanceof Failure) || error.status >= 500 || ["transport_error", "outcome_unknown", "invalid_response"].includes(problem.code);
      let removed = false;
      let saved: Attempt | null = null;
      const provesAbsent = ["revision_conflict", "operation_conflict"].includes(problem.code);
      try {
        saved = matchingSaved(key, request);
        if (!uncertain && saved && !saved.admission && saved.dispatchID === request.dispatchID && (!retry || provesAbsent)) {
          sessionStorage.removeItem(key);
          removed = true;
        }
      } catch { /* Keep the saved request when storage cannot be changed. */ }
      if (!alive || storageKey !== key) return;
      if (saved?.admission) {
        attempt = saved;
        failure = null;
        closeDialog();
        onRefresh?.();
        return;
      }
      failure = problem;
      if (removed) {
        attempt = null;
        plan = null;
        typedName = "";
        onRefresh?.();
      }
    } finally {
      delete inFlight[key];
    }
  }
</script>

<Panel title="Cluster actions" note="Requests return an operation; acceptance is not completion.">
  <div class="controls">
    <button type="button" disabled={!canCreate || previewLoading} onclick={() => begin("start")}>Start</button>
    <button type="button" disabled={!canCreate || previewLoading} onclick={() => begin("stop")}>Stop</button>
    <button type="button" class="danger" disabled={blocked || !current || (!canCreate && !(unresolved && attempt?.action === "delete"))} onclick={() => openPlan("delete")}>Delete…</button>
    {#if cluster?.spec.engine === "redis" || (unresolved && (attempt?.action === "scale_up" || attempt?.action === "scale_down"))}
      <button type="button" disabled={!canAdd || previewLoading} onclick={() => begin("scale_up")}>{residualReplica ? "Retry replica add" : "Add replica"}</button>
      <button type="button" class="danger" disabled={blocked || !current || (!canRemove && !(unresolved && attempt?.action === "scale_down"))} onclick={() => openPlan("scale_down")}>Remove replica…</button>
    {/if}
    {#if standalonePostgres || (unresolved && attempt?.action === "recover-setup")}
      <button type="button" disabled={blocked || !current || (!canRecover && !(unresolved && attempt?.action === "recover-setup"))} onclick={() => openPlan("recover-setup")}>Recover setup…</button>
    {/if}
  </div>
  {#if standalonePostgres}
    <p class="scale-note">Standalone PostgreSQL runs one node. Use the patroni topology for replicas and automatic failover. Setup recovery verifies native failure before it runs.</p>
  {/if}
  {#if cluster?.spec.engine === "redis"}
    <p class="scale-note">Desired replicas: {cluster.desired_replicas}. The supported range is zero to one with RDB, AOF, or combined persistence. Scaling requires a running desired state. This mode has no automatic failover.</p>
  {/if}
  {#if current && attempt?.admission}
    <div class="result" role="status">
      <strong>Operation accepted · {attempt.action}</strong>
      <p>This confirms admission only. Open the operation to inspect progress and completion.</p>
      <a class="mono" href={path("operation", attempt.admission.operation_id)} onclick={(event) => follow(event, () => onOpenOperation(attempt!.admission!.operation_id))}>Open operation {attempt.admission.operation_id} ↗</a>
    </div>
  {:else if unresolved && attempt}
    <div class="result" role="status">
      <strong>{sending ? "Awaiting admission" : "Outcome uncertain"} · {attempt.action}</strong>
      <p class="mono">{attempt.targetName} · {attempt.targetID} · revision {attempt.expectedRevision}</p>
      <p>{sending ? "The request is in flight. Closing this view does not cancel it." : "The request may have been accepted. Retry only this saved request with the same key and body."}</p>
      {#if !sending}
        <button type="button" disabled={blocked} onclick={() => dispatch(attempt!, storageKey, true)}>Retry same request</button>
      {/if}
    </div>
  {/if}
  {#if current && failure && !dialog}<ErrorPanel view={errorView(failure)} />{/if}
  {#if blocked}
    <div class="result"><a href={path("operations")}>Inspect Operations ↗</a></div>
  {/if}
</Panel>

{#if dialog && current}
  <Dialog label={`Confirm ${reviews[dialogAction].label}`} title={`${reviews[dialogAction].label.toUpperCase()} — REVIEW VALIDATED PLAN`} placement="centre" danger onClose={closeDialog}>
    {#if previewLoading}<p role="status">Loading {reviews[dialogAction].label} preview…</p>{/if}
    {#if failure}<ErrorPanel view={errorView(failure)} />{/if}
    {#if shownPlan}
      <section>
        <h3 class="eyebrow">Exact target</h3>
        <dl class="mono">
          <dt>cluster</dt><dd>{shownPlan.cluster_name}</dd>
          <dt>cluster_id</dt><dd>{shownPlan.cluster_id}</dd>
          <dt>action</dt><dd>{shownPlan.action}</dd>
          {#if "member_id" in shownPlan}<dt>member_id</dt><dd>{shownPlan.member_id}</dd>{/if}
          {#if "resource_id" in shownPlan}<dt>resource_id</dt><dd>{shownPlan.resource_id}</dd>{/if}
          <dt>expected_revision</dt><dd>{shownPlan.expected_revision}</dd>
          <dt>expected_downtime</dt><dd>{shownPlan.downtime}</dd>
          <dt>data_disposition</dt><dd>{shownPlan.data_disposition}</dd>
          <dt>plan_token</dt><dd>{shownPlan.plan_token}</dd>
          <dt>expires_at</dt><dd>{shownPlan.expires_at}</dd>
        </dl>
      </section>
      <section class="retention">
        <h3 class="eyebrow">Retained volumes</h3>
        {#if shownPlan.retained_volumes.length}
          <ul class="mono">{#each shownPlan.retained_volumes as volume}<li>{volume}</li>{/each}</ul>
        {:else}<p>No recorded volumes.</p>{/if}
        <p>{shownPlan.action === "delete" ? "Deletion stops cluster service." : shownPlan.action === "recover_setup" ? "Recovery checks the existing setup and restarts the member." : "Removal stops the named replica. The primary remains."} These volumes are retained. This action does not purge data.</p>
      </section>
      {#if unresolved && attempt}
        <p>The original confirmation is saved. A retry uses this exact plan, revision, and request key, even after the preview expires.</p>
        <button class="destructive" type="button" disabled={sending || blocked} onclick={() => dispatch(attempt!, storageKey, true)}>{sending ? "Awaiting admission…" : `Retry same ${dialogAction === "delete" ? "delete" : reviews[dialogAction].label} request`}</button>
      {:else}
        <label for="delete-cluster-name">Type <span class="mono">{shownPlan.cluster_name}</span> to confirm this plan.</label>
        <input id="delete-cluster-name" aria-label="Cluster name confirmation" autocomplete="off" bind:value={typedName} />
        <button class="destructive" type="button" disabled={!canCreate || typedName !== shownPlan.cluster_name} onclick={() => begin(dialogAction)}>{reviews[dialogAction].confirm}</button>
      {/if}
    {:else if !previewLoading}
      <p>Fetch a fresh preview before confirming this action. Confirm the cluster name again after a refused or expired plan.</p>
      <button type="button" disabled={!canCreate} onclick={() => preview(dialogAction)}>Fetch new preview</button>
    {/if}
  </Dialog>
{/if}

<style>
  .scale-note { padding: 0 14px 12px; }
  .controls { display: flex; gap: 8px; flex-wrap: wrap; padding: 12px 14px; }
  button { padding: 7px 12px; }
  .danger { border-color: var(--danger-line); color: var(--danger-ink); }
  .result { border-top: 1px solid var(--line); padding: 12px 14px; overflow-wrap: anywhere; }
  .result strong { font-size: 12px; font-weight: 500; }
  p { font-size: 12px; line-height: 1.6; color: var(--ink-body); margin: 8px 0; }
  a { font-size: 12px; }
  h3 { margin: 0 0 10px; font-weight: 400; text-transform: uppercase; }
  dl { display: grid; grid-template-columns: auto 1fr; gap: 6px 14px; font-size: 11px; margin: 0; }
  dt { color: var(--ink-dim); }
  dd { margin: 0; overflow-wrap: anywhere; }
  .retention { border: 1px solid var(--danger-line); padding: 14px; }
  ul { padding-left: 20px; font-size: 11px; overflow-wrap: anywhere; }
  label { font-size: 12px; color: var(--ink-body); }
  input { width: 100%; box-sizing: border-box; }
</style>
