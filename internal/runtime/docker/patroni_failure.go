package docker

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"strconv"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// verifyPatroniStartFailure proves a stopped member's failed first start. It names one of three
// states: storage never started, a storage attempt that never completed, or completed initial
// data that Patroni moved aside.
// It uses Docker API reads only. It does not start, stop, exec, write, or remove anything.
func (r *Runtime) verifyPatroniStartFailure(ctx context.Context, command *agentv1.Command, result runtime.Result) (runtime.Result, error) {
	p := command.Input.GetVerifyPatroniStartFailure()
	inspected, err := r.api.ContainerInspect(ctx, "synod-instance-"+command.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	item := inspected.Container
	if err := r.checkContainer(ctx, command, item, p.Container); err != nil {
		return result, err
	}
	result = r.containerResult(command, item)
	if item.State == nil || item.State.Running || item.State.Restarting || len(item.ExecIDs) != 0 {
		result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
		return result, runtime.ErrOutcomeUnknown
	}
	// A member that never started cannot have recorded an attempt.
	if item.State.Status == container.StateCreated {
		return result, runtime.ErrConfigurationConflict
	}
	if err := r.inspectPatroniSource(ctx, item); err != nil {
		result.Effects[0].State = "patroni_configuration_unknown"
		result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	expected := patroniIdentity{Installation: command.Epoch.InstallationId, Cluster: command.Epoch.ClusterId, Resource: command.Input.ResourceId, Volume: p.Container.VolumeId, Provision: p.Container.Postgres.Patroni.ProvisionId, Format: 1}
	var outside, inside patroniIdentity
	outsidePresent, outsideEmpty, err := r.patroniOptionalRecord(ctx, item.ID, "/var/lib/synod-patroni/identity.json", &outside)
	if err != nil {
		return result, err
	}
	// Without the first guard record, the member never passed the empty-storage checks.
	if !outsidePresent {
		return result, r.missingPatroniStorage(ctx, item.ID)
	}
	insidePresent, insideEmpty, err := r.patroniOptionalRecord(ctx, item.ID, postgres.DataDirectory+"/.synod-patroni-identity.json", &inside)
	if err != nil {
		return result, err
	}
	if !outsideEmpty && outside != expected || insidePresent && !insideEmpty && inside != expected {
		return result, runtime.ErrConfigurationConflict
	}
	pair := !outsideEmpty && insidePresent && !insideEmpty
	var attempt struct {
		Identity patroniIdentity `json:"identity"`
		Method   string          `json:"method"`
	}
	attemptPresent, attemptEmpty, err := r.patroniOptionalRecord(ctx, item.ID, "/var/lib/synod-patroni/attempt.json", &attempt)
	if err != nil {
		return result, err
	}
	if attemptPresent && !attemptEmpty && (attempt.Identity != expected || attempt.Method != "init" && attempt.Method != "clone") {
		return result, runtime.ErrConfigurationConflict
	}
	var present [3]bool
	for index, filename := range []string{"/var/lib/synod-patroni/complete.json", postgres.DataDirectory + "/.synod-patroni-complete.json", "/var/lib/synod-patroni/application-attempt.json"} {
		_, err := r.api.ContainerStatPath(ctx, item.ID, client.ContainerStatPathOptions{Path: filename})
		if err != nil && !errdefs.IsNotFound(err) {
			return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}
		present[index] = err == nil
	}
	var state string
	switch {
	case attemptPresent && !attemptEmpty && pair:
		switch {
		case present == [3]bool{}:
			state = "patroni_storage_attempt_failed"
		case present[0] && present[1] && attempt.Method == "init":
			if err := r.checkPatroniCancelledBootstrap(ctx, command, item.ID, expected, present[2]); err != nil {
				return result, err
			}
			state = "patroni_bootstrap_cancelled"
		default:
			// A partial pair, a completed clone, or an application record without completion.
			return result, runtime.ErrConfigurationConflict
		}
	case present != [3]bool{}:
		return result, runtime.ErrConfigurationConflict
	case attemptPresent && attemptEmpty && pair:
		// The guard stopped inside its attempt write, before the staging directory.
		state = "patroni_storage_attempt_failed"
	case !attemptPresent && (pair || !outsideEmpty || !insidePresent):
		// The guard stopped between its identity writes and an attempt. Records are written outside first.
		state = "patroni_storage_not_started"
	default:
		return result, runtime.ErrConfigurationConflict
	}
	if state == "patroni_storage_not_started" || attemptEmpty {
		for _, filename := range []string{postgres.PatroniDataDirectory, postgres.DataDirectory + "/.synod-patroni-stage"} {
			_, err := r.api.ContainerStatPath(ctx, item.ID, client.ContainerStatPathOptions{Path: filename})
			if err == nil {
				return result, runtime.ErrConfigurationConflict
			}
			if !errdefs.IsNotFound(err) {
				return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
			}
		}
	}
	// An absent path is also reported for a replaced container. Require the same stopped container.
	current, err := r.api.ContainerInspect(ctx, item.ID, client.ContainerInspectOptions{})
	if err != nil {
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	now := current.Container
	if now.ID != item.ID || now.State == nil || now.State.Running || now.State.Restarting || len(now.ExecIDs) != 0 ||
		now.State.StartedAt != item.State.StartedAt || now.State.FinishedAt != item.State.FinishedAt || now.RestartCount != item.RestartCount {
		result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
		return result, runtime.ErrOutcomeUnknown
	}
	result.Effects[0].State, result.Effects[0].Active, result.Effects[0].OutcomeUnknown = state, false, false
	return result, nil
}

// patroniOptionalRecord reads a guard record as absent, empty, or valid.
// The guard creates each record exclusively and then writes it, so a kill can leave it empty.
func (r *Runtime) patroniOptionalRecord(ctx context.Context, containerID, filename string, target any) (present, empty bool, err error) {
	found, err := r.api.ContainerStatPath(ctx, containerID, client.ContainerStatPathOptions{Path: filename})
	if errdefs.IsNotFound(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if found.Stat.Size != 0 {
		return true, false, r.patroniRecord(ctx, containerID, filename, target)
	}
	if !found.Stat.Mode.IsRegular() || found.Stat.Mode.Perm() != 0600 || found.Stat.LinkTarget != "" {
		return true, false, runtime.ErrConfigurationConflict
	}
	return true, true, nil
}

// checkPatroniCancelledBootstrap proves Patroni moved completed initial data to patroni.failed.
// Patroni also uses that rename when a data removal fails, so this alone does not prove the
// member never served. The caller must also know that its startup was never proved.
func (r *Runtime) checkPatroniCancelledBootstrap(ctx context.Context, command *agentv1.Command, containerID string, expected patroniIdentity, application bool) error {
	var outside, inside patroniCompletion
	if err := r.patroniRecord(ctx, containerID, "/var/lib/synod-patroni/complete.json", &outside); err != nil {
		return err
	}
	if err := r.patroniRecord(ctx, containerID, postgres.DataDirectory+"/.synod-patroni-complete.json", &inside); err != nil {
		return err
	}
	systemID, err := strconv.ParseUint(outside.SystemID, 10, 64)
	if err != nil || systemID == 0 || strconv.FormatUint(systemID, 10) != outside.SystemID || outside.Identity != expected || outside != inside {
		return runtime.ErrConfigurationConflict
	}
	// The guard keeps its staging directory after completion, so only the data directory must be gone.
	_, err = r.api.ContainerStatPath(ctx, containerID, client.ContainerStatPathOptions{Path: postgres.PatroniDataDirectory})
	if err == nil {
		return runtime.ErrConfigurationConflict
	}
	if !errdefs.IsNotFound(err) {
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	failed := postgres.PatroniDataDirectory + ".failed"
	moved, err := r.api.ContainerStatPath(ctx, containerID, client.ContainerStatPathOptions{Path: failed})
	if errdefs.IsNotFound(err) {
		return runtime.ErrConfigurationConflict
	}
	if err != nil {
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if !moved.Stat.Mode.IsDir() || moved.Stat.LinkTarget != "" {
		return runtime.ErrConfigurationConflict
	}
	version, err := r.containerRecord(ctx, containerID, failed+"/PG_VERSION")
	if errdefs.IsNotFound(err) {
		return runtime.ErrConfigurationConflict
	}
	if err != nil {
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if strings.TrimSpace(version) != "17" {
		return runtime.ErrConfigurationConflict
	}
	control, err := r.api.CopyFromContainer(ctx, containerID, client.CopyFromContainerOptions{SourcePath: failed + "/global/pg_control"})
	if errdefs.IsNotFound(err) {
		return runtime.ErrConfigurationConflict
	}
	if err != nil {
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	defer control.Content.Close()
	// PostgreSQL writes pg_control as one 8192-byte block that starts with the system identifier.
	if !control.Stat.Mode.IsRegular() || control.Stat.LinkTarget != "" || control.Stat.Size != 8192 {
		return runtime.ErrConfigurationConflict
	}
	reader := tar.NewReader(io.LimitReader(control.Content, 16<<10))
	header, err := reader.Next()
	if err != nil || header.Typeflag != tar.TypeReg || header.Name != "pg_control" || header.Size != 8192 {
		return runtime.ErrOutcomeUnknown
	}
	var identifier [8]byte
	if _, err := io.ReadFull(reader, identifier[:]); err != nil {
		return runtime.ErrOutcomeUnknown
	}
	if binary.NativeEndian.Uint64(identifier[:]) != systemID {
		return runtime.ErrConfigurationConflict
	}
	if !application {
		return nil
	}
	var receipt struct {
		Format              int    `json:"format"`
		InstallationID      string `json:"installation_id"`
		ClusterID           string `json:"cluster_id"`
		CredentialID        string `json:"application_credential_id"`
		CredentialVersion   int64  `json:"application_credential_version"`
		VerifierFingerprint string `json:"verifier_fingerprint"`
		SystemID            string `json:"system_id"`
	}
	// The hook runs no SQL before its record is written, so an empty record needs no content check.
	_, empty, err := r.patroniOptionalRecord(ctx, containerID, "/var/lib/synod-patroni/application-attempt.json", &receipt)
	if err != nil || empty {
		return err
	}
	stored := command.Input.GetVerifyPatroniStartFailure().Container.Postgres
	fingerprint, err := hex.DecodeString(receipt.VerifierFingerprint)
	if receipt.Format != 1 || receipt.InstallationID != command.Epoch.InstallationId || receipt.ClusterID != command.Epoch.ClusterId ||
		receipt.CredentialID != stored.ApplicationCredentialId || receipt.CredentialVersion != stored.ApplicationCredentialVersion ||
		receipt.SystemID != outside.SystemID || err != nil || len(fingerprint) != sha256.Size || hex.EncodeToString(fingerprint) != receipt.VerifierFingerprint {
		return runtime.ErrConfigurationConflict
	}
	return nil
}
