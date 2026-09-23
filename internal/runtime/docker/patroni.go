package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/runtime"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

const labelPatroniSource = "io.synod.patroni-source-hash"
const patroniTmpfs = "rw,noexec,nosuid,nodev,size=4m"

func (r *Runtime) preparePatroni(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	p := command.Input.GetEnsureContainer()
	settings, err := postgres.PatroniMemberSettings(p, command.Epoch.InstallationId, command.Epoch.ClusterId, command.Input.ResourceId)
	if err != nil || len(secrets) != 8 {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	seen := make(map[string]bool)
	for _, purpose := range []string{"superuser", "application", "replication", "patroni_rewind", "patroni_dcs", "patroni_rest"} {
		value := secrets[purpose]
		if len(value) < 32 || len(value) > 256 || seen[string(value)] || slices.ContainsFunc(value, func(b byte) bool { return b < 33 || b > 126 }) {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
		seen[string(value)] = true
	}
	var member, dcs postgres.PatroniTLSMaterial
	for _, target := range []struct {
		purpose  string
		material *postgres.PatroniTLSMaterial
	}{{"patroni_member_tls", &member}, {"patroni_dcs_tls", &dcs}} {
		value := secrets[target.purpose]
		if len(value) == 0 || len(value) > secret.MaxPlaintextBytes {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(target.material); err != nil {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
	}
	verifier, err := postgresVerifier(secrets["application"], p.Postgres.ApplicationCredentialId, p.Postgres.ApplicationCredentialVersion, "application")
	if err != nil {
		return runtime.Result{}, err
	}
	files, err := postgres.PatroniFiles(settings, postgres.PatroniSecrets{
		Superuser: string(secrets["superuser"]), Replication: string(secrets["replication"]), Rewind: string(secrets["patroni_rewind"]),
		DCS: string(secrets["patroni_dcs"]), REST: string(secrets["patroni_rest"]), ApplicationVerifier: verifier,
	}, member, dcs, time.Now())
	if err != nil {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	encoded, err := json.Marshal(files)
	if err != nil {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	digest := sha256.Sum256(encoded)
	options, err := r.containerOptions(ctx, command, p, true)
	if err != nil {
		return runtime.Result{}, err
	}
	options.Config.Labels[labelPatroniSource] = hex.EncodeToString(digest[:])
	existing, err := r.api.ContainerInspect(ctx, options.Name, client.ContainerInspectOptions{})
	if err == nil {
		if err := r.checkContainer(ctx, command, existing.Container, p); err != nil {
			return runtime.Result{}, err
		}
		if existing.Container.Config.Labels[labelPatroniSource] != options.Config.Labels[labelPatroniSource] {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
		// Existing sources are inspection-only, including incomplete prior copies.
		return r.Recover(ctx, command, nil)
	}
	if !errdefs.IsNotFound(err) {
		return runtime.Result{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "synod-patroni/", Typeflag: tar.TypeDir, Mode: 0o700}); err != nil {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	for _, name := range []string{"config.json", "ca.crt", "member.crt", "member.key", "dcs-ca.crt", "dcs-client.crt", "dcs-client.key"} {
		data := files[name]
		if err := writer.WriteHeader(&tar.Header{Name: "synod-patroni/" + name, Mode: 0o600, Size: int64(len(data))}); err != nil {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
		if _, err := writer.Write(data); err != nil {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
	}
	if err := writer.Close(); err != nil {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	created, mutationErr := r.api.ContainerCreate(ctx, options)
	if mutationErr == nil {
		_, mutationErr = r.api.CopyToContainer(ctx, created.ID, client.CopyToContainerOptions{DestinationPath: "/etc", Content: &archive, CopyUIDGID: true})
	}
	inspectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.InspectionTimeout)
	defer cancel()
	result, err := r.Recover(inspectCtx, command, nil)
	if err != nil {
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: errors.Join(err, mutationErr)}
	}
	current, err := r.api.ContainerInspect(inspectCtx, options.Name, client.ContainerInspectOptions{})
	if err != nil || current.Container.Config == nil || current.Container.Config.Labels[labelPatroniSource] != options.Config.Labels[labelPatroniSource] {
		for _, effect := range result.Effects {
			effect.Active, effect.OutcomeUnknown = true, true
		}
		return result, runtime.ErrOutcomeUnknown
	}
	return result, nil
}

// inspectPatroniSource proves the protected file set without returning secret material to a caller.
func (r *Runtime) inspectPatroniSource(ctx context.Context, item container.InspectResponse) error {
	if item.Config == nil {
		return runtime.ErrConfigurationConflict
	}
	expected, err := hex.DecodeString(item.Config.Labels[labelPatroniSource])
	if err != nil || len(expected) != sha256.Size || hex.EncodeToString(expected) != item.Config.Labels[labelPatroniSource] {
		return runtime.ErrConfigurationConflict
	}
	copied, err := r.api.CopyFromContainer(ctx, item.ID, client.CopyFromContainerOptions{SourcePath: postgres.PatroniSourceDirectory})
	if err != nil {
		return runtime.ErrOutcomeUnknown
	}
	defer copied.Content.Close()
	if !copied.Stat.Mode.IsDir() || copied.Stat.Mode.Perm() != 0o700 || copied.Stat.LinkTarget != "" {
		return runtime.ErrConfigurationConflict
	}
	encoded, err := io.ReadAll(io.LimitReader(copied.Content, (1<<20)+1))
	if err != nil || len(encoded) > 1<<20 {
		return runtime.ErrOutcomeUnknown
	}
	reader := tar.NewReader(bytes.NewReader(encoded))
	files, directory := make(map[string][]byte, 7), false
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || header.Uid != 0 || header.Gid != 0 || header.Linkname != "" {
			return runtime.ErrConfigurationConflict
		}
		if header.Name == "synod-patroni/" || header.Name == "synod-patroni" {
			if directory || header.Typeflag != tar.TypeDir || header.Mode != 0o700 {
				return runtime.ErrConfigurationConflict
			}
			directory = true
			continue
		}
		name, ok := strings.CutPrefix(header.Name, "synod-patroni/")
		if !ok || !slices.Contains([]string{"config.json", "ca.crt", "member.crt", "member.key", "dcs-ca.crt", "dcs-client.crt", "dcs-client.key"}, name) || files[name] != nil || header.Typeflag != tar.TypeReg || header.Mode != 0o600 || header.Size < 1 || header.Size > 128<<10 {
			return runtime.ErrConfigurationConflict
		}
		data, err := io.ReadAll(reader)
		if err != nil || int64(len(data)) != header.Size {
			return runtime.ErrOutcomeUnknown
		}
		files[name] = data
	}
	if !directory || len(files) != 7 {
		return runtime.ErrOutcomeUnknown
	}
	canonical, err := json.Marshal(files)
	if err != nil {
		return runtime.ErrConfigurationConflict
	}
	found := sha256.Sum256(canonical)
	if !bytes.Equal(found[:], expected) {
		return runtime.ErrConfigurationConflict
	}
	return nil
}
