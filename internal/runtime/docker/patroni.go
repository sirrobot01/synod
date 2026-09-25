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
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

const labelPatroniSource = "io.synod.patroni-source-hash"
const labelRouteSource = "io.synod.route-source-hash"
const patroniTmpfs = "rw,noexec,nosuid,nodev,size=4m"

func (r *Runtime) preparePatroni(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	return r.settleCreate(ctx, command, r.createPatroni(ctx, command, command.Input.GetEnsureContainer(), secrets))
}

// prepareRoute creates a pooler or router container beside a Patroni member.
func (r *Runtime) prepareRoute(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	return r.settleCreate(ctx, command, r.createRoute(ctx, command, command.Input.GetEnsureContainer(), secrets))
}

// settleCreate reports a protected create. A clean refusal changed nothing. Otherwise
// recovery reads the container and settles a lost reply.
func (r *Runtime) settleCreate(ctx context.Context, command *agentv1.Command, mutationErr error) (runtime.Result, error) {
	if mutationErr != nil && !errors.Is(mutationErr, runtime.ErrOutcomeUnknown) {
		return runtime.Result{}, mutationErr
	}
	inspectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.InspectionTimeout)
	defer cancel()
	// Recovery proves the protected files, so a lost reply is settled by what it reads.
	result, err := r.Recover(inspectCtx, command, nil)
	if err != nil {
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: errors.Join(err, mutationErr)}
	}
	return result, nil
}

// createPatroni creates the canonical member container with p and its protected files.
// An existing container must already hold p. A failure before the create changes nothing.
// A failure after it reports outcome unknown.
func (r *Runtime) createPatroni(ctx context.Context, command *agentv1.Command, p *agentv1.EnsureContainer, secrets runtime.Secrets) error {
	settings, err := postgres.PatroniMemberSettings(p, command.Epoch.InstallationId, command.Epoch.ClusterId, command.Input.ResourceId)
	if err != nil || len(secrets) != 8 {
		return runtime.ErrConfigurationConflict
	}
	seen := make(map[string]bool)
	for _, purpose := range []string{"superuser", "application", "replication", "patroni_rewind", "patroni_dcs", "patroni_rest"} {
		value := secrets[purpose]
		if len(value) < 32 || len(value) > 256 || seen[string(value)] || slices.ContainsFunc(value, func(b byte) bool { return b < 33 || b > 126 }) {
			return runtime.ErrConfigurationConflict
		}
		seen[string(value)] = true
	}
	member, err := decodeTLSMaterial(secrets["patroni_member_tls"])
	if err != nil {
		return err
	}
	dcs, err := decodeTLSMaterial(secrets["patroni_dcs_tls"])
	if err != nil {
		return err
	}
	verifier, err := postgresVerifier(secrets["application"], p.Postgres.ApplicationCredentialId, p.Postgres.ApplicationCredentialVersion, "application")
	if err != nil {
		return err
	}
	files, err := postgres.PatroniFiles(settings, postgres.PatroniSecrets{
		Superuser: string(secrets["superuser"]), Replication: string(secrets["replication"]), Rewind: string(secrets["patroni_rewind"]),
		DCS: string(secrets["patroni_dcs"]), REST: string(secrets["patroni_rest"]), ApplicationVerifier: verifier,
	}, member, dcs, time.Now())
	if err != nil {
		return runtime.ErrConfigurationConflict
	}
	return r.createProtected(ctx, command, p, patroniSource, files)
}

// decodeTLSMaterial reads one sealed certificate credential.
func decodeTLSMaterial(value []byte) (postgres.PatroniTLSMaterial, error) {
	material, err := postgres.DecodePatroniTLSMaterial(value)
	if err != nil {
		return material, runtime.ErrConfigurationConflict
	}
	return material, nil
}

// createRoute renders the pooler or router files and creates the container. The pooler gets
// the application password, so its server login does not depend on any client's SCRAM keys.
func (r *Runtime) createRoute(ctx context.Context, command *agentv1.Command, p *agentv1.EnsureContainer, secrets runtime.Secrets) error {
	pooler, router, purposes, err := postgres.RouteSettings(p.Route)
	if err != nil || len(secrets) != len(purposes) {
		return runtime.ErrConfigurationConflict
	}
	member, err := decodeTLSMaterial(secrets["patroni_member_tls"])
	if err != nil {
		return err
	}
	if router != nil {
		files, err := postgres.RouterFiles(*router, member)
		if err != nil {
			return runtime.ErrConfigurationConflict
		}
		return r.createProtected(ctx, command, p, routerSource, files)
	}
	files, err := postgres.PoolerFiles(*pooler, string(secrets["application"]), member)
	if err != nil {
		return runtime.ErrConfigurationConflict
	}
	return r.createProtected(ctx, command, p, poolerSource, files)
}

// routeSource is the protected file set of a route container.
func routeSource(route *agentv1.PostgresRouteConfig) protectedSource {
	if route.Pooler != nil {
		return poolerSource
	}
	return routerSource
}

// protectedSource names the files that a container restores from /etc at each start.
type protectedSource struct {
	label, directory string
	names            []string
}

var (
	patroniSource = protectedSource{labelPatroniSource, "synod-patroni", []string{"config.json", "ca.crt", "member.crt", "member.key", "dcs-ca.crt", "dcs-client.crt", "dcs-client.key"}}
	poolerSource  = protectedSource{labelRouteSource, "synod-pooler", []string{"pgbouncer.ini", "userlist.txt", "ca.crt", "member.crt", "member.key", "role.json"}}
	routerSource  = protectedSource{labelRouteSource, "synod-router", []string{"haproxy.cfg", "ca.crt", "check.pem"}}
)

// createProtected creates the canonical container with p and copies its protected files into
// /etc. A label pins their digest. An existing container must already hold p and the same files.
// A failure before the create changes nothing. A failure after it reports outcome unknown.
func (r *Runtime) createProtected(ctx context.Context, command *agentv1.Command, p *agentv1.EnsureContainer, source protectedSource, files map[string][]byte) error {
	if len(files) != len(source.names) {
		return runtime.ErrConfigurationConflict
	}
	for _, name := range source.names {
		if len(files[name]) == 0 {
			return runtime.ErrConfigurationConflict
		}
	}
	encoded, err := json.Marshal(files)
	if err != nil {
		return runtime.ErrConfigurationConflict
	}
	digest := sha256.Sum256(encoded)
	options, err := r.containerOptions(ctx, command, p, true)
	if err != nil {
		return err
	}
	options.Config.Labels[source.label] = hex.EncodeToString(digest[:])
	existing, err := r.api.ContainerInspect(ctx, options.Name, client.ContainerInspectOptions{})
	if err == nil {
		if err := r.checkContainer(ctx, command, existing.Container, p); err != nil {
			return err
		}
		if existing.Container.Config.Labels[source.label] != options.Config.Labels[source.label] {
			return runtime.ErrConfigurationConflict
		}
		// Existing sources are inspection-only, including incomplete prior copies.
		return nil
	}
	if !errdefs.IsNotFound(err) {
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: source.directory + "/", Typeflag: tar.TypeDir, Mode: 0o700}); err != nil {
		return runtime.ErrConfigurationConflict
	}
	for _, name := range source.names {
		data := files[name]
		if err := writer.WriteHeader(&tar.Header{Name: source.directory + "/" + name, Mode: 0o600, Size: int64(len(data))}); err != nil {
			return runtime.ErrConfigurationConflict
		}
		if _, err := writer.Write(data); err != nil {
			return runtime.ErrConfigurationConflict
		}
	}
	if err := writer.Close(); err != nil {
		return runtime.ErrConfigurationConflict
	}
	created, mutationErr := r.api.ContainerCreate(ctx, options)
	if mutationErr == nil {
		_, mutationErr = r.api.CopyToContainer(ctx, created.ID, client.CopyToContainerOptions{DestinationPath: "/etc", Content: &archive, CopyUIDGID: true})
	}
	inspectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.InspectionTimeout)
	defer cancel()
	current, err := r.api.ContainerInspect(inspectCtx, options.Name, client.ContainerInspectOptions{})
	if err != nil || mutationErr != nil || current.Container.Config == nil || current.Container.Config.Labels[source.label] != options.Config.Labels[source.label] {
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: errors.Join(err, mutationErr)}
	}
	return nil
}

// inspectPatroniSource proves the protected Patroni file set without returning secret material to a caller.
func (r *Runtime) inspectPatroniSource(ctx context.Context, item container.InspectResponse) error {
	return r.inspectSource(ctx, item, patroniSource)
}

// inspectSource proves a protected file set against its digest label.
func (r *Runtime) inspectSource(ctx context.Context, item container.InspectResponse, source protectedSource) error {
	if item.Config == nil {
		return runtime.ErrConfigurationConflict
	}
	expected, err := hex.DecodeString(item.Config.Labels[source.label])
	if err != nil || len(expected) != sha256.Size || hex.EncodeToString(expected) != item.Config.Labels[source.label] {
		return runtime.ErrConfigurationConflict
	}
	copied, err := r.api.CopyFromContainer(ctx, item.ID, client.CopyFromContainerOptions{SourcePath: "/etc/" + source.directory})
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
	files, directory := make(map[string][]byte, len(source.names)), false
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || header.Uid != 0 || header.Gid != 0 || header.Linkname != "" {
			return runtime.ErrConfigurationConflict
		}
		if header.Name == source.directory+"/" || header.Name == source.directory {
			if directory || header.Typeflag != tar.TypeDir || header.Mode != 0o700 {
				return runtime.ErrConfigurationConflict
			}
			directory = true
			continue
		}
		name, ok := strings.CutPrefix(header.Name, source.directory+"/")
		if !ok || !slices.Contains(source.names, name) || files[name] != nil || header.Typeflag != tar.TypeReg || header.Mode != 0o600 || header.Size < 1 || header.Size > 128<<10 {
			return runtime.ErrConfigurationConflict
		}
		data, err := io.ReadAll(reader)
		if err != nil || int64(len(data)) != header.Size {
			return runtime.ErrOutcomeUnknown
		}
		files[name] = data
	}
	if !directory || len(files) != len(source.names) {
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
