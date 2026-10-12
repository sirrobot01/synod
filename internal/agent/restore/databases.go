package restore

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"maps"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/repository"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func prepareDatabases(ctx context.Context, api *client.Client, r *repository.Repository, p *plan, operationID uuid.UUID) error {
	seen := make(map[string]bool)
	for _, dump := range p.Snapshot.Dumps {
		index := slices.IndexFunc(p.Containers, func(c container.InspectResponse) bool { return c.ID == dump.ContainerID })
		if index < 0 || seen[dump.ContainerID] || !slices.Contains([]string{"postgres", "mysql", "mariadb", "redis"}, dump.Database) || (dump.Database == "redis" && dump.Format != "rdb") || (dump.Database != "redis" && dump.Format != "sql") || dump.Archive != "databases/"+dump.ContainerID+"."+dump.Format {
			return &failure{Code: "snapshot_invalid"}
		}
		seen[dump.ContainerID] = true
		files, err := databaseFiles(p.Snapshot, dump.ContainerID)
		if err != nil {
			return err
		}
		if files {
			continue
		}
		if err := loadDatabase(ctx, api, r, p, operationID, p.Containers[index], dump); err != nil {
			return &failure{Code: "database_restore_failed", Resource: p.Definition.Services[p.Services[dump.ContainerID]].ContainerName, Cause: err}
		}
	}
	for _, c := range p.Snapshot.App.Definition.Containers {
		if c.Database != "" && !seen[c.Id] {
			return &failure{Code: "snapshot_invalid"}
		}
	}
	return nil
}

// databaseFiles reports whether the snapshot holds the database data directories of the
// container. The data restore then writes them, and the export is not loaded. The files
// are usable only from one filesystem snapshot of the whole app.
func databaseFiles(saved backup.Snapshot, containerID string) (bool, error) {
	copied, missing := 0, 0
	for _, item := range saved.App.Definition.Paths {
		if item.ContainerId != containerID || item.Status != "Dumped" {
			continue
		}
		if slices.ContainsFunc(saved.Paths, func(path backup.Path) bool { return path.Source == item.Source }) {
			copied++
		} else {
			missing++
		}
	}
	if copied == 0 {
		return false, nil
	}
	if missing > 0 || saved.Consistency != backup.ConsistencyFilesystem {
		return false, &failure{Code: "snapshot_invalid"}
	}
	return true, nil
}

// loadDatabase imports into isolated data. It never publishes a port or mounts an excluded host path.
func loadDatabase(ctx context.Context, api *client.Client, r *repository.Repository, p *plan, operationID uuid.UUID, original container.InspectResponse, dump backup.DatabaseDump) (result error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	service := p.Definition.Services[p.Services[original.ID]]
	env := make(map[string]string)
	for _, variable := range service.Environment {
		key, value, _ := strings.Cut(variable, "=")
		env[key] = value
	}
	dataPath := ""
	for _, candidate := range p.Snapshot.App.Definition.Paths {
		if candidate.ContainerId == original.ID && candidate.Status == "Dumped" && (dataPath == "" || len(candidate.Target) < len(dataPath)) {
			dataPath = candidate.Target
		}
	}
	if dataPath == "" || !path.IsAbs(dataPath) || path.Clean(dataPath) != dataPath {
		return &failure{Code: "definition_unsupported"}
	}
	var mounts []mount.Mount
	dataMount := ""
	for _, m := range service.Volumes {
		originalPath := slices.IndexFunc(p.Snapshot.App.Definition.Paths, func(item *agentv1.DiscoveredPath) bool {
			return item.ContainerId == original.ID && item.Target == m.Target && item.Status != "Never"
		})
		if originalPath < 0 || m.Target == "/docker-entrypoint-initdb.d" || strings.HasPrefix(m.Target, "/docker-entrypoint-initdb.d/") {
			continue
		}
		item := mount.Mount{Type: mount.Type(m.Type), Source: m.Source, Target: m.Target, ReadOnly: m.ReadOnly}
		if m.Type == "volume" {
			item.Source = p.Definition.Volumes[m.Source].Name
			item.VolumeOptions = &mount.VolumeOptions{NoCopy: true}
			if m.Volume != nil {
				item.VolumeOptions.Subpath = m.Volume.Subpath
			}
		}
		if dataPath == m.Target || strings.HasPrefix(dataPath, strings.TrimSuffix(m.Target, "/")+"/") {
			if m.ReadOnly {
				return &failure{Code: "definition_unsupported"}
			}
			if len(m.Target) > len(dataMount) {
				dataMount = m.Target
			}
		}
		mounts = append(mounts, item)
	}
	if dataMount == "" {
		return &failure{Code: "definition_unsupported"}
	}
	// pg_dumpall omits CREATE ROLE for the user that ran it, so the load initializes that same user.
	originalUser := env["POSTGRES_USER"]
	if originalUser == "" {
		originalUser = "postgres"
	}
	password := uuid.NewString()
	command := slices.Clone(service.Command)
	workingDir := service.WorkingDir
	switch dump.Database {
	case "postgres":
		delete(env, "POSTGRES_USER_FILE")
		delete(env, "POSTGRES_PASSWORD_FILE")
		delete(env, "POSTGRES_DB_FILE")
		env["POSTGRES_USER"], env["POSTGRES_DB"], env["POSTGRES_PASSWORD"] = originalUser, "postgres", password
	case "mysql", "mariadb":
		for key := range env {
			if strings.HasPrefix(key, "MYSQL_") || strings.HasPrefix(key, "MARIADB_") {
				delete(env, key)
			}
		}
		env["MYSQL_ROOT_PASSWORD"], env["MARIADB_ROOT_PASSWORD"] = password, password
	case "redis":
		if len(command) > 1 && !strings.HasPrefix(command[1], "--") {
			return &failure{Code: "definition_unsupported"}
		}
		if slices.Contains(command, "--include") {
			return &failure{Code: "definition_unsupported"}
		}
		command = append(command, "--appendonly", "no", "--save", "")
		workingDir = dataPath
	}
	variables := make([]string, 0, len(env))
	for _, key := range slices.Sorted(maps.Keys(env)) {
		variables = append(variables, key+"="+env[key])
	}
	config := &container.Config{Image: service.Image, Env: variables, Cmd: command, Entrypoint: service.Entrypoint, WorkingDir: workingDir, User: service.User, Labels: map[string]string{operationLabel: operationID.String(), "io.synod.restore.role": "database", "com.docker.compose.project": ""}}
	created, err := api.ContainerCreate(ctx, client.ContainerCreateOptions{Config: config, HostConfig: &container.HostConfig{NetworkMode: "none", Mounts: mounts, Tmpfs: map[string]string{"/docker-entrypoint-initdb.d": "rw,noexec,nosuid,nodev,size=1m"}}})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := api.ContainerRemove(cleanup, created.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
		result = errors.Join(result, err)
	}()
	var empty bytes.Buffer
	archive := tar.NewWriter(&empty)
	relative := strings.TrimPrefix(strings.TrimPrefix(dataPath, dataMount), "/")
	if relative == "" {
		relative = "."
	}
	if err := archive.WriteHeader(&tar.Header{Name: relative, Typeflag: tar.TypeDir, Mode: 0700}); err != nil {
		return err
	}
	if err := archive.Close(); err != nil {
		return err
	}
	if _, err := api.CopyToContainer(ctx, created.ID, client.CopyToContainerOptions{DestinationPath: dataMount, Content: &empty}); err != nil {
		return err
	}
	redisPort, redisPassword, redisFile, appendOnly := "6379", env["REDIS_PASSWORD"], "dump.rdb", false
	if dump.Database == "redis" {
		for i, arg := range service.Command {
			if i+1 >= len(service.Command) {
				continue
			}
			switch arg {
			case "--port":
				redisPort = service.Command[i+1]
			case "--requirepass":
				redisPassword = service.Command[i+1]
			case "--dbfilename":
				redisFile = service.Command[i+1]
			case "--appendonly":
				appendOnly = service.Command[i+1] == "yes"
			}
		}
		if redisFile == "." || redisFile == ".." || strings.ContainsAny(redisFile, "/\\\x00") || redisFile == "" {
			return &failure{Code: "definition_unsupported"}
		}
		if err := copyArchive(ctx, api, r, created.ID, dataPath, p.Snapshot.ID, dump.Archive, redisFile, false); err != nil {
			return err
		}
	}
	if _, err := api.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		return err
	}
	var ready, load []string
	var clientEnv []string
	switch dump.Database {
	case "postgres":
		port := env["PGPORT"]
		if port == "" {
			port = "5432"
		}
		ready = []string{"psql", "--no-password", "--host=/var/run/postgresql", "--port=" + port, "--username=" + originalUser, "--dbname=postgres", "--set=ON_ERROR_STOP=1", "--command=SELECT 1"}
		// The role section always creates the dump user, which the initialized server already has.
		// Remove only that first statement so that every other error still stops the load.
		load = []string{"sh", "-ec", `awk -v a="CREATE ROLE $1;" -v b="CREATE ROLE \"$1\";" '!done && ($0 == a || $0 == b) { done = 1; next } { print }' /tmp/synod.sql > /tmp/synod.load
rm /tmp/synod.sql
exec psql --no-password --host=/var/run/postgresql --port="$2" --username="$1" --dbname=postgres --set=ON_ERROR_STOP=1 --file=/tmp/synod.load`, "synod-load", originalUser, port}
		clientEnv = []string{"PGPASSWORD=" + password}
	case "mysql", "mariadb":
		tool := "mysql"
		if dump.Database == "mariadb" {
			tool = "mariadb"
		}
		ready = []string{tool, "--no-defaults", "--protocol=socket", "--user=root", "--execute=SELECT 1"}
		load = []string{"sh", "-ec", "exec " + tool + " --no-defaults --protocol=socket --user=root < /tmp/synod.sql"}
		clientEnv = []string{"MYSQL_PWD=" + password}
	case "redis":
		ready = []string{"redis-cli", "-e", "--no-auth-warning", "-h", "127.0.0.1", "-p", redisPort, "PING"}
		clientEnv = []string{"REDISCLI_AUTH=" + redisPassword}
	}
	for deadline := time.Now().Add(2 * time.Minute); ; {
		// Entry points can start a short-lived initializer. Wait for the final server.
		process := map[string]string{"postgres": "postgres", "mysql": "mysqld", "mariadb": "mariadbd", "redis": "redis-server"}[dump.Database]
		err := execCommand(ctx, api, created.ID, []string{"sh", "-ec", `[ "$(cat /proc/1/comm)" = "$1" ]`, "synod-ready", process}, nil, nil)
		if err == nil {
			err = execCommand(ctx, api, created.ID, ready, clientEnv, nil)
		}
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	if dump.Database != "redis" {
		if err := copyArchive(ctx, api, r, created.ID, "/tmp", p.Snapshot.ID, dump.Archive, "synod.sql", true); err != nil {
			return err
		}
		if err := execCommand(ctx, api, created.ID, load, clientEnv, nil); err != nil {
			return err
		}
	}
	if dump.Database == "redis" && appendOnly {
		base := slices.Clone(ready[:len(ready)-1])
		if err := execCommand(ctx, api, created.ID, append(slices.Clone(base), "CONFIG", "SET", "appendonly", "yes"), clientEnv, nil); err != nil {
			return err
		}
		for deadline := time.Now().Add(2 * time.Minute); ; {
			var output bytes.Buffer
			if err := execCommand(ctx, api, created.ID, append(slices.Clone(base), "INFO", "persistence"), clientEnv, &output); err != nil {
				return err
			}
			if strings.Contains(output.String(), "aof_rewrite_in_progress:0") && strings.Contains(output.String(), "aof_rewrite_scheduled:0") && strings.Contains(output.String(), "aof_last_bgrewrite_status:ok") {
				break
			}
			if time.Now().After(deadline) {
				return &failure{Code: "database_restore_failed"}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	_, err = api.ContainerStop(ctx, created.ID, client.ContainerStopOptions{Timeout: new(60)})
	return err
}
