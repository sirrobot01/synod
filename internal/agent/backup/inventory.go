package backup

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

var errInventory = errors.New("database inventory failed")

// The scripts use the same local administrator credentials as the exports.
// Each output line is a database name, a tab, and its table or key count.
var inventoryScripts = map[string]string{
	"postgres": `if [ -n "${POSTGRES_PASSWORD_FILE:-}" ]; then PGPASSWORD=$(cat "$POSTGRES_PASSWORD_FILE"); else PGPASSWORD=${POSTGRES_PASSWORD:-${PGPASSWORD:-}}; fi
export PGPASSWORD
q() { psql --no-password --host=/var/run/postgresql --port="${PGPORT:-5432}" --username="${POSTGRES_USER:-postgres}" --no-align --tuples-only --set=ON_ERROR_STOP=1 "$@"; }
q --dbname=postgres --command="SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY 1" | while IFS= read -r database; do
 count=$(q --dbname="$database" --command="SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r', 'p') AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%'")
 printf '%s\t%s\n' "$database" "$count"
done`,
	"mysql": `password_file=${MARIADB_ROOT_PASSWORD_FILE:-${MYSQL_ROOT_PASSWORD_FILE:-}}
if [ -n "$password_file" ]; then MYSQL_PWD=$(cat "$password_file"); else MYSQL_PWD=${MARIADB_ROOT_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}; fi
export MYSQL_PWD
tool=$(command -v mariadb || command -v mysql)
exec "$tool" --no-defaults --protocol=socket --user=root --batch --skip-column-names --execute="SELECT s.schema_name, (SELECT count(*) FROM information_schema.tables t WHERE t.table_schema = s.schema_name) FROM information_schema.schemata s WHERE s.schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') ORDER BY 1"`,
	"redis": `redis-cli -e --no-auth-warning -h 127.0.0.1 -p "$SYNOD_REDIS_PORT" --raw INFO keyspace | awk -F '[:=,]' '/^db[0-9]+:/ { print $1 "\t" $3 }'`,
}

// DatabaseInventory lists the databases of a running database container with their
// table counts, or the key counts of each Redis logical database.
func DatabaseInventory(ctx context.Context, api *client.Client, c container.InspectResponse, database string) (map[string]int64, error) {
	script := inventoryScripts[database]
	if database == "mariadb" {
		script = inventoryScripts["mysql"]
	}
	if script == "" || c.Config == nil {
		return nil, errInventory
	}
	var env []string
	if database == "redis" {
		port := "6379"
		for i, arg := range c.Config.Cmd[:max(0, len(c.Config.Cmd)-1)] {
			switch arg {
			case "--requirepass":
				env = append(env, "REDISCLI_AUTH="+c.Config.Cmd[i+1])
			case "--port":
				port = c.Config.Cmd[i+1]
			}
		}
		env = append(env, "SYNOD_REDIS_PORT="+port)
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	created, err := api.ExecCreate(ctx, c.ID, client.ExecCreateOptions{Cmd: []string{"sh", "-ec", script}, Env: env, AttachStdout: true, AttachStderr: true})
	if err != nil {
		return nil, errInventory
	}
	attached, err := api.ExecAttach(ctx, created.ID, client.ExecAttachOptions{})
	if err != nil {
		return nil, errInventory
	}
	defer attached.Close()
	stop := context.AfterFunc(ctx, attached.Close)
	defer stop()
	// Tool errors can contain credentials. Discard them.
	var output bytes.Buffer
	if _, err := stdcopy.StdCopy(&limitedWriter{&output, 1 << 20}, io.Discard, attached.Reader); err != nil {
		return nil, errInventory
	}
	// The exec can report running for a moment after its output ends.
	for deadline := time.Now().Add(2 * time.Second); ; {
		status, err := api.ExecInspect(ctx, created.ID, client.ExecInspectOptions{})
		if err != nil || (!status.Running && status.ExitCode != 0) || (status.Running && time.Now().After(deadline)) {
			return nil, errInventory
		}
		if !status.Running {
			break
		}
		select {
		case <-ctx.Done():
			return nil, errInventory
		case <-time.After(20 * time.Millisecond):
		}
	}
	inventory := make(map[string]int64)
	scanner := bufio.NewScanner(&output)
	for scanner.Scan() {
		name, value, found := strings.Cut(scanner.Text(), "\t")
		count, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if !found || name == "" || err != nil || count < 0 || len(inventory) >= 1024 {
			return nil, errInventory
		}
		inventory[name] = count
	}
	return inventory, nil
}

type limitedWriter struct {
	w     io.Writer
	limit int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if len(p) > l.limit {
		return 0, errInventory
	}
	l.limit -= len(p)
	return l.w.Write(p)
}
