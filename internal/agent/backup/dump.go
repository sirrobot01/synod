package backup

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"time"

	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/snapshot"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/repository"
)

var errDump = errors.New("database dump failed; check that the database is running and its native tool and local administrator credentials are available")

// stageDumps finishes native exports before any filesystem capture starts.
// Only encrypted repository objects reach disk. Publication belongs to the final app snapshot.
func stageDumps(ctx context.Context, r *repository.Repository, dumps []kfs.Entry, source snapshot.SourceInfo) ([]kfs.Entry, error) {
	prepared, err := r.UploadFiles(ctx, virtualfs.NewStaticDirectory("databases", dumps), source, false)
	if err != nil {
		return nil, err
	}
	root, err := r.PreparedEntry(ctx, prepared, ".")
	if err != nil {
		return nil, err
	}
	defer root.Close()
	directory, ok := root.(kfs.Directory)
	if !ok {
		return nil, repository.ErrSnapshotIncomplete
	}
	var entries []kfs.Entry
	err = kfs.IterateEntries(ctx, directory, func(_ context.Context, entry kfs.Entry) error {
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		for _, entry := range entries {
			entry.Close()
		}
		return nil, err
	}
	return entries, nil
}

// dumpFile starts its command only when Kopia reads it. No plaintext dump reaches disk.
type dumpFile struct {
	kfs.StreamingFile
	api         *client.Client
	containerID string
	command     string
	signal      string
	env         []string
	failed      atomic.Bool
}

func (*dumpFile) Mode() os.FileMode { return 0600 }

func newDump(api *client.Client, c container.InspectResponse, database string) (*dumpFile, backup.DatabaseDump, error) {
	d := &dumpFile{api: api, containerID: c.ID, signal: "TERM"}
	format := "sql"
	switch database {
	case "postgres":
		d.signal = "INT"
		d.command = `if [ -n "${POSTGRES_PASSWORD_FILE:-}" ]; then PGPASSWORD=$(cat "$POSTGRES_PASSWORD_FILE"); else PGPASSWORD=${POSTGRES_PASSWORD:-${PGPASSWORD:-}}; fi
export PGPASSWORD
exec pg_dumpall --no-password --host=/var/run/postgresql --port="${PGPORT:-5432}" --username="${POSTGRES_USER:-postgres}"`
	case "mysql", "mariadb":
		tool := "mysqldump"
		extra := " --set-gtid-purged=OFF"
		if database == "mariadb" {
			tool, extra = "mariadb-dump", ""
		}
		d.command = `password_file=${MARIADB_ROOT_PASSWORD_FILE:-${MYSQL_ROOT_PASSWORD_FILE:-}}
if [ -n "$password_file" ]; then MYSQL_PWD=$(cat "$password_file"); else MYSQL_PWD=${MARIADB_ROOT_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}; fi
export MYSQL_PWD
exec ` + tool + ` --no-defaults --protocol=socket --user=root --all-databases --single-transaction --quick --routines --events --triggers --hex-blob --flush-privileges` + extra + ` --skip-lock-tables`
	case "redis":
		format = "rdb"
		d.command = `if [ -n "${REDIS_PASSWORD_FILE:-}" ]; then REDISCLI_AUTH=$(cat "$REDIS_PASSWORD_FILE"); export REDISCLI_AUTH; elif [ -n "${REDIS_PASSWORD:-}" ]; then REDISCLI_AUTH=$REDIS_PASSWORD; export REDISCLI_AUTH; fi
while :; do
 persistence=$(redis-cli -e -h 127.0.0.1 -p "$SYNOD_REDIS_PORT" --raw INFO persistence)
 if printf '%s\n' "$persistence" | grep -q '^rdb_bgsave_in_progress:0'; then break; fi
 sleep 1
done
exec redis-cli -h 127.0.0.1 -p "$SYNOD_REDIS_PORT" --rdb -`
		port := "6379"
		for i, arg := range c.Config.Cmd {
			if i+1 >= len(c.Config.Cmd) {
				continue
			}
			switch arg {
			case "--requirepass":
				d.env = append(d.env, "REDISCLI_AUTH="+c.Config.Cmd[i+1])
			case "--port":
				port = c.Config.Cmd[i+1]
			}
		}
		d.env = append(d.env, "SYNOD_REDIS_PORT="+port)
	default:
		return nil, backup.DatabaseDump{}, errDump
	}
	name := c.ID + "." + format
	d.StreamingFile = virtualfs.StreamingFileFromReader(name, nil)
	return d, backup.DatabaseDump{ContainerID: c.ID, Database: database, Archive: "databases/" + name, Format: format}, nil
}

// The stdin watcher ends the command when the agent closes its connection.
// The independent deadline also ends stalled reads. The dump runs in the foreground
// so the shell does not make it ignore SIGINT. Signal only this exec process group.
// SIGINT lets pg_dumpall wait for pg_dump. Reap the watcher before exit too.
// PostgreSQL as PID 1 treats a killed orphan as a server crash.
const dumpSupervisor = `exec 3<&0
command -v timeout >/dev/null 2>&1 || exit 1
read -r pid name state parent group rest < /proc/$$/stat
[ "$group" = "$$" ] || exit 1
trap ':' INT TERM
timeout -s USR1 "$3" sh -c '
trap "kill -$2 -$1 2>/dev/null || :; exit 0" USR1
read -r unused || :
kill "-$2" "-$1" 2>/dev/null || :
' synod-dump-watch "$$" "$2" <&3 &
watcher=$!
trap 'kill "$watcher" 2>/dev/null || :; wait "$watcher" 2>/dev/null || :' EXIT
sh -ec "$1" </dev/null 3<&-`

func (d *dumpFile) GetReader(ctx context.Context) (io.ReadCloser, error) {
	fail := func() (io.ReadCloser, error) { d.failed.Store(true); return nil, errDump }
	created, err := d.api.ExecCreate(ctx, d.containerID, client.ExecCreateOptions{Cmd: []string{"sh", "-c", dumpSupervisor, "synod-dump", d.command, d.signal, "1800"}, Env: d.env, AttachStdin: true, AttachStdout: true, AttachStderr: true})
	if err != nil {
		return fail()
	}
	attached, err := d.api.ExecAttach(ctx, created.ID, client.ExecAttachOptions{})
	if err != nil {
		return fail()
	}
	reader, writer := io.Pipe()
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		d.failed.Store(true)
		attached.Close()
		writer.CloseWithError(errDump)
	})
	go func() {
		defer close(done)
		defer attached.Close()
		defer stop()
		// Tool errors can contain credentials or data. Only stdout enters the repository.
		var count int64
		var err error
		for {
			var header [8]byte
			if _, err = io.ReadFull(attached.Reader, header[:]); err != nil {
				if errors.Is(err, io.EOF) {
					err = nil
				}
				break
			}
			var output io.Writer = writer
			switch header[0] {
			case 1:
			case 2:
				output = io.Discard
			default:
				err = errDump
			}
			if err != nil {
				break
			}
			var n int64
			n, err = io.CopyN(output, attached.Reader, int64(binary.BigEndian.Uint32(header[4:])))
			if header[0] == 1 {
				count += n
			}
			if err != nil {
				break
			}
		}
		if err == nil && ctx.Err() == nil && count > 0 {
			deadline := time.Now().Add(2 * time.Second)
			for {
				status, inspectErr := d.api.ExecInspect(ctx, created.ID, client.ExecInspectOptions{})
				if inspectErr != nil || !status.Running || time.Now().After(deadline) {
					if inspectErr != nil || status.Running || status.ExitCode != 0 {
						err = errDump
					}
					break
				}
				select {
				case <-ctx.Done():
					err = errDump
				case <-time.After(10 * time.Millisecond):
				}
				if err != nil {
					break
				}
			}
		} else {
			err = errDump
		}
		if err != nil {
			d.failed.Store(true)
			err = errDump
		}
		writer.CloseWithError(err)
	}()
	return &dumpReader{PipeReader: reader, closeStream: attached.Close, done: done}, nil
}

type dumpReader struct {
	*io.PipeReader
	closeStream func()
	done        <-chan struct{}
}

func (r *dumpReader) Close() error {
	r.closeStream()
	err := r.PipeReader.Close()
	<-r.done
	return err
}
