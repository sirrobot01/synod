package postgres

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPostgresNativeReplicationPolicy(t *testing.T) {
	for _, synchronous := range []bool{false, true} {
		t.Run(map[bool]string{false: "asynchronous", true: "synchronous"}[synchronous], func(t *testing.T) {
			primary := bootstrapContainerWithPolicy(t, "", synchronous)
			writer := primary.connect(t)
			expected := ""
			if synchronous {
				expected = `ANY 1 ("synod_` + primary.command[6] + `")`
			}
			var actual, commit string
			if err := writer.QueryRow(t.Context(), `SELECT current_setting('synchronous_standby_names'),current_setting('synchronous_commit')`).Scan(&actual, &commit); err != nil || actual != expected || commit != "on" {
				t.Fatalf("policy: %q %q %v", actual, commit, err)
			}
			// Seed the test table locally before a standby exists. Application commits below use the declared default.
			if _, err := writer.Exec(t.Context(), `SET synchronous_commit=local`); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Exec(t.Context(), `CREATE TABLE policy_rows(value integer); INSERT INTO policy_rows VALUES(137)`); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Exec(t.Context(), `SET synchronous_commit=on`); err != nil {
				t.Fatal(err)
			}
			if !synchronous {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				if _, err := writer.Exec(ctx, `INSERT INTO policy_rows VALUES(138)`); err != nil {
					t.Fatalf("asynchronous write required a replica: %v", err)
				}
				primary.docker(t, "restart", primary.name)
				primary.waitPhase(t, "ready")
				after := primary.connect(t)
				var sum int
				if err := after.QueryRow(t.Context(), `SELECT sum(value)::int FROM policy_rows`).Scan(&sum); err != nil || sum != 275 {
					t.Fatalf("async restart: %d %v", sum, err)
				}
				return
			}
			source := ReplicaSource{Address: strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primary.name)), Port: 5432, ResourceID: primary.command[7]}
			replica := replicaContainer(t, primary, source)
			program, err := CloneReplicaCommand(replica.config, source, replica.command[5], replica.command[6], replica.command[7], uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			cloneCtx, cancelClone := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancelClone()
			args := append([]string{"--host", replica.host, "exec", "--user", "postgres", replica.name}, program...)
			if output, err := exec.CommandContext(cloneCtx, "docker", args...).CombinedOutput(); err != nil {
				t.Fatalf("synchronous clone: %v (%d bytes)", err, len(output))
			}
			replica.waitPhase(t, "cloned")
			replica.docker(t, "restart", replica.name)
			replica.waitPhase(t, "ready")
			observer := primary.connect(t)
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			if _, err := writer.Exec(ctx, `INSERT INTO policy_rows VALUES(138)`); err != nil {
				t.Fatalf("synchronous commit with connected standby: %v", err)
			}
			replica.docker(t, "stop", replica.name)
			done := make(chan error, 1)
			writerPID := writer.PgConn().PID()
			go func() { _, err := writer.Exec(ctx, `INSERT INTO policy_rows VALUES(139)`); done <- err }()
			waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
			defer cancelWait()
			for {
				var waiting bool
				if err := observer.QueryRow(waitCtx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='SyncRep')`, writerPID).Scan(&waiting); err != nil {
					t.Fatalf("commit did not wait for WAL acknowledgement: %v", err)
				}
				if waiting {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("commit returned without a standby: %v", err)
				default:
				}
				time.Sleep(25 * time.Millisecond)
			}
			// The pending commit must still wait until the standby returns.
			select {
			case err := <-done:
				t.Fatalf("missing standby lost synchronous policy: %v", err)
			default:
			}
			replica.docker(t, "start", replica.name)
			replica.waitPhase(t, "ready")
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("commit did not resume after standby reconnect")
			}
			reader := replica.connect(t)
			deadline := time.Now().Add(10 * time.Second)
			for {
				var sum int
				err := reader.QueryRow(t.Context(), `SELECT sum(value)::int FROM policy_rows`).Scan(&sum)
				if err == nil && sum == 414 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("acknowledged data missing on replica: %d %v", sum, err)
				}
				time.Sleep(25 * time.Millisecond)
			}
			if err := reader.QueryRow(t.Context(), `SELECT current_setting('synchronous_standby_names'),current_setting('synchronous_commit')`).Scan(&actual, &commit); err != nil || actual != expected || commit != "on" {
				t.Fatalf("replica lost declared policy: %q %q %v", actual, commit, err)
			}
			primary.docker(t, "restart", primary.name)
			primary.waitPhase(t, "ready")
			after := primary.connect(t)
			if err := after.QueryRow(t.Context(), `SELECT current_setting('synchronous_standby_names'),current_setting('synchronous_commit')`).Scan(&actual, &commit); err != nil || actual != expected || commit != "on" {
				t.Fatalf("restart lost policy: %q %q %v", actual, commit, err)
			}
		})
	}
}
