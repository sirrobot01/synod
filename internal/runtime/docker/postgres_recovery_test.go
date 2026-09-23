package docker

import (
	"errors"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func failedPostgresRecoveryFixture(t *testing.T, change string) (*Runtime, *client.Client, *agentv1.Command, runtime.Secrets) {
	t.Helper()
	r, api, clusterID := runtimeFixture(t)
	secrets := runtime.Secrets{"superuser": []byte(uuid.NewString()), "application": []byte(uuid.NewString()), "replication": []byte(uuid.NewString())}
	ensure := postgresStorageFixture(t, r, api, clusterID, secrets)
	id, name := ensure.Input.ResourceId, "synod-instance-"+ensure.Input.ResourceId
	start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
	startPostgresFixture(t, r, start)
	storageExec(t, api, name, "psql", "-h", postgres.ConfigDirectory, "-U", "synod_admin", "-d", "app", "-v", "ON_ERROR_STOP=1", "-c", "CREATE TABLE kept(value integer); INSERT INTO kept VALUES(119)")
	if change != "" {
		storageExec(t, api, name, "psql", "-h", postgres.ConfigDirectory, "-U", "synod_admin", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", change)
	}
	storageExec(t, api, name, "sh", "-c", "printf 'failed\\n' > "+postgres.BootstrapFailurePath)
	stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatal(err)
	}
	start = runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	result, err := r.Execute(t.Context(), start, nil)
	deadline := time.Now().Add(30 * time.Second)
	for errors.Is(err, runtime.ErrOutcomeUnknown) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		result, err = r.Recover(t.Context(), start, result.Effects)
	}
	if !errors.Is(err, runtime.ErrConfigurationConflict) || result.Effects[0].State != "bootstrap_failed" {
		t.Fatalf("fixture did not confirm setup failure: %+v %v", result, err)
	}
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Secrets: ensure.Input.Secrets, Payload: &agentv1.CommandInput_RecoverPostgresSetup{RecoverPostgresSetup: &agentv1.RecoverPostgresSetup{Postgres: ensure.Input.GetEnsureContainer().Postgres}}})
	return r, api, command, secrets
}

func TestTypedPostgresRecoverySurvivesLostReplyAndJournalRestart(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "lost_start_reply"}[lost], func(t *testing.T) {
			r, api, command, secrets := failedPostgresRecoveryFixture(t, "")
			var creates, starts atomic.Int32
			hooked, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
				if response.Request.Method == http.MethodPost && strings.HasSuffix(response.Request.URL.Path, "/exec") {
					creates.Add(1)
				}
				if response.Request.Method == http.MethodPost && strings.Contains(response.Request.URL.Path, "/exec/") && strings.HasSuffix(response.Request.URL.Path, "/start") {
					starts.Add(1)
					if lost {
						response.Body.Close()
						response.StatusCode = http.StatusInternalServerError
						response.Body = io.NopCloser(strings.NewReader(`{"message":"lost start reply fixture"}`))
						response.Header.Set("Content-Type", "application/json")
					}
				}
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer hooked.Close()
			driver, err := New(hooked, r.options)
			if err != nil {
				t.Fatal(err)
			}
			j, options, location := runtimeJournal(t, command)
			result, err := driver.Execute(t.Context(), command, secrets)
			if err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) {
				t.Fatalf("execute: %v", err)
			}
			if err := j.Observe(command.CommandId, result.Effects); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			options.AllowCreate = false
			reopened, err := journal.Open(location, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			record, err := reopened.Get(command.CommandId)
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(30 * time.Second)
			for {
				result, err = driver.Recover(t.Context(), command, record.Effects)
				if err == nil && !slices.ContainsFunc(result.Effects, func(e *agentv1.Effect) bool { return e.Active || e.OutcomeUnknown }) {
					break
				}
				if err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) {
					t.Fatalf("recover: %+v %v", result, err)
				}
				if time.Now().After(deadline) {
					t.Fatalf("recovery did not settle: %+v %v", result, err)
				}
				time.Sleep(50 * time.Millisecond)
			}
			if creates.Load() != 1 || starts.Load() != 1 {
				t.Fatalf("recovery replayed mutation: creates=%d starts=%d", creates.Load(), starts.Load())
			}
			step := &agentv1.PlanStep{Command: command, Postcondition: agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED}
			terminal := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: result.Effects}
			if err := operation.CheckStepResult(step, terminal); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Observe(command.CommandId, result.Effects); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Complete(terminal); err != nil {
				t.Fatal(err)
			}
			// A new observer can prove the outcome even if no exec ID reached the journal.
			deadline = time.Now().Add(10 * time.Second)
			for {
				recovered, err := r.Recover(t.Context(), command, nil)
				if err == nil && recovered.Effects[0].State == "setup_recovered" {
					break
				}
				if !errors.Is(err, runtime.ErrOutcomeUnknown) || time.Now().After(deadline) {
					t.Fatalf("native receipt did not survive lost effect ID: %+v %v", recovered, err)
				}
				time.Sleep(25 * time.Millisecond)
			}
			name := "synod-instance-" + command.Input.ResourceId
			clusterID := uuid.MustParse(command.Epoch.ClusterId)
			stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: command.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
			if _, err := r.Execute(t.Context(), stop, nil); err != nil {
				t.Fatal(err)
			}
			start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: command.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
			startPostgresFixture(t, r, start)
			if got := storageExec(t, api, name, "psql", "-h", postgres.ConfigDirectory, "-U", "synod_admin", "-d", "app", "-At", "-c", "SELECT value FROM kept"); got != "119" {
				t.Fatal("recovery lost application data")
			}
		})
	}
}

func TestTypedPostgresRecoveryRefusalSurvivesLostEffectID(t *testing.T) {
	for _, change := range []string{"DROP DATABASE app", "ALTER ROLE synod_app CREATEDB"} {
		t.Run(change, func(t *testing.T) {
			r, _, command, secrets := failedPostgresRecoveryFixture(t, change)
			result, err := r.Execute(t.Context(), command, secrets)
			deadline := time.Now().Add(30 * time.Second)
			for errors.Is(err, runtime.ErrOutcomeUnknown) && time.Now().Before(deadline) {
				time.Sleep(50 * time.Millisecond)
				result, err = r.Recover(t.Context(), command, nil)
			}
			if !errors.Is(err, runtime.ErrConfigurationConflict) || len(result.Effects) == 0 || result.Effects[0].State != "setup_refused" || result.Effects[0].Active || result.Effects[0].OutcomeUnknown {
				t.Fatalf("refusal was not durable: %+v %v", result, err)
			}
		})
	}
}

func TestTypedPostgresRecoveryLostCreateReplyRemainsUnknown(t *testing.T) {
	r, _, command, secrets := failedPostgresRecoveryFixture(t, "")
	var creates, starts atomic.Int32
	hooked, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if response.Request.Method == http.MethodPost && strings.HasSuffix(response.Request.URL.Path, "/exec") {
			creates.Add(1)
			response.Body.Close()
			response.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
		}
		if response.Request.Method == http.MethodPost && strings.Contains(response.Request.URL.Path, "/exec/") {
			starts.Add(1)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer hooked.Close()
	driver, err := New(hooked, r.options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := driver.Execute(t.Context(), command, secrets)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatalf("lost create reply settled without proof: %+v %v", result, err)
	}
	for range 2 {
		result, err = driver.Recover(t.Context(), command, result.Effects)
		if !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].Active || !result.Effects[0].OutcomeUnknown {
			t.Fatalf("missing receipt proved nonexecution: %+v %v", result, err)
		}
	}
	if creates.Load() != 1 || starts.Load() != 0 {
		t.Fatal("read-only recovery created or started an exec")
	}
}

func TestTypedPostgresRecoveryRefusesConfigurationMismatch(t *testing.T) {
	r, _, command, secrets := failedPostgresRecoveryFixture(t, "")
	command.Input.GetRecoverPostgresSetup().Postgres.Port++
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	if result, err := r.Execute(t.Context(), command, secrets); !errors.Is(err, runtime.ErrConfigurationConflict) || len(result.Effects) != 0 {
		t.Fatalf("mismatched configuration executed: %+v %v", result, err)
	}
}

func TestTypedPostgresRecoveryReceiptReadFailureStaysUnknown(t *testing.T) {
	r, _, command, secrets := failedPostgresRecoveryFixture(t, "")
	result, err := r.Execute(t.Context(), command, secrets)
	deadline := time.Now().Add(30 * time.Second)
	for errors.Is(err, runtime.ErrOutcomeUnknown) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		result, err = r.Recover(t.Context(), command, result.Effects)
	}
	if err != nil {
		t.Fatalf("recovery did not complete: %+v %v", result, err)
	}
	var reads atomic.Int32
	hooked, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if response.Request.Method == http.MethodGet && response.Request.URL.Query().Get("path") == postgres.InitializationDirectory+"/recovery-"+command.CommandId {
			reads.Add(1)
			response.Body.Close()
			response.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer hooked.Close()
	observer, err := New(hooked, r.options)
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for reads.Load() == 0 {
		result, err = observer.Recover(t.Context(), command, nil)
		if !errors.Is(err, runtime.ErrOutcomeUnknown) || len(result.Effects) == 0 || !result.Effects[0].Active || !result.Effects[0].OutcomeUnknown {
			t.Fatalf("failed receipt read proved completion: %+v %v", result, err)
		}
		if time.Now().After(deadline) {
			t.Fatal("receipt inspection did not run")
		}
		time.Sleep(25 * time.Millisecond)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		result, err = r.Recover(t.Context(), command, result.Effects)
		if err == nil {
			break
		}
		if !errors.Is(err, runtime.ErrOutcomeUnknown) || time.Now().After(deadline) {
			t.Fatalf("receipt did not resolve after inspection recovered: %+v %v", result, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestTypedPostgresRecoveryWaitDoesNotIgnoreNativeWork(t *testing.T) {
	r, api, command, secrets := failedPostgresRecoveryFixture(t, "")
	native, err := api.ExecCreate(t.Context(), "synod-instance-"+command.Input.ResourceId, client.ExecCreateOptions{Cmd: []string{"sleep", "10"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ExecStart(t.Context(), native.ID, client.ExecStartOptions{Detach: true}); err != nil {
		t.Fatal(err)
	}
	r.options.InspectionTimeout = time.Second
	if result, err := r.Execute(t.Context(), command, secrets); !errors.Is(err, runtime.ErrBusy) || len(result.Effects) != 0 {
		t.Fatalf("recovery ignored active native work: %+v %v", result, err)
	}
}
