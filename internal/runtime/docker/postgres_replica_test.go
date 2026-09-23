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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func postgresReplicaFixture(t *testing.T, wrongSource bool) (*Runtime, *client.Client, *agentv1.Command, *agentv1.Command, runtime.Secrets) {
	t.Helper()
	r, api, cluster := runtimeFixture(t)
	secrets := runtime.Secrets{"superuser": []byte(uuid.NewString()), "application": []byte(uuid.NewString()), "replication": []byte("escaped:\\" + uuid.NewString())}
	primary := postgresStorageFixture(t, r, api, cluster, secrets)
	start := runtimeCommand(t, r, cluster, &agentv1.CommandInput{ResourceId: primary.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
	startPostgresFixture(t, r, start)
	name := "synod-instance-" + primary.Input.ResourceId
	storageExec(t, api, name, "sh", "-c", "until pg_isready -h 127.0.0.1 -q; do sleep 0.1; done")
	storageExec(t, api, name, "psql", "-h", postgres.ConfigDirectory, "-U", "synod_admin", "-d", "app", "-v", "ON_ERROR_STOP=1", "-c", "CREATE TABLE cloned(value int); INSERT INTO cloned VALUES(124)")
	inspect, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	config := proto.Clone(primary.Input.GetEnsureContainer()).(*agentv1.EnsureContainer)
	address := inspect.Container.NetworkSettings.Networks["synod-network-"+config.NetworkId].IPAddress.String()
	config.Postgres.ReplicaSource = &agentv1.PostgresReplicaSource{Address: address, Port: 5432, ResourceId: primary.Input.ResourceId}
	if wrongSource {
		config.Postgres.ReplicaSource.ResourceId = uuid.NewString()
	}
	config.VolumeId = uuid.NewString()
	volume := runtimeCommand(t, r, cluster, &agentv1.CommandInput{ResourceId: config.VolumeId, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: config.VolumeId}}})
	if _, err := r.Execute(t.Context(), volume, nil); err != nil {
		t.Fatal(err)
	}
	config.SecretEnv[0] = &agentv1.SecretEnv{Name: "SYNOD_REPL_PASSWORD", Purpose: "replication"}
	delete(secrets, "superuser")
	ensure := runtimeCommand(t, r, cluster, &agentv1.CommandInput{ResourceId: uuid.NewString(), Secrets: primary.Input.Secrets[1:], Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: config}})
	if _, err := r.Execute(t.Context(), ensure, secrets); err != nil {
		t.Fatal(err)
	}
	for _, initialize := range []bool{false, true} {
		start := runtimeCommand(t, r, cluster, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: initialize}}})
		if _, err := r.Execute(t.Context(), start, nil); !errors.Is(err, runtime.ErrStorageLost) && !errors.Is(err, runtime.ErrConfigurationConflict) {
			t.Fatalf("ordinary start initialized replica: %v", err)
		}
	}
	prepare := runtimeCommand(t, r, cluster, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_PreparePostgresReplica{PreparePostgresReplica: &agentv1.PreparePostgresReplica{}}})
	result, err := r.Execute(t.Context(), prepare, nil)
	result, err = settleReplica(t, r, prepare, result, err)
	if err != nil || result.Effects[0].State != "replica_prepared" {
		t.Fatalf("prepare: %+v %v", result, err)
	}
	if err := operation.CheckStepEffects(&agentv1.PlanStep{Command: prepare, Postcondition: agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_PREPARED}, result.Effects); err != nil {
		t.Fatal(err)
	}
	clone := runtimeCommand(t, r, cluster, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Secrets: ensure.Input.Secrets, Payload: &agentv1.CommandInput_ClonePostgresReplica{ClonePostgresReplica: &agentv1.ClonePostgresReplica{RequiredFreeBytes: 1 << 30, Postgres: config.Postgres}}})
	return r, api, primary, clone, secrets
}

func settleReplica(t *testing.T, r *Runtime, command *agentv1.Command, result runtime.Result, err error) (runtime.Result, error) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for errors.Is(err, runtime.ErrOutcomeUnknown) || slices.ContainsFunc(result.Effects, func(e *agentv1.Effect) bool { return e.Active || e.OutcomeUnknown }) {
		if time.Now().After(deadline) {
			t.Fatalf("replica command did not settle: %+v %v", result, err)
		}
		time.Sleep(50 * time.Millisecond)
		result, err = r.Recover(t.Context(), command, result.Effects)
	}
	return result, err
}

func TestTypedPostgresCloneSurvivesLostReplyAndJournalRestart(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "lost_start_reply"}[lost], func(t *testing.T) {
			r, api, primary, command, secrets := postgresReplicaFixture(t, false)
			var mutations atomic.Int32
			hooked, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
				if response.Request.Method == http.MethodPost {
					mutations.Add(1)
				}
				if lost && response.Request.Method == http.MethodPost && strings.Contains(response.Request.URL.Path, "/exec/") && strings.HasSuffix(response.Request.URL.Path, "/start") {
					response.Body.Close()
					response.StatusCode = http.StatusInternalServerError
					response.Body = io.NopCloser(strings.NewReader(`{"message":"lost clone reply"}`))
					response.Header.Set("Content-Type", "application/json")
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
				t.Fatal(err)
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
			result, err = driver.Recover(t.Context(), record.Command, record.Effects)
			result, err = settleReplica(t, driver, command, result, err)
			if err != nil || result.Effects[0].State != "replica_cloned" {
				t.Fatalf("clone: %+v %v", result, err)
			}
			if mutations.Load() != 2 {
				t.Fatalf("recovery repeated mutation: %d", mutations.Load())
			}
			if err := operation.CheckStepEffects(&agentv1.PlanStep{Command: command, Postcondition: agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED}, result.Effects); err != nil {
				t.Fatal(err)
			}
			terminal := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: result.Effects}
			if err := reopened.Observe(command.CommandId, result.Effects); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Complete(terminal); err != nil {
				t.Fatal(err)
			}
			result, err = driver.Recover(t.Context(), command, nil)
			result, err = settleReplica(t, driver, command, result, err)
			if err != nil || result.Effects[0].State != "replica_cloned" {
				t.Fatalf("lost exec identity: %+v %v", result, err)
			}
			cluster := uuid.MustParse(command.Epoch.ClusterId)
			name := "synod-instance-" + command.Input.ResourceId
			inspect, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if slices.ContainsFunc(inspect.Container.Config.Env, func(s string) bool { return strings.HasPrefix(s, "POSTGRES_PASSWORD=") }) {
				t.Fatal("replica received admin password")
			}
			if detail, err := r.containerChanges(t.Context(), inspect.Container); err != nil || detail != "" {
				t.Fatalf("replica drift: %s %v", detail, err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				inventory, err := r.InspectCluster(t.Context(), cluster, nil)
				if err == nil && !slices.ContainsFunc(inventory.Effects, func(e *agentv1.Effect) bool { return e.Active || e.OutcomeUnknown }) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("quiet clone did not drain: %+v %v", inventory, err)
				}
				time.Sleep(50 * time.Millisecond)
			}
			start := runtimeCommand(t, r, cluster, &agentv1.CommandInput{ResourceId: command.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
			if _, err := r.Execute(t.Context(), start, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
				t.Fatalf("inert clone counted as running database: %v", err)
			}
			stop := runtimeCommand(t, r, cluster, &agentv1.CommandInput{ResourceId: command.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
			if _, err := r.Execute(t.Context(), stop, nil); err != nil {
				t.Fatal(err)
			}
			if result, err := r.Recover(t.Context(), command, nil); err != nil || result.Effects[0].State != "replica_cloned" {
				t.Fatalf("stopped clone proof: %+v %v", result, err)
			}
			startPostgresFixture(t, r, start)
			storageExec(t, api, name, "sh", "-c", "until pg_isready -h 127.0.0.1 -q; do sleep 0.1; done")
			got := storageExec(t, api, name, "psql", "-h", postgres.ConfigDirectory, "-U", "synod_admin", "-d", "app", "-At", "-c", "SELECT pg_is_in_recovery(),current_setting('transaction_read_only'),value FROM cloned")
			if got != "t|on|124" {
				t.Fatalf("replica data/role: %s", got)
			}
			storageExec(t, api, "synod-instance-"+primary.Input.ResourceId, "psql", "-h", postgres.ConfigDirectory, "-U", "synod_admin", "-d", "app", "-v", "ON_ERROR_STOP=1", "-c", "INSERT INTO cloned VALUES(125)")
			got = storageExec(t, api, name, "sh", "-c", "for i in $(seq 1 100); do n=$(psql -h /run/synod-postgres -U synod_admin -d app -At -c 'SELECT count(*) FROM cloned'); if [ \"$n\" = 2 ]; then echo streamed; exit 0; fi; sleep 0.1; done; exit 1")
			if got != "streamed" {
				t.Fatal("new data did not stream")
			}
		})
	}
}

func TestTypedPostgresCloneRejectsSourceAndCredentialMismatch(t *testing.T) {
	for _, wrongSource := range []bool{true, false} {
		t.Run(map[bool]string{true: "source_identity", false: "password"}[wrongSource], func(t *testing.T) {
			r, _, _, command, secrets := postgresReplicaFixture(t, wrongSource)
			if !wrongSource {
				secrets["replication"] = []byte("wrong-password")
			}
			result, err := r.Execute(t.Context(), command, secrets)
			result, err = settleReplica(t, r, command, result, err)
			if !errors.Is(err, runtime.ErrConfigurationConflict) || result.Effects[0].State != "replica_refused" {
				t.Fatalf("clone mismatch: %+v %v", result, err)
			}
			result, err = r.Recover(t.Context(), command, nil)
			result, err = settleReplica(t, r, command, result, err)
			if !errors.Is(err, runtime.ErrConfigurationConflict) || result.Effects[0].Active || result.Effects[0].OutcomeUnknown {
				t.Fatalf("refusal not durable: %+v %v", result, err)
			}
		})
	}
}

func TestTypedPostgresCloneMissingExecCreateReplyRetainsClaim(t *testing.T) {
	r, _, _, command, secrets := postgresReplicaFixture(t, false)
	var mutations atomic.Int32
	hooked, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if response.Request.Method == http.MethodPost {
			mutations.Add(1)
		}
		if response.Request.Method == http.MethodPost && strings.HasSuffix(response.Request.URL.Path, "/exec") {
			response.Body.Close()
			response.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
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
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].OutcomeUnknown {
		t.Fatalf("lost create settled: %+v %v", result, err)
	}
	for range 3 {
		result, err = driver.Recover(t.Context(), command, result.Effects)
		if !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].Active {
			t.Fatalf("missing intent settled: %+v %v", result, err)
		}
	}
	if mutations.Load() != 1 {
		t.Fatalf("read-only recovery mutated: %d", mutations.Load())
	}
	j, _, _ := runtimeJournal(t, command)
	if err := j.Observe(command.CommandId, result.Effects); err != nil {
		t.Fatal(err)
	}
	next := proto.Clone(command.Epoch).(*agentv1.Epoch)
	next.ExecutionEpoch++
	if err := j.Prepare(next); err != nil {
		t.Fatal(err)
	}
	barrier, err := j.Barrier(next)
	if err != nil {
		t.Fatal(err)
	}
	if operation.BarrierDrained(barrier) == nil {
		t.Fatal("unknown clone released barrier")
	}
}

func TestTypedPostgresCloneRecoveryRequiresQuietMatchingRecords(t *testing.T) {
	r, api, _, command, secrets := postgresReplicaFixture(t, false)
	result, err := r.Execute(t.Context(), command, secrets)
	result, err = settleReplica(t, r, command, result, err)
	if err != nil {
		t.Fatal(err)
	}
	name := "synod-instance-" + command.Input.ResourceId
	for nameCase, file := range map[string]string{
		"outside configuration": postgres.InitializationDirectory + "/replica-config",
		"outside attempt":       postgres.InitializationDirectory + "/replica-attempt",
		"volume intent":         postgres.DataDirectory + "/.synod-clone-intent",
		"target receipt":        postgres.ReplicaReceiptFile,
	} {
		t.Run(nameCase, func(t *testing.T) {
			storageExec(t, api, name, "mv", file, file+".fixture")
			result, err := r.Recover(t.Context(), command, nil)
			if !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].Active {
				t.Fatalf("missing record settled: %+v %v", result, err)
			}
			storageExec(t, api, name, "ln", "-s", file+".fixture", file)
			result, err = r.Recover(t.Context(), command, nil)
			if !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].OutcomeUnknown {
				t.Fatalf("linked record settled: %+v %v", result, err)
			}
			storageExec(t, api, name, "rm", file)
			storageExec(t, api, name, "sh", "-c", "printf 'wrong-command\\n' > \"$1\"", "fixture", file)
			result, err = r.Recover(t.Context(), command, nil)
			if !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].Active {
				t.Fatalf("wrong record settled: %+v %v", result, err)
			}
			storageExec(t, api, name, "mv", file+".fixture", file)
			result, err = r.Recover(t.Context(), command, nil)
			result, err = settleReplica(t, r, command, result, err)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	changed := proto.Clone(command).(*agentv1.Command)
	changed.CommandId = uuid.NewString()
	hash, err := operation.CommandHash(changed)
	if err != nil {
		t.Fatal(err)
	}
	changed.InputHash = hash[:]
	if result, err := r.Recover(t.Context(), changed, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].Active {
		t.Fatalf("another command adopted clone: %+v %v", result, err)
	}
	native, err := api.ExecCreate(t.Context(), name, client.ExecCreateOptions{Cmd: []string{"sleep", "2"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ExecStart(t.Context(), native.ID, client.ExecStartOptions{Detach: true}); err != nil {
		t.Fatal(err)
	}
	result, err = r.Recover(t.Context(), command, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || len(result.Effects) < 2 || !result.Effects[0].Active {
		t.Fatalf("native work was ignored: %+v %v", result, err)
	}
	result, err = settleReplica(t, r, command, result, err)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTypedPostgresCloneMissingPublicationPhaseStaysUnknownUntilStopped(t *testing.T) {
	r, api, _, command, secrets := postgresReplicaFixture(t, false)
	result, err := r.Execute(t.Context(), command, secrets)
	result, err = settleReplica(t, r, command, result, err)
	if err != nil {
		t.Fatal(err)
	}
	name := "synod-instance-" + command.Input.ResourceId
	storageExec(t, api, name, "rm", postgres.ConfigDirectory+"/bootstrap-complete")
	deadline := time.Now().Add(10 * time.Second)
	for {
		item, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := postgresPhase(item.Container); errors.Is(err, runtime.ErrOutcomeUnknown) && len(item.Container.ExecIDs) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("healthcheck did not observe missing phase")
		}
		time.Sleep(50 * time.Millisecond)
	}
	result, err = r.Recover(t.Context(), command, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].Active {
		t.Fatalf("publication phase gap settled: %+v %v", result, err)
	}
	// A verified container stop supplies quiescence without repeating native work.
	if _, err := api.ContainerStop(t.Context(), name, client.ContainerStopOptions{Timeout: new(10)}); err != nil {
		t.Fatal(err)
	}
	result, err = r.Recover(t.Context(), command, nil)
	if err != nil || result.Effects[0].State != "replica_cloned" || result.Effects[0].Active {
		t.Fatalf("stopped publication proof: %+v %v", result, err)
	}
}

func TestTypedPostgresVerificationReadsThePriorCloneWithoutMutation(t *testing.T) {
	r, api, _, clone, secrets := postgresReplicaFixture(t, false)
	clusterID := uuid.MustParse(clone.Epoch.ClusterId)
	verify := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: clone.Input.ResourceId, Payload: &agentv1.CommandInput_VerifyPostgresReplicaClone{VerifyPostgresReplicaClone: &agentv1.VerifyPostgresReplicaClone{Postgres: clone.Input.GetClonePostgresReplica().Postgres, CloneCommandId: clone.CommandId}}})
	var mutations atomic.Int32
	hooked, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if response.Request.Method != http.MethodGet && response.Request.Method != http.MethodHead {
			mutations.Add(1)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer hooked.Close()
	reader, err := New(hooked, r.options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Execute(t.Context(), verify, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || mutations.Load() != 0 {
		t.Fatalf("verification copied absent data: %+v %v", result, err)
	}
	result, err = r.Execute(t.Context(), clone, secrets)
	if _, err = settleReplica(t, r, clone, result, err); err != nil {
		t.Fatal(err)
	}
	for _, recovering := range []bool{false, true} {
		if recovering {
			result, err = reader.Recover(t.Context(), verify, nil)
		} else {
			result, err = reader.Execute(t.Context(), verify, nil)
		}
		result, err = settleReplica(t, reader, verify, result, err)
		if err != nil || len(result.Effects) != 1 || result.Effects[0].CommandId != verify.CommandId || result.Effects[0].NativeOperationId != "docker-command:"+verify.CommandId || result.Effects[0].Active || result.Effects[0].OutcomeUnknown {
			t.Fatalf("prior clone not bound to new proof: %+v %v", result, err)
		}
		if err := operation.CheckStepEffects(&agentv1.PlanStep{Command: verify, Postcondition: agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED}, result.Effects); err != nil {
			t.Fatal(err)
		}
	}
	failed := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: clone.Input.ResourceId, Payload: &agentv1.CommandInput_VerifyPostgresReplicaFailure{VerifyPostgresReplicaFailure: &agentv1.VerifyPostgresReplicaFailure{Postgres: clone.Input.GetClonePostgresReplica().Postgres, CloneCommandId: clone.CommandId}}})
	result, err = reader.Execute(t.Context(), failed, nil)
	result, err = settleReplica(t, reader, failed, result, err)
	if !errors.Is(err, runtime.ErrConfigurationConflict) || result.Effects[0].State != "replica_cloned" {
		t.Fatalf("completed clone passed failure precondition: %+v %v", result, err)
	}
	wrong := proto.Clone(verify.Input).(*agentv1.CommandInput)
	wrong.GetVerifyPostgresReplicaClone().CloneCommandId = uuid.NewString()
	wrongCommand := runtimeCommand(t, r, clusterID, wrong)
	if result, err := reader.Execute(t.Context(), wrongCommand, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].OutcomeUnknown {
		t.Fatalf("accepted another clone identity: %+v %v", result, err)
	}
	changed := proto.Clone(verify.Input).(*agentv1.CommandInput)
	changed.GetVerifyPostgresReplicaClone().Postgres.ApplicationCredentialVersion++
	if _, err := reader.Execute(t.Context(), runtimeCommand(t, r, clusterID, changed), nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatalf("accepted changed configuration: %v", err)
	}
	name := "synod-instance-" + clone.Input.ResourceId
	for _, filename := range []string{postgres.InitializationDirectory + "/replica-attempt", postgres.DataDirectory + "/.synod-clone-intent", postgres.ReplicaReceiptFile} {
		original, err := r.postgresReplicaRecord(t.Context(), name, filename)
		if err != nil {
			t.Fatal(err)
		}
		storageExec(t, api, name, "sh", "-c", `printf '%s' "$1" > "$2"`, "--", "conflicting record\n", filename)
		if result, err := reader.Execute(t.Context(), verify, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].OutcomeUnknown {
			t.Fatalf("accepted conflicting record %s: %+v %v", filename, result, err)
		}
		storageExec(t, api, name, "sh", "-c", `printf '%s' "$1" > "$2"`, "--", original, filename)
	}
	native, err := api.ExecCreate(t.Context(), name, client.ExecCreateOptions{Cmd: []string{"sleep", "2"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ExecStart(t.Context(), native.ID, client.ExecStartOptions{Detach: true}); err != nil {
		t.Fatal(err)
	}
	if result, err := reader.Execute(t.Context(), verify, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) || !slices.ContainsFunc(result.Effects, func(e *agentv1.Effect) bool { return e.Active }) {
		t.Fatalf("active native work was ignored: %+v %v", result, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		state, err := api.ExecInspect(t.Context(), native.ID, client.ExecInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !state.Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture exec did not stop")
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: clone.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatal(err)
	}
	if result, err := reader.Execute(t.Context(), verify, nil); err != nil || result.Effects[0].State != "replica_cloned" {
		t.Fatalf("stopped clone proof: %+v %v", result, err)
	}
	start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: clone.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	startPostgresFixture(t, r, start)
	if result, err := reader.Recover(t.Context(), verify, nil); err != nil || result.Effects[0].State != "replica_cloned" {
		t.Fatalf("running clone proof: %+v %v", result, err)
	}
	if mutations.Load() != 0 {
		t.Fatalf("verification made %d native mutation requests", mutations.Load())
	}
}

func TestTypedPostgresVerificationKeepsAKnownCloneRefusal(t *testing.T) {
	r, _, _, clone, secrets := postgresReplicaFixture(t, true)
	result, err := r.Execute(t.Context(), clone, secrets)
	result, err = settleReplica(t, r, clone, result, err)
	if !errors.Is(err, runtime.ErrConfigurationConflict) || result.Effects[0].State != "replica_refused" {
		t.Fatalf("fixture clone did not refuse: %+v %v", result, err)
	}
	verify := runtimeCommand(t, r, uuid.MustParse(clone.Epoch.ClusterId), &agentv1.CommandInput{ResourceId: clone.Input.ResourceId, Payload: &agentv1.CommandInput_VerifyPostgresReplicaClone{VerifyPostgresReplicaClone: &agentv1.VerifyPostgresReplicaClone{Postgres: clone.Input.GetClonePostgresReplica().Postgres, CloneCommandId: clone.CommandId}}})
	result, err = r.Execute(t.Context(), verify, nil)
	result, err = settleReplica(t, r, verify, result, err)
	if !errors.Is(err, runtime.ErrConfigurationConflict) || result.Effects[0].State != "replica_refused" || result.Effects[0].CommandId != verify.CommandId || result.Effects[0].Active || result.Effects[0].OutcomeUnknown {
		t.Fatalf("verification lost terminal refusal: %+v %v", result, err)
	}
}

func TestTypedPostgresFailureVerificationProvesQuiescenceWithoutMutation(t *testing.T) {
	r, api, _, clone, secrets := postgresReplicaFixture(t, true)
	clusterID := uuid.MustParse(clone.Epoch.ClusterId)
	verify := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: clone.Input.ResourceId, Payload: &agentv1.CommandInput_VerifyPostgresReplicaFailure{VerifyPostgresReplicaFailure: &agentv1.VerifyPostgresReplicaFailure{Postgres: clone.Input.GetClonePostgresReplica().Postgres, CloneCommandId: clone.CommandId}}})
	var mutations atomic.Int32
	hooked, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if response.Request.Method != http.MethodGet && response.Request.Method != http.MethodHead {
			mutations.Add(1)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer hooked.Close()
	reader, err := New(hooked, r.options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Execute(t.Context(), verify, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatalf("absent failure passed: %+v %v", result, err)
	}
	result, err = r.Execute(t.Context(), clone, secrets)
	result, err = settleReplica(t, r, clone, result, err)
	if !errors.Is(err, runtime.ErrConfigurationConflict) || result.Effects[0].State != "replica_refused" {
		t.Fatalf("fixture did not refuse clone: %+v %v", result, err)
	}
	for _, recovering := range []bool{false, true} {
		if recovering {
			result, err = reader.Recover(t.Context(), verify, nil)
		} else {
			result, err = reader.Execute(t.Context(), verify, nil)
		}
		result, err = settleReplica(t, reader, verify, result, err)
		if err != nil || len(result.Effects) != 1 || result.Effects[0].CommandId != verify.CommandId || result.Effects[0].Active || result.Effects[0].OutcomeUnknown {
			t.Fatalf("failed-copy proof: %+v %v", result, err)
		}
		if err := operation.CheckStepEffects(&agentv1.PlanStep{Command: verify, Postcondition: agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_FAILED}, result.Effects); err != nil {
			t.Fatal(err)
		}
	}
	wrong := proto.Clone(verify.Input).(*agentv1.CommandInput)
	wrong.GetVerifyPostgresReplicaFailure().CloneCommandId = uuid.NewString()
	if result, err := reader.Execute(t.Context(), runtimeCommand(t, r, clusterID, wrong), nil); !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatalf("wrong original failure passed: %+v %v", result, err)
	}
	changed := proto.Clone(verify.Input).(*agentv1.CommandInput)
	changed.GetVerifyPostgresReplicaFailure().Postgres.ApplicationCredentialVersion++
	if _, err := reader.Execute(t.Context(), runtimeCommand(t, r, clusterID, changed), nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatalf("changed configuration passed: %v", err)
	}
	name := "synod-instance-" + clone.Input.ResourceId
	for _, filename := range []string{postgres.InitializationDirectory + "/replica-config", postgres.InitializationDirectory + "/replica-attempt", postgres.InitializationDirectory + "/replica-failed"} {
		original, err := r.postgresReplicaRecord(t.Context(), name, filename)
		if err != nil {
			t.Fatal(err)
		}
		storageExec(t, api, name, "sh", "-c", `printf '%s' "$1" > "$2"`, "--", "conflicting record\n", filename)
		if result, err := reader.Execute(t.Context(), verify, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) {
			t.Fatalf("changed native evidence passed: %+v %v", result, err)
		}
		storageExec(t, api, name, "sh", "-c", `printf '%s' "$1" > "$2"`, "--", original, filename)
	}
	native, err := api.ExecCreate(t.Context(), name, client.ExecCreateOptions{Cmd: []string{"sleep", "2"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ExecStart(t.Context(), native.ID, client.ExecStartOptions{Detach: true}); err != nil {
		t.Fatal(err)
	}
	result, err = reader.Recover(t.Context(), verify, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || !slices.ContainsFunc(result.Effects, func(effect *agentv1.Effect) bool { return effect.Active }) {
		t.Fatalf("active work passed failure proof: %+v %v", result, err)
	}
	if _, err := settleReplica(t, reader, verify, result, err); err != nil {
		t.Fatal(err)
	}
	stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: clone.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatal(err)
	}
	result, err = reader.Execute(t.Context(), verify, nil)
	if err != nil || result.Effects[0].State != "replica_refused" {
		t.Fatalf("stopped failed-copy proof: %+v %v", result, err)
	}
	if mutations.Load() != 0 {
		t.Fatalf("failure verification made %d native mutations", mutations.Load())
	}
}

func TestTypedPostgresFailureVerificationAcceptsAFailedLaterStart(t *testing.T) {
	r, api, _, clone, secrets := postgresReplicaFixture(t, false)
	result, err := r.Execute(t.Context(), clone, secrets)
	if _, err = settleReplica(t, r, clone, result, err); err != nil {
		t.Fatal(err)
	}
	name := "synod-instance-" + clone.Input.ResourceId
	receipt, err := r.postgresReplicaRecord(t.Context(), name, postgres.ReplicaReceiptFile)
	if err != nil {
		t.Fatal(err)
	}
	storageExec(t, api, name, "rm", postgres.DataDirectory+"/replica/standby.signal")
	clusterID := uuid.MustParse(clone.Epoch.ClusterId)
	stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: clone.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatal(err)
	}
	start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: clone.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	result, err = r.Execute(t.Context(), start, nil)
	result, err = settleReplica(t, r, start, result, err)
	if err == nil || slices.ContainsFunc(result.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown }) {
		t.Fatalf("later start did not stop after drift: %+v %v", result, err)
	}
	verify := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: clone.Input.ResourceId, Payload: &agentv1.CommandInput_VerifyPostgresReplicaFailure{VerifyPostgresReplicaFailure: &agentv1.VerifyPostgresReplicaFailure{Postgres: clone.Input.GetClonePostgresReplica().Postgres, CloneCommandId: clone.CommandId}}})
	result, err = r.Execute(t.Context(), verify, nil)
	result, err = settleReplica(t, r, verify, result, err)
	if err != nil || result.Effects[0].State != "replica_refused" {
		t.Fatalf("later standby failure was not proved: %+v %v", result, err)
	}
	retained, err := r.postgresReplicaRecord(t.Context(), name, postgres.ReplicaReceiptFile)
	if err != nil || retained != receipt {
		t.Fatalf("failure proof changed the completed copy receipt: %v", err)
	}
}
