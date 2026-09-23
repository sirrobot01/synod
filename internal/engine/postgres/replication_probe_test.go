package postgres

import (
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/engine"
)

func TestPostgresReplicationProbeChecksIdentityAndNativeStream(t *testing.T) {
	primary := bootstrapContainer(t, "")
	writer := primary.connect(t)
	host, port, err := net.SplitHostPort(primary.address)
	if err != nil {
		t.Fatal(err)
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := engine.Endpoint{Address: host, Port: uint16(number), Database: "postgres", Role: "synod_admin", PostgresRole: "primary", PostgresReplication: &engine.PostgresReplicationCheck{InstallationID: primary.command[5], ClusterID: primary.command[6], ResourceID: primary.command[7]}}
	password := []byte("admin-fixture-password")
	observed := New().Probe(t.Context(), endpoint, password, 1)
	if !observed.Healthy() || observed.Postgres == nil || observed.Postgres.SystemIdentifier == "" {
		t.Fatalf("primary identity: %+v", observed)
	}
	systemID := observed.Postgres.SystemIdentifier
	for _, field := range []*string{&endpoint.PostgresReplication.InstallationID, &endpoint.PostgresReplication.ClusterID, &endpoint.PostgresReplication.ResourceID} {
		before := *field
		*field = uuid.NewString()
		if observed := New().Probe(t.Context(), endpoint, password, 1); observed.Healthy() || observed.Ready.Reason != reasonIdentityMismatch {
			t.Fatalf("wrong identity: %+v", observed)
		}
		*field = before
	}
	primary.adminSQL(t, "GRANT SELECT ON synod_control.bootstrap TO PUBLIC")
	if observed := New().Probe(t.Context(), endpoint, password, 1); observed.Healthy() || observed.Ready.Reason != reasonIdentityMismatch {
		t.Fatalf("unprotected receipt: %+v", observed)
	}
	primary.adminSQL(t, "REVOKE SELECT ON synod_control.bootstrap FROM PUBLIC")
	primary.adminSQL(t, "ALTER TABLE synod_control.bootstrap RENAME TO missing_bootstrap")
	if observed := New().Probe(t.Context(), endpoint, password, 1); observed.Healthy() || observed.Ready.State != engine.ProbeUnavailable || observed.Ready.Reason != reasonIdentityUnavailable {
		t.Fatalf("unavailable identity: %+v", observed)
	}
	primary.adminSQL(t, "ALTER TABLE synod_control.missing_bootstrap RENAME TO bootstrap")
	source := ReplicaSource{Address: strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primary.name)), Port: 5432, ResourceID: primary.command[7]}
	replica := replicaContainer(t, primary, source)
	program, err := CloneReplicaCommand(replica.config, source, replica.command[5], replica.command[6], replica.command[7], uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	replica.docker(t, append([]string{"exec", "--user", "postgres", replica.name}, program...)...)
	replica.waitPhase(t, "cloned")
	replica.docker(t, "restart", replica.name)
	replica.waitPhase(t, "ready")
	replica.connect(t)
	host, port, err = net.SplitHostPort(replica.address)
	if err != nil {
		t.Fatal(err)
	}
	number, err = strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	endpoint.Address, endpoint.Port, endpoint.PostgresRole = host, uint16(number), "replica"
	check := endpoint.PostgresReplication
	check.ResourceID = replica.command[7]
	check.Source = &engine.PostgresSource{Address: source.Address, Port: uint32(source.Port), ResourceID: source.ResourceID}
	check.MaxReplayLagBytes, check.MaxMessageAge = 16<<20, 120*time.Second
	deadline := time.Now().Add(15 * time.Second)
	for {
		observed = New().Probe(t.Context(), endpoint, password, 2)
		if observed.Healthy() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream not ready: %+v; receiver host and port: %s; expected %s:%d", observed, replica.adminSQL(t, "SELECT sender_host,sender_port FROM pg_stat_wal_receiver"), source.Address, source.Port)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if observed.Postgres.SystemIdentifier != systemID || observed.Postgres.Replication == nil || observed.Postgres.Replication.ReplayLagBytes == nil {
		t.Fatalf("native stream identity: %+v", observed)
	}
	for _, test := range []struct {
		name    string
		change  func()
		restore func()
		reason  string
	}{
		{"source address", func() { check.Source.Address = "192.0.2.1" }, func() { check.Source.Address = source.Address }, reasonReplicationSourceMismatch},
		{"source port", func() { check.Source.Port++ }, func() { check.Source.Port-- }, reasonReplicationSourceMismatch},
		{"source identity", func() { check.Source.ResourceID = uuid.NewString() }, func() { check.Source.ResourceID = source.ResourceID }, reasonIdentityMismatch},
		{"paused replay", func() { replica.adminSQL(t, "SELECT pg_wal_replay_pause()") }, func() { replica.adminSQL(t, "SELECT pg_wal_replay_resume()") }, reasonReplicationPaused},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.change()
			defer test.restore()
			observed := New().Probe(t.Context(), endpoint, password, 3)
			if observed.Healthy() || observed.Ready.Reason != test.reason {
				t.Fatalf("native refusal: %+v", observed)
			}
		})
	}
	t.Run("stale messages", func(t *testing.T) {
		check.MaxMessageAge = time.Second
		defer func() { check.MaxMessageAge = 120 * time.Second }()
		deadline := time.Now().Add(10 * time.Second)
		for {
			observed := New().Probe(t.Context(), endpoint, password, 4)
			if !observed.Healthy() && observed.Ready.Reason == reasonReplicationStale {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("idle message freshness not measured: %+v", observed)
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
	t.Run("replay backlog", func(t *testing.T) {
		replica.adminSQL(t, "ALTER SYSTEM SET recovery_min_apply_delay='10s'")
		replica.adminSQL(t, "SELECT pg_reload_conf()")
		defer func() {
			replica.adminSQL(t, "ALTER SYSTEM RESET recovery_min_apply_delay")
			replica.adminSQL(t, "SELECT pg_reload_conf()")
		}()
		if _, err := writer.Exec(t.Context(), "CREATE TABLE lag_rows AS SELECT generate_series(1,1000) AS id"); err != nil {
			t.Fatal(err)
		}
		check.MaxReplayLagBytes = 1
		defer func() { check.MaxReplayLagBytes = 16 << 20 }()
		deadline := time.Now().Add(5 * time.Second)
		for {
			observed := New().Probe(t.Context(), endpoint, password, 5)
			if !observed.Healthy() && observed.Ready.Reason == reasonReplicationLagExceeded {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("backlog not bounded: %+v", observed)
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
	t.Run("unavailable receiver statistics", func(t *testing.T) {
		// Remove the statistics view name only in this isolated native fixture.
		primary.adminSQL(t, "ALTER VIEW pg_catalog.pg_stat_wal_receiver RENAME TO synod_fixture_wal_receiver")
		defer primary.adminSQL(t, "ALTER VIEW pg_catalog.synod_fixture_wal_receiver RENAME TO pg_stat_wal_receiver")
		deadline := time.Now().Add(20 * time.Second)
		for {
			observed := New().Probe(t.Context(), endpoint, password, 6)
			if !observed.Healthy() && observed.Ready.State == engine.ProbeUnavailable && observed.Ready.Reason == reasonReplicationUnavailable {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("unavailable stats passed readiness: %+v", observed)
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
	deadline = time.Now().Add(20 * time.Second)
	for {
		observed := New().Probe(t.Context(), endpoint, password, 6)
		if observed.Healthy() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restored statistics did not recover: %+v", observed)
		}
		time.Sleep(50 * time.Millisecond)
	}
	primary.docker(t, "stop", primary.name)
	deadline = time.Now().Add(15 * time.Second)
	for {
		observed = New().Probe(t.Context(), endpoint, password, 6)
		if !observed.Healthy() && observed.Ready.Reason == reasonReplicationDisconnected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing receiver not refused: %+v", observed)
		}
		time.Sleep(50 * time.Millisecond)
	}
	encoded, err := json.Marshal(observed)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{string(password), primary.password, primary.replicationPassword, primary.verifier, primary.replicationVerifier} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("observation exposed secret material")
		}
	}
}

func TestPostgresReplicationProbeRejectsInvalidLocalInput(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*engine.Endpoint)
	}{
		{"application role", func(e *engine.Endpoint) { e.Role = "synod_app" }},
		{"invalid installation", func(e *engine.Endpoint) { e.PostgresReplication.InstallationID = "" }},
		{"absent source", func(e *engine.Endpoint) { e.PostgresReplication.Source = nil }},
		{"DNS source", func(e *engine.Endpoint) { e.PostgresReplication.Source.Address = "localhost" }},
		{"unbounded age", func(e *engine.Endpoint) { e.PostgresReplication.MaxMessageAge = 121 * time.Second }},
		{"unbounded backlog", func(e *engine.Endpoint) { e.PostgresReplication.MaxReplayLagBytes = 1<<30 + 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint := engine.Endpoint{Database: "postgres", Role: "synod_admin", PostgresRole: "replica", PostgresReplication: &engine.PostgresReplicationCheck{InstallationID: uuid.NewString(), ClusterID: uuid.NewString(), ResourceID: uuid.NewString(), Source: &engine.PostgresSource{Address: "192.0.2.1", Port: 5432, ResourceID: uuid.NewString()}, MaxReplayLagBytes: 16 << 20, MaxMessageAge: 60 * time.Second}}
			test.change(&endpoint)
			observed := New().Probe(t.Context(), endpoint, nil, 1)
			if observed.Healthy() || observed.Reachable.Reason != reasonReplicationInvalid {
				t.Fatalf("invalid native probe accepted: %+v", observed)
			}
		})
	}
}
