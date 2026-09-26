package cluster

import (
	"encoding/json"
	"testing"
)

func TestValidate(t *testing.T) {
	valid := CreateSpec{SchemaVersion: 1, Name: "primary", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "17.11", Topology: "standalone"}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	patroni := CreateSpec{SchemaVersion: 1, Name: "ha", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "17.11", Topology: "patroni", FailureDomain: "host", ReplicationPolicy: "synchronous"}}
	if err := patroni.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, persistence := range []string{"none", "rdb", "aof", "rdb+aof"} {
		s := CreateSpec{SchemaVersion: 1, Name: "cache", Engine: Redis, Redis: &RedisSpec{Version: "8.2.1", Topology: "standalone", Persistence: persistence}}
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"", "Mixed", " has-space", "bad_name", "-first", "å"} {
		s := valid
		s.Name = name
		if err := s.Validate(); err == nil {
			t.Fatalf("accepted name %q", name)
		}
	}
	for _, s := range []CreateSpec{
		{},
		{SchemaVersion: 2, Name: "db", Engine: PostgreSQL, PostgreSQL: valid.PostgreSQL},
		{SchemaVersion: 1, Name: "db", Engine: PostgreSQL},
		{SchemaVersion: 1, Name: "db", Engine: PostgreSQL, PostgreSQL: valid.PostgreSQL, Redis: &RedisSpec{}},
		{SchemaVersion: 1, Name: "db", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "latest", Topology: "standalone"}},
		{SchemaVersion: 1, Name: "db", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "17.11", Topology: "replicas"}},
		{SchemaVersion: 1, Name: "db", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "17.11", Topology: "patroni"}},
		{SchemaVersion: 1, Name: "db", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "17.11", Topology: "patroni", FailureDomain: "zone"}},
		{SchemaVersion: 1, Name: "db", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "17.11", Topology: "standalone", FailureDomain: "host"}},
		{SchemaVersion: 1, Name: "db", Engine: Redis, Redis: &RedisSpec{Version: "8.2.1", Topology: "standalone", Persistence: "maybe"}},
	} {
		if err := s.Validate(); err == nil {
			t.Fatalf("accepted invalid spec %#v", s)
		}
	}
}

func TestCreateSpecV1Representation(t *testing.T) {
	const want = `{"schema_version":1,"name":"db","engine":"postgres","postgresql":{"version":"17.11","topology":"standalone"},"redis":null}`
	var spec CreateSpec
	if err := json.Unmarshal([]byte(`{"postgresql":{"topology":"standalone","version":"17.11"}, "engine":"postgres", "name":"db", "schema_version":1}`), &spec); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != want {
		t.Fatalf("v1 hash representation changed: %s", body)
	}
}

func TestPostgresReplicationPolicyValidation(t *testing.T) {
	for _, topology := range []string{"standalone", "patroni"} {
		for _, policy := range []string{"", "asynchronous", "synchronous", "automatic", "sync", "SYNCHRONOUS"} {
			spec := CreateSpec{SchemaVersion: 1, Name: "db", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "17.11", Topology: topology, ReplicationPolicy: policy}}
			if topology == "patroni" {
				spec.PostgreSQL.FailureDomain = "host"
			}
			// Standalone PostgreSQL has no replica to wait for.
			valid := policy == "" || policy == "asynchronous" || (policy == "synchronous" && topology == "patroni")
			if err := spec.Validate(); (err == nil) != valid {
				t.Fatalf("%s policy %q: %v", topology, policy, err)
			}
		}
	}
}

func TestPostgresEndpointValidation(t *testing.T) {
	endpoint := func(address string, writer, reader uint16) *PostgresEndpoint {
		return &PostgresEndpoint{Address: address, WriterPort: writer, ReaderPort: reader}
	}
	for name, test := range map[string]struct {
		topology string
		endpoint *PostgresEndpoint
		valid    bool
	}{
		"none":           {"patroni", nil, true},
		"load balancer":  {"patroni", endpoint("192.0.2.10", 5432, 5433), true},
		"ipv6":           {"patroni", endpoint("2001:db8::10", 5432, 5433), true},
		"standalone":     {"standalone", endpoint("192.0.2.10", 5432, 5433), false},
		"loopback":       {"patroni", endpoint("127.0.0.1", 5432, 5433), false},
		"name":           {"patroni", endpoint("db.example.com", 5432, 5433), false},
		"non-canonical":  {"patroni", endpoint("2001:DB8::10", 5432, 5433), false},
		"same ports":     {"patroni", endpoint("192.0.2.10", 5432, 5432), false},
		"missing writer": {"patroni", endpoint("192.0.2.10", 0, 5433), false},
	} {
		spec := CreateSpec{SchemaVersion: 1, Name: "db", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "17.11", Topology: test.topology, Endpoint: test.endpoint}}
		if test.topology == "patroni" {
			spec.PostgreSQL.FailureDomain = "host"
		}
		if err := spec.Validate(); (err == nil) != test.valid {
			t.Errorf("%s: %v", name, err)
		}
	}
}
