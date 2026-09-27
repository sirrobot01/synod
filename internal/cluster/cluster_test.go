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
		{SchemaVersion: 1, Name: "db", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "17.11"}},
		{SchemaVersion: 1, Name: "db", Engine: PostgreSQL, PostgreSQL: &PostgreSQLSpec{Version: "17.11", Topology: "Primary Replica"}},
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
