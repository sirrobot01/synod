package engine

import (
	"context"
	"time"
)

// ProbeState is what one check found. Unavailable is its own state because
// specification 06 forbids reporting healthy when a required probe could not run.
type ProbeState string

const (
	// ProbeUnavailable is the zero value, so an unfilled probe is never a pass.
	ProbeUnavailable ProbeState = ""
	ProbePassed      ProbeState = "passed"
	ProbeFailed      ProbeState = "failed"
)

// Probe is one check and a stable reason code. The reason never carries a credential
// or a host path, because an observation reaches an operator's screen.
type Probe struct {
	State  ProbeState `json:"state"`
	Reason string     `json:"reason,omitempty"`
}

// Observation separates the checks specification 06 requires to be reported apart. A
// container that is running but whose database has not finished starting is a
// different problem from one that crashed.
type Observation struct {
	Postgres         *PostgresObservation `json:"postgres,omitzero"`
	Reachable        Probe                `json:"reachable"`
	ProcessLive      Probe                `json:"process_live"`
	Ready            Probe                `json:"ready"`
	Endpoint         Probe                `json:"endpoint"`
	ObservedRevision int64                `json:"observed_revision"`
	ObservedAt       time.Time            `json:"observed_at"`
}

// Healthy reports an engine that is working. An unavailable probe is not a pass, and
// an observation with no time on it cannot be told from one taken an hour ago.
func (o Observation) Healthy() bool {
	for _, probe := range []Probe{o.Reachable, o.ProcessLive, o.Ready, o.Endpoint} {
		if probe.State != ProbePassed {
			return false
		}
	}
	return !o.ObservedAt.IsZero()
}

// Endpoint is where a client reaches one engine.
type Endpoint struct {
	PostgresTLS      *PostgresTLS
	PostgresRole     string
	Address          string
	Port             uint16
	Database         string
	Role             string
	RedisReplication *RedisReplicationCheck
}

// PostgresTLS names public trust and the IP identity of the database certificate.
type PostgresTLS struct {
	RootCAPEM  []byte
	ServerName string
}

// RedisReplicationCheck describes a native role and upstream that a probe must prove.
type RedisReplicationCheck struct {
	ExpectedRole string
	Upstream     *RedisUpstream
}

type RedisUpstream struct {
	Address string
	Port    uint32
}

// Prober checks one live engine through the endpoint it publishes. It is separate
// from Engine because probing needs a network and a credential and resolving
// capabilities needs neither.
type Prober interface {
	// Probe never returns an error: a check that could not run is an unavailable
	// probe, and an error here would invite a caller to read "could not tell" as
	// "not working".
	Probe(ctx context.Context, endpoint Endpoint, password []byte, revision int64) Observation
}

// PostgresObservation records native role state at the observation time.
type PostgresObservation struct {
	SystemIdentifier string `json:"system_identifier,omitempty"`
	Role             string `json:"role"`
	ReadOnly         bool   `json:"read_only"`
}
