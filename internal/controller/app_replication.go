package controller

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func runAppReplication(ctx context.Context, store *postgres.Store, transport *controlstream.Server, installation uuid.UUID, ring *secret.Keyring, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	pushed := map[uuid.UUID]*sessionSecrets{}
	for {
		work, cancel := context.WithTimeout(ctx, 10*time.Second)
		apps, err := store.ListApps(work, installation)
		if err != nil && ctx.Err() == nil {
			logger.Warn("read app failover candidates", "error", err)
		}
		for _, item := range apps {
			if (item.Tier != app.TierSingleWriter && item.Tier != app.TierDatabase) || item.DesiredLifecycle != "running" {
				continue
			}
			if err := store.ReconcileAppMove(work, installation, item.ID); err != nil && ctx.Err() == nil {
				logger.Warn("reconcile app move", "app_id", item.ID, "error", err)
			}
			if item.Tier == app.TierDatabase {
				if _, err := store.AdmitDatabaseRejoin(work, installation, item.ID); err != nil && ctx.Err() == nil {
					logger.Warn("admit database rejoin", "app_id", item.ID, "error", err)
				}
			}
			if _, err := store.AdmitAppFailover(work, installation, item.ID); err != nil && ctx.Err() == nil {
				logger.Warn("admit app failover", "app_id", item.ID, "error", err)
			}
		}
		targets, err := store.AppReplicationTargets(work, installation)
		if err != nil && ctx.Err() == nil {
			logger.Warn("read app replication", "error", err)
		}
		for node, desired := range targets {
			peer, ok := transport.Session(node)
			if !ok || peer.SessionID != desired.SessionId || !slices.Contains(peer.Capabilities, replication.Capability) {
				continue
			}
			table, acknowledged, err := store.PrepareAppReplication(work, installation, node, desired)
			if err != nil {
				if ctx.Err() == nil {
					logger.Warn("prepare app replication", "node_id", node, "error", err)
				}
				continue
			}
			if !acknowledged {
				if err := transport.Send(node, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Replication{Replication: table}}); err != nil && ctx.Err() == nil {
					logger.Warn("send app replication", "node_id", node, "error", err)
				}
				continue
			}
			for _, assignment := range table.Apps {
				if assignment.Database != nil {
					if err := pushDatabaseCredentials(work, store, ring, transport, installation, node, peer.SessionID, uuid.MustParse(assignment.AppId), pushed); err != nil && ctx.Err() == nil {
						logger.Warn("push database credentials", "app_id", assignment.AppId, "node_id", node, "error", err)
					}
				}
				if assignment.Role != replication.RoleActive {
					continue
				}
				grant, err := store.PrepareAppWriterLease(work, installation, uuid.MustParse(assignment.AppId), node, uuid.MustParse(peer.SessionID), assignment.Epoch, false)
				if err != nil {
					if ctx.Err() == nil {
						logger.Warn("prepare app writer lease", "app_id", assignment.AppId, "error", err)
					}
					continue
				}
				if grant == nil {
					continue
				}
				if err := transport.Send(node, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: grant}}); err != nil && ctx.Err() == nil {
					logger.Warn("send app writer lease", "app_id", assignment.AppId, "error", err)
				}
			}
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sessionSecrets records which credential versions one agent session already holds.
type sessionSecrets struct {
	session string
	sent    map[string]bool
}

// Session secret names of the database credentials. The agent supervisor reads them
// as "db:<app>:<name>".
var databaseSecretNames = map[string]string{postgres.SuperuserCredential: "admin", postgres.ReplicationCredential: "replication", postgres.ApplicationCredential: "app"}

// pushDatabaseCredentials sends a tier 1 app's credentials to one agent session. Each
// version is sent once per session. Secrets live only in agent memory, so a new
// session receives them again.
func pushDatabaseCredentials(ctx context.Context, store *postgres.Store, ring *secret.Keyring, transport *controlstream.Server, installation, node uuid.UUID, session string, appID uuid.UUID, pushed map[uuid.UUID]*sessionSecrets) error {
	if ring == nil {
		return errors.New("this controller holds no keyring")
	}
	state := pushed[node]
	if state == nil || state.session != session {
		state = &sessionSecrets{session: session, sent: map[string]bool{}}
		pushed[node] = state
	}
	for _, purpose := range []string{postgres.SuperuserCredential, postgres.ReplicationCredential, postgres.ApplicationCredential} {
		credential, err := store.ReadAppCredential(ctx, installation, appID, purpose)
		if err != nil {
			return err
		}
		if credential == nil {
			continue
		}
		key := credential.CredentialID.String() + "/" + strconv.FormatInt(credential.Version, 10)
		if state.sent[key] {
			continue
		}
		binding := secret.Binding{InstallationID: installation, OwnerType: "app", OwnerID: appID, CredentialID: credential.CredentialID, Version: credential.Version}
		material, err := ring.Open(binding, credential.Envelope)
		if err != nil {
			return err
		}
		message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Secret{Secret: &agentv1.SecretMaterial{CredentialId: credential.CredentialID.String(), Version: credential.Version, Purpose: "db:" + appID.String() + ":" + databaseSecretNames[purpose], Material: material}}}
		err = transport.Send(node, session, message)
		clear(material)
		if err != nil {
			return err
		}
		state.sent[key] = true
	}
	return nil
}
