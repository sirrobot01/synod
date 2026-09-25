package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ErrPatroniAccountDrift reports a Patroni account or role that grants more than its own scope.
var ErrPatroniAccountDrift = errors.New("patroni_dcs_account_drift")

// PatroniAccounts creates and deletes one etcd account for each Patroni cluster.
// Its client must use an account with the etcd root role.
type PatroniAccounts struct {
	client       *clientv3.Client
	installation uuid.UUID
}

// NewPatroniAccounts binds account administration to one installation namespace.
func NewPatroniAccounts(client *clientv3.Client, installation uuid.UUID) (*PatroniAccounts, error) {
	if client == nil || installation == uuid.Nil {
		return nil, fmt.Errorf("Patroni DCS administration requires a client and an installation")
	}
	return &PatroniAccounts{client: client, installation: installation}, nil
}

// VerifyAdmin checks that the administration account holds the root role.
func (a *PatroniAccounts) VerifyAdmin(ctx context.Context, username string) error {
	user, err := a.client.Auth.UserGet(ctx, username)
	if err != nil {
		return fmt.Errorf("read the Patroni DCS admin account: %w", err)
	}
	if slices.Contains(user.Roles, "root") {
		return nil
	}
	return fmt.Errorf("the Patroni DCS admin account %q does not hold the etcd root role", username)
}

// PatroniScope returns the account name and the key prefix Patroni uses for one cluster.
func (a *PatroniAccounts) PatroniScope(cluster uuid.UUID) (name, prefix string) {
	return "synod_patroni_" + cluster.String(), "/synod-patroni/" + a.installation.String() + "/" + cluster.String() + "/"
}

// EnsureAccount creates the cluster account and role when absent and sets the password.
// It refuses a role or user with more access than the cluster scope. It does not repair drift.
func (a *PatroniAccounts) EnsureAccount(ctx context.Context, cluster uuid.UUID, password string) error {
	if cluster == uuid.Nil || len(password) < 32 || len(password) > 256 {
		return fmt.Errorf("Patroni DCS account requires a cluster and a 32 to 256 byte password")
	}
	for _, value := range []byte(password) {
		if value < 33 || value > 126 {
			return fmt.Errorf("Patroni DCS account password requires visible ASCII bytes")
		}
	}
	name, prefix := a.PatroniScope(cluster)
	end := clientv3.GetPrefixRangeEnd(prefix)
	if _, err := a.client.Auth.RoleAdd(ctx, name); err != nil && !errors.Is(err, rpctypes.ErrRoleAlreadyExist) {
		return fmt.Errorf("create Patroni DCS role for cluster %s: %w", cluster, err)
	}
	role, err := a.client.Auth.RoleGet(ctx, name)
	if err != nil {
		return fmt.Errorf("read Patroni DCS role for cluster %s: %w", cluster, err)
	}
	switch {
	case len(role.Perm) == 0:
		if _, err := a.client.Auth.RoleGrantPermission(ctx, name, prefix, end, clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
			return fmt.Errorf("grant Patroni DCS scope for cluster %s: %w", cluster, err)
		}
	case len(role.Perm) != 1 || role.Perm[0].PermType != authpb.READWRITE || !bytes.Equal(role.Perm[0].Key, []byte(prefix)) || !bytes.Equal(role.Perm[0].RangeEnd, []byte(end)):
		return fmt.Errorf("%w: role %s grants %d permissions outside its scope", ErrPatroniAccountDrift, name, len(role.Perm))
	}
	if _, err := a.client.Auth.UserAdd(ctx, name, password); err != nil && !errors.Is(err, rpctypes.ErrUserAlreadyExist) {
		return fmt.Errorf("create Patroni DCS account for cluster %s: %w", cluster, err)
	}
	user, err := a.client.Auth.UserGet(ctx, name)
	if err != nil {
		return fmt.Errorf("read Patroni DCS account for cluster %s: %w", cluster, err)
	}
	switch {
	case len(user.Roles) == 0:
		if _, err := a.client.Auth.UserGrantRole(ctx, name, name); err != nil {
			return fmt.Errorf("grant Patroni DCS role for cluster %s: %w", cluster, err)
		}
	case len(user.Roles) != 1 || user.Roles[0] != name:
		return fmt.Errorf("%w: account %s holds %d roles", ErrPatroniAccountDrift, name, len(user.Roles))
	}
	// A retry after a lost reply sets the same password again.
	if _, err := a.client.Auth.UserChangePassword(ctx, name, password); err != nil {
		return fmt.Errorf("set Patroni DCS password for cluster %s: %w", cluster, err)
	}
	return nil
}

// DeleteAccount removes the cluster account and then its role. Absent objects are success.
func (a *PatroniAccounts) DeleteAccount(ctx context.Context, cluster uuid.UUID) error {
	if cluster == uuid.Nil {
		return fmt.Errorf("Patroni DCS account deletion requires a cluster")
	}
	name, _ := a.PatroniScope(cluster)
	if _, err := a.client.Auth.UserDelete(ctx, name); err != nil && !errors.Is(err, rpctypes.ErrUserNotFound) {
		return fmt.Errorf("delete Patroni DCS account for cluster %s: %w", cluster, err)
	}
	if _, err := a.client.Auth.RoleDelete(ctx, name); err != nil && !errors.Is(err, rpctypes.ErrRoleNotFound) {
		return fmt.Errorf("delete Patroni DCS role for cluster %s: %w", cluster, err)
	}
	return nil
}

// DeleteScope removes every key under the cluster's Patroni scope and then its account.
// Call it only after the members are gone, or Patroni writes the keys again. A repeat is harmless.
func (a *PatroniAccounts) DeleteScope(ctx context.Context, cluster uuid.UUID) error {
	if cluster == uuid.Nil {
		return fmt.Errorf("Patroni DCS scope deletion requires a cluster")
	}
	_, prefix := a.PatroniScope(cluster)
	if _, err := a.client.Delete(ctx, prefix, clientv3.WithPrefix()); err != nil {
		return fmt.Errorf("delete Patroni DCS keys for cluster %s: %w", cluster, err)
	}
	return a.DeleteAccount(ctx, cluster)
}
