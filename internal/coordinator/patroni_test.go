package coordinator

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// DeleteScope removes a cluster's keys and account and leaves every other scope alone.
func TestPatroniDeleteScope(t *testing.T) {
	client := testClient(t)
	accounts, err := NewPatroniAccounts(client, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	deleted, kept := uuid.New(), uuid.New()
	for _, cluster := range []uuid.UUID{deleted, kept} {
		if err := accounts.EnsureAccount(t.Context(), cluster, strings.Repeat("p", 40)); err != nil {
			t.Fatal(err)
		}
		_, scope := accounts.PatroniScope(cluster)
		for _, key := range []string{"leader", "members/a", "initialize"} {
			if _, err := client.Put(t.Context(), scope+key, "value"); err != nil {
				t.Fatal(err)
			}
		}
	}
	for range 2 {
		if err := accounts.DeleteScope(t.Context(), deleted); err != nil {
			t.Fatal("delete Patroni scope", err)
		}
	}
	name, scope := accounts.PatroniScope(deleted)
	if remaining, err := client.Get(t.Context(), scope, clientv3.WithPrefix(), clientv3.WithCountOnly()); err != nil || remaining.Count != 0 {
		t.Fatalf("deleted scope keeps %v keys: %v", remaining, err)
	}
	if _, err := client.Auth.UserGet(t.Context(), name); !errors.Is(err, rpctypes.ErrUserNotFound) {
		t.Fatal("deleted scope keeps its account", err)
	}
	if _, err := client.Auth.RoleGet(t.Context(), name); !errors.Is(err, rpctypes.ErrRoleNotFound) {
		t.Fatal("deleted scope keeps its role", err)
	}
	keptName, keptScope := accounts.PatroniScope(kept)
	if remaining, err := client.Get(t.Context(), keptScope, clientv3.WithPrefix(), clientv3.WithCountOnly()); err != nil || remaining.Count != 3 {
		t.Fatalf("another scope lost keys: %v %v", remaining, err)
	}
	if _, err := client.Auth.UserGet(t.Context(), keptName); err != nil {
		t.Fatal("another scope lost its account", err)
	}
	if err := accounts.DeleteScope(t.Context(), uuid.Nil); err == nil {
		t.Fatal("scope deletion accepted a nil cluster")
	}
}
