package postgres

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestListClustersPagination(t *testing.T) {
	store := testStore(t)
	installation := uuid.New()
	options := cluster.ListOptions{Limit: 2}
	empty, err := store.ListClusters(t.Context(), installation, options)
	if err != nil || empty.Items == nil || len(empty.Items) != 0 || empty.NextCursor != "" {
		t.Fatalf("empty page: %#v %v", empty, err)
	}
	base := time.Date(2026, 9, 21, 0, 0, 0, 123456000, time.UTC)
	insert := func(number int, owner uuid.UUID, created time.Time) uuid.UUID {
		t.Helper()
		id := uuid.MustParse(fmt.Sprintf("11111111-1111-4111-8111-%012x", number))
		spec, err := json.Marshal(cluster.CreateSpec{SchemaVersion: 1, Name: fmt.Sprintf("db-%d", number), Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.pool.Exec(t.Context(), `INSERT INTO clusters
			(id,installation_id,name,engine,spec_version,spec,desired_revision,desired_lifecycle,observed_revision,observed_lifecycle,execution_epoch,created_at,updated_at)
			VALUES ($1,$2,$3,'postgres',1,$4,1,'running',0,'provisioning',0,$5,$5)`, id, owner, fmt.Sprintf("db-%d", number), spec, created)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	var ids []uuid.UUID
	for i := 1; i <= 7; i++ {
		ids = append(ids, insert(i, installation, base))
	}
	insert(8, uuid.New(), base.Add(time.Hour))
	var collected []uuid.UUID
	var first cluster.Page
	for pageNumber := 0; ; pageNumber++ {
		page, err := store.ListClusters(t.Context(), installation, options)
		if err != nil || len(page.Items) > options.Limit {
			t.Fatalf("page %d: %#v %v", pageNumber, page, err)
		}
		if pageNumber == 0 {
			first = page
		}
		for _, item := range page.Items {
			if item.InstallationID != installation {
				t.Fatal("foreign installation in page")
			}
			collected = append(collected, item.ID)
		}
		if page.NextCursor == "" {
			break
		}
		options.Cursor = page.NextCursor
		if pageNumber > 7 {
			t.Fatal("cursor did not advance")
		}
	}
	slices.Reverse(ids)
	if !slices.Equal(collected, ids) {
		t.Fatalf("equal timestamp order: %v, want %v", collected, ids)
	}
	exact, err := store.ListClusters(t.Context(), installation, cluster.ListOptions{Limit: 7})
	if err != nil || len(exact.Items) != 7 || exact.NextCursor != "" {
		t.Fatalf("exact page: %#v %v", exact, err)
	}
	newID := insert(9, installation, base.Add(time.Hour))
	if _, err := store.pool.Exec(t.Context(), `UPDATE clusters SET desired_lifecycle='deleted',observed_lifecycle='deleted',deleted_at=$2 WHERE id=$1`, ids[3], base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(t.Context(), `UPDATE clusters SET desired_lifecycle='deleted',observed_lifecycle='deleting' WHERE id=$1`, ids[2]); err != nil {
		t.Fatal(err)
	}
	continued, err := store.ListClusters(t.Context(), installation, cluster.ListOptions{Limit: 200, Cursor: first.NextCursor})
	if err != nil || len(continued.Items) != 4 || continued.Items[0].ID != ids[2] || continued.Items[0].ObservedLifecycle != "deleting" || continued.NextCursor != "" {
		t.Fatalf("concurrent insert/deletion continuation: %#v %v", continued, err)
	}
	for _, item := range continued.Items {
		if item.ID == newID || item.ID == ids[3] {
			t.Fatal("continuation included newer insert or completed deletion")
		}
	}
	all, err := store.ListClusters(t.Context(), installation, cluster.ListOptions{Limit: 200, IncludeDeleted: true})
	if err != nil || len(all.Items) != 8 || all.Items[0].ID != newID || all.Items[4].DeletedAt == nil {
		t.Fatalf("all records: %#v %v", all, err)
	}
	for _, name := range []string{"operations", "outbox", "audit_events"} {
		var count int
		if err := store.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+name).Scan(&count); err != nil || count != 0 {
			t.Fatalf("list wrote %s: %d %v", name, count, err)
		}
	}
}

func TestListClustersCursorValidation(t *testing.T) {
	store := testStore(t)
	installation := uuid.New()
	valid := clusterCursor{Version: 1, InstallationID: installation, IncludeDeleted: false, CreatedAt: time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC), ID: uuid.New()}
	body, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	token := base64.RawURLEncoding.EncodeToString(body)
	page, err := store.ListClusters(t.Context(), installation, cluster.ListOptions{Limit: 1, Cursor: token})
	if err != nil || page.Items == nil || len(page.Items) != 0 {
		t.Fatalf("cursor tuple need not exist: %#v %v", page, err)
	}
	for _, options := range []cluster.ListOptions{
		{Limit: 0}, {Limit: -1}, {Limit: 201}, {Limit: 1, Cursor: "!"}, {Limit: 1, Cursor: strings.Repeat("x", 1025)},
		{Limit: 1, Cursor: token + "="}, {Limit: 1, Cursor: token + "\n"}, {Limit: 1, Cursor: token, IncludeDeleted: true},
	} {
		_, err := store.ListClusters(t.Context(), installation, options)
		requireCode(t, err, InvalidRequest)
	}
	_, err = store.ListClusters(t.Context(), uuid.New(), cluster.ListOptions{Limit: 1, Cursor: token})
	requireCode(t, err, InvalidRequest)
	_, err = store.ListClusters(t.Context(), uuid.Nil, cluster.ListOptions{Limit: 1})
	requireCode(t, err, InvalidRequest)
	for _, invalid := range []string{
		`null`, `{}`, string(body) + `{}`, " " + string(body),
		strings.Replace(string(body), `"version":1`, `"version":2`, 1),
		strings.Replace(string(body), `"version":1`, `"version":1,"version":1`, 1),
		strings.Replace(string(body), `"version":1`, `"version":1,"unknown":true`, 1),
		strings.Replace(string(body), `"include_deleted":false,`, "", 1),
		strings.Replace(string(body), valid.ID.String(), uuid.Nil.String(), 1),
		strings.Replace(string(body), "T00:00:00Z", "T00:00:00+00:00", 1),
	} {
		_, err := store.ListClusters(t.Context(), installation, cluster.ListOptions{Limit: 1, Cursor: base64.RawURLEncoding.EncodeToString([]byte(invalid))})
		requireCode(t, err, InvalidRequest)
	}
}
