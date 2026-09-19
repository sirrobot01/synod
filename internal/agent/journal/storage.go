package journal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

const (
	metadataKey                = "synod/agent/journal/v2/meta"
	commandPrefix              = "synod/agent/journal/v2/command/"
	clusterPrefix              = "synod/agent/journal/v2/cluster/"
	metadataReservation  int64 = 4096
	maxFormatRecordBytes       = 64 << 20
)

// Usage separates logical records, reserved growth, and estimated physical use.
// LastWriteBytes includes the Pebble batch encoding, before WAL framing.
type Usage struct {
	Records            int
	Clusters           int
	LogicalStoredBytes int64
	ReservedBytes      int64
	PhysicalDiskBytes  uint64
	LastWriteBytes     int
	LastWriteRecords   int
	ExecutionBlocked   bool
}

type metadata struct {
	Version          int      `json:"version"`
	Identity         Identity `json:"identity"`
	ExecutionBlocked bool     `json:"execution_blocked,omitzero"`
}

// Protobuf encoding bounds effect storage without JSON string expansion.
type storedCommand struct {
	Record  commandRecord `json:"record"`
	Effects []byte        `json:"effects,omitempty"`
}

type transition struct {
	batch *pebble.Batch
	usage Usage
	sizes map[string]int64
}

func commandReservation(commandBytes, resultBytes int) int64 {
	return 4096 + 4*((int64(commandBytes)+2)/3) + 8*((int64(resultBytes)+2)/3)
}

func inventoryReservation(inventoryBytes int) int64 {
	return 4096 + 4*((int64(inventoryBytes)+2)/3)
}

func decodeRecord(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrRecoveryRequired
	}
	return nil
}

func (j *Journal) load() error {
	j.state = diskState{Clusters: make(map[string]clusterState), Commands: make(map[string]commandRecord)}
	iter, err := j.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()
	var legacy []byte
	foundMetadata := false
	foundLegacy := false
	for iter.First(); iter.Valid(); iter.Next() {
		key, value := string(iter.Key()), iter.Value()
		if key == string(stateKey) {
			foundLegacy = true
			// The old snapshot is subject to the requested total capacity.
			if int64(len(value)) > j.options.MaxStoredBytes {
				return ErrBusy
			}
			legacy = bytes.Clone(value)
			continue
		}
		if len(key)+len(value) > maxFormatRecordBytes {
			return ErrRecoveryRequired
		}
		switch {
		case key == metadataKey:
			var m metadata
			if err := decodeRecord(value, &m); err != nil {
				return err
			}
			if m.Version != 2 || len(key)+len(value) > int(metadataReservation) {
				return ErrRecoveryRequired
			}
			j.state.Version, j.state.Identity, j.state.ExecutionBlocked = m.Version, m.Identity, m.ExecutionBlocked
			foundMetadata = true
		case strings.HasPrefix(key, commandPrefix):
			if len(j.state.Commands) >= j.options.MaxRecords {
				return ErrBusy
			}
			var stored storedCommand
			if err := decodeRecord(value, &stored); err != nil {
				return err
			}
			if len(stored.Record.Effects) != 0 {
				return ErrRecoveryRequired
			}
			var effects agentv1.BarrierResult
			if err := proto.Unmarshal(stored.Effects, &effects); err != nil {
				return err
			}
			if effects.Epoch != nil || effects.AdmissionsClosed || effects.Drained || effects.FailureCode != "" {
				return ErrRecoveryRequired
			}
			stored.Record.Effects = effects.Effects
			j.state.Commands[strings.TrimPrefix(key, commandPrefix)] = stored.Record
		case strings.HasPrefix(key, clusterPrefix):
			if len(j.state.Clusters) >= j.options.MaxClusters {
				return ErrBusy
			}
			var cluster clusterState
			if err := decodeRecord(value, &cluster); err != nil {
				return err
			}
			j.state.Clusters[strings.TrimPrefix(key, clusterPrefix)] = cluster
		default:
			return ErrRecoveryRequired
		}
		j.sizes[key] = int64(len(key) + len(value))
		j.usage.LogicalStoredBytes += j.sizes[key]
		if j.usage.LogicalStoredBytes > j.options.MaxStoredBytes {
			return ErrBusy
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if foundLegacy {
		if len(j.sizes) != 0 {
			return ErrRecoveryRequired
		}
		if err := decodeRecord(legacy, &j.state); err != nil {
			return err
		}
		if j.state.Version != 1 {
			return ErrRecoveryRequired
		}
		if err := j.validate(); err != nil {
			return err
		}
		if err := j.prepareMigration(); err != nil {
			return err
		}
		return j.initialize(true)
	}
	if !foundMetadata {
		return ErrRecoveryRequired
	}
	if err := j.validate(); err != nil {
		return err
	}
	j.usage.Records, j.usage.Clusters = len(j.state.Commands), len(j.state.Clusters)
	j.usage.ReservedBytes = metadataReservation
	for id, record := range j.state.Commands {
		size := j.sizes[commandPrefix+id]
		if err := checkCommandStorage(record, size); err != nil {
			return err
		}
		if record.State == Compacted {
			j.usage.ReservedBytes += size
		} else {
			j.usage.ReservedBytes += record.ReservedBytes
		}
	}
	for id, cluster := range j.state.Clusters {
		if err := checkClusterStorage(cluster, j.sizes[clusterPrefix+id]); err != nil {
			return err
		}
		j.usage.ReservedBytes += cluster.ReservedBytes
	}
	if j.usage.ReservedBytes > j.options.MaxStoredBytes || retainedBytes(j.state.Commands) > j.options.MaxRetainedBytes {
		return ErrBusy
	}
	return nil
}

func (j *Journal) prepareMigration() error {
	if len(j.state.Commands) > j.options.MaxRecords || len(j.state.Clusters) > j.options.MaxClusters {
		return ErrBusy
	}
	for id, record := range j.state.Commands {
		record.CommandLimit, record.ResultLimit = j.options.MaxCommandBytes, j.options.MaxResultBytes
		record.ReservedBytes = commandReservation(record.CommandLimit, record.ResultLimit)
		j.state.Commands[id] = record
	}
	for id, cluster := range j.state.Clusters {
		cluster.InventoryLimit = j.options.MaxInventoryBytes
		cluster.ReservedBytes = inventoryReservation(cluster.InventoryLimit)
		j.state.Clusters[id] = cluster
	}
	j.state.Version = 2
	return nil
}

func checkCommandStorage(r commandRecord, size int64) error {
	if r.CommandLimit < 1 || r.ResultLimit < 1 || r.CommandLimit > maxFormatRecordBytes || r.ResultLimit > maxFormatRecordBytes || r.ReservedBytes != commandReservation(r.CommandLimit, r.ResultLimit) || r.ReservedBytes > maxFormatRecordBytes || size > r.ReservedBytes || len(r.Command) > r.CommandLimit || len(r.Result) > r.ResultLimit || proto.Size(&agentv1.BarrierResult{Effects: r.Effects}) > r.ResultLimit {
		return errors.Join(ErrRecoveryRequired, ErrBusy)
	}
	return nil
}

func checkClusterStorage(c clusterState, size int64) error {
	if c.InventoryLimit < 1 || c.InventoryLimit > maxFormatRecordBytes || c.ReservedBytes != inventoryReservation(c.InventoryLimit) || c.ReservedBytes > maxFormatRecordBytes || size > c.ReservedBytes || len(c.Barrier) > c.InventoryLimit {
		return errors.Join(ErrRecoveryRequired, ErrBusy)
	}
	if len(c.Barrier) != 0 {
		var inventory agentv1.BarrierResult
		if proto.Unmarshal(c.Barrier, &inventory) != nil || inventory.Epoch == nil {
			return ErrRecoveryRequired
		}
		inventory.Epoch.ExecutionEpoch = math.MaxInt64
		if proto.Size(&inventory) > c.InventoryLimit {
			return errors.Join(ErrRecoveryRequired, ErrBusy)
		}
	}
	return nil
}

// makeBatch encodes only supplied records. The live maps change after Commit.
func (j *Journal) makeBatch(commands map[string]commandRecord, clusters map[string]clusterState, includeMetadata bool) (*transition, error) {
	t := &transition{batch: j.db.NewBatch(), usage: j.usage, sizes: make(map[string]int64)}
	ok := false
	defer func() {
		if !ok {
			_ = t.batch.Close()
		}
	}()
	if includeMetadata {
		data, err := json.Marshal(metadata{Version: 2, Identity: j.state.Identity, ExecutionBlocked: j.state.ExecutionBlocked})
		if err != nil {
			return nil, err
		}
		size := int64(len(metadataKey) + len(data))
		if size > metadataReservation {
			return nil, ErrRecoveryRequired
		}
		if err := t.batch.Set([]byte(metadataKey), data, nil); err != nil {
			return nil, err
		}
		t.sizes[metadataKey] = size
		t.usage.LogicalStoredBytes += size - j.sizes[metadataKey]
		if j.sizes[metadataKey] == 0 {
			t.usage.ReservedBytes += metadataReservation
		}
	}
	for id, record := range commands {
		key := commandPrefix + id
		stored := storedCommand{Record: record}
		var err error
		stored.Effects, err = proto.Marshal(&agentv1.BarrierResult{Effects: record.Effects})
		if err != nil {
			return nil, err
		}
		stored.Record.Effects = nil
		data, err := json.Marshal(stored)
		if err != nil {
			return nil, err
		}
		size := int64(len(key) + len(data))
		if err := checkCommandStorage(record, size); err != nil {
			return nil, err
		}
		if err := t.batch.Set([]byte(key), data, nil); err != nil {
			return nil, err
		}
		t.sizes[key] = size
		t.usage.LogicalStoredBytes += size - j.sizes[key]
		if j.sizes[key] != 0 {
			old := j.state.Commands[id]
			if old.State == Compacted {
				t.usage.ReservedBytes -= j.sizes[key]
			} else {
				t.usage.ReservedBytes -= old.ReservedBytes
			}
		} else {
			t.usage.Records++
		}
		if record.State == Compacted {
			t.usage.ReservedBytes += size
		} else {
			t.usage.ReservedBytes += record.ReservedBytes
		}
	}
	for id, cluster := range clusters {
		key := clusterPrefix + id
		data, err := json.Marshal(cluster)
		if err != nil {
			return nil, err
		}
		size := int64(len(key) + len(data))
		if err := checkClusterStorage(cluster, size); err != nil {
			return nil, err
		}
		if err := t.batch.Set([]byte(key), data, nil); err != nil {
			return nil, err
		}
		t.sizes[key] = size
		t.usage.LogicalStoredBytes += size - j.sizes[key]
		if j.sizes[key] != 0 {
			t.usage.ReservedBytes -= j.state.Clusters[id].ReservedBytes
		} else {
			t.usage.Clusters++
		}
		t.usage.ReservedBytes += cluster.ReservedBytes
	}
	if t.usage.ReservedBytes > j.options.MaxStoredBytes || t.usage.Records > j.options.MaxRecords || t.usage.Clusters > j.options.MaxClusters {
		return nil, ErrBusy
	}
	t.usage.LastWriteBytes, t.usage.LastWriteRecords = t.batch.Len(), len(t.sizes)
	ok = true
	return t, nil
}

func (j *Journal) apply(t *transition, commands map[string]commandRecord, clusters map[string]clusterState) error {
	defer t.batch.Close()
	if err := t.batch.Commit(pebble.Sync); err != nil {
		j.failed = true
		return fmt.Errorf("persist journal transition: %w", errors.Join(ErrRecoveryRequired, err))
	}
	maps.Copy(j.state.Commands, commands)
	maps.Copy(j.state.Clusters, clusters)
	maps.Copy(j.sizes, t.sizes)
	j.usage = t.usage
	return nil
}

func (j *Journal) persist(commands map[string]commandRecord, clusters map[string]clusterState) error {
	t, err := j.makeBatch(commands, clusters, false)
	if err != nil {
		return err
	}
	return j.apply(t, commands, clusters)
}

func (j *Journal) initialize(migrate bool) error {
	if retainedBytes(j.state.Commands) > j.options.MaxRetainedBytes {
		return ErrBusy
	}
	t, err := j.makeBatch(j.state.Commands, j.state.Clusters, true)
	if err != nil {
		return err
	}
	if migrate {
		if err := t.batch.Delete(stateKey, nil); err != nil {
			_ = t.batch.Close()
			return err
		}
		t.usage.LastWriteBytes, t.usage.LastWriteRecords = t.batch.Len(), t.usage.LastWriteRecords+1
	}
	return j.apply(t, nil, nil)
}

// A rejected native observation must not leave a later execution grant available.
// Metadata has its own reservation, independent of admission capacity.
func (j *Journal) blockExecution(cause error) error {
	if j.failed {
		return errors.Join(ErrRecoveryRequired, cause)
	}
	if !j.state.ExecutionBlocked {
		j.state.ExecutionBlocked = true
		t, err := j.makeBatch(nil, nil, true)
		if err != nil {
			j.failed = true
			return errors.Join(ErrRecoveryRequired, cause, err)
		}
		if err := j.apply(t, nil, nil); err != nil {
			return errors.Join(cause, err)
		}
	}
	return errors.Join(ErrRecoveryRequired, cause)
}

// Usage reports the current record counts and byte totals against their bounds.
func (j *Journal) Usage() (Usage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return Usage{}, err
	}
	u := j.usage
	u.PhysicalDiskBytes = j.db.Metrics().DiskSpaceUsage()
	u.ExecutionBlocked = j.state.ExecutionBlocked
	return u, nil
}
