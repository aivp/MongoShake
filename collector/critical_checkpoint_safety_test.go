package collector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

// Uses the real CheckpointManager with an isolated loopback HTTP backing store.
func criticalCheckpointFixture(t *testing.T) (*OplogSyncer, func() int64) {
	t.Helper()
	old := conf.Options
	t.Cleanup(func() { conf.Options = old })
	var mu sync.Mutex
	value := ckpt.CheckpointContext{Name: "local-critical-test", Timestamp: 1, Version: utils.FcvCheckpoint.CurrentVersion}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodPost {
			if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
				http.Error(w, "bad checkpoint", 400)
				return
			}
		}
		json.NewEncoder(w).Encode(value)
	}))
	t.Cleanup(server.Close)
	conf.Options = conf.Configuration{KafkaAcknowledged: true, CheckpointStorage: utils.VarCheckpointStorageApi, CheckpointStorageCollection: server.URL}
	s := mockCheckpointSyncer(1)
	s.replMetric = &utils.ReplicationMetric{}
	s.startTime = time.Now().Add(-time.Hour)
	s.ckptManager = ckpt.NewCheckpointManager("local-critical-test", 1)
	if _, _, err := s.ckptManager.Get(); err != nil {
		t.Fatal(err)
	}
	return s, func() int64 { mu.Lock(); defer mu.Unlock(); return value.Timestamp }
}

func TestCriticalFilteredCheckpointWaitsForEarlierKafkaACK(t *testing.T) {
	s, persisted := criticalCheckpointFixture(t)
	w := s.batcher.workerGroup[0]
	atomic.StoreInt64(&w.ack, 100)
	atomic.StoreInt64(&w.unack, 200)
	s.checkpoint(true, 300)
	if persisted() != 1 {
		t.Fatal("filtered tail skipped unacknowledged Kafka records")
	}
	s.checkpoint(true, 0)
	if persisted() != 100 {
		t.Fatal("confirmed prefix was not checkpointed")
	}
	atomic.StoreInt64(&w.ack, 200)
	s.checkpoint(true, 300)
	if persisted() != 300 {
		t.Fatal("filtered tail did not advance after real ACK")
	}
	restarted := ckpt.NewCheckpointManager("local-critical-test", 1)
	got, _, err := restarted.Get()
	if err != nil || got.Timestamp != 300 {
		t.Fatal("restart position did not match persisted position")
	}
}

func TestCriticalShutdownDrainsBeforeCheckpoint(t *testing.T) {
	s, persisted := criticalCheckpointFixture(t)
	w := s.batcher.workerGroup[0]
	atomic.StoreInt64(&w.ack, 100)
	atomic.StoreInt64(&w.unack, 200)
	done := make(chan struct{})
	go func() { s.drainAcknowledgedCheckpoint(300); close(done) }()
	deadline := time.Now().Add(time.Second)
	for persisted() == 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if persisted() != 100 {
		atomic.StoreInt64(&w.ack, 200)
		<-done
		t.Fatal("shutdown advanced beyond real ACK")
	}
	select {
	case <-done:
		t.Fatal("shutdown completed while Kafka records were pending")
	default:
	}
	atomic.StoreInt64(&w.ack, 200)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not finish")
	}
	if persisted() != 300 {
		t.Fatal("drained checkpoint was not persisted")
	}
}

func TestCriticalShutdownFilteredOnlyTail(t *testing.T) {
	s, persisted := criticalCheckpointFixture(t)
	s.drainAcknowledgedCheckpoint(300)
	if persisted() != 300 {
		t.Fatal("filtered-only shutdown could not checkpoint")
	}
}

func TestCriticalPendingEqualTimestampCannotAdvanceFilteredCheckpoint(t *testing.T) {
	s, persisted := criticalCheckpointFixture(t)
	w := s.batcher.workerGroup[0]
	atomic.StoreInt64(&w.ack, 100)
	atomic.StoreInt64(&w.unack, 100)
	atomic.StoreInt64(&w.pendingKafkaBatches, 1)
	s.checkpoint(true, 300)
	if persisted() != 1 || !s.hasUnacknowledgedWork() {
		t.Fatal("equal timestamps hid a pending worker message")
	}
	atomic.StoreInt64(&w.pendingKafkaBatches, 0)
	s.checkpoint(true, 300)
	if persisted() != 300 {
		t.Fatal("confirmed duplicate blocked filtered progress")
	}
}
