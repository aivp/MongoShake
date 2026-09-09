package ckpt

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	utils "github.com/alibaba/MongoShake/v2/common"
)

type criticalCheckpointStore struct {
	value    CheckpointContext
	fail     bool
	attempts int
}

func TestCriticalCheckpointAPIRejectsHTTPFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "injected failure", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	store := &HttpApiCheckpoint{URL: server.URL}
	if err := store.Insert(&CheckpointContext{Name: "local-test", Timestamp: 200}); err == nil {
		t.Fatal("non-200 checkpoint response reported success")
	}
	if got, _ := store.Get(); got != nil {
		t.Fatal("non-200 checkpoint response was accepted")
	}
}

func (s *criticalCheckpointStore) Get() (*CheckpointContext, bool) {
	value := s.value
	return &value, true
}
func (s *criticalCheckpointStore) Insert(value *CheckpointContext) error {
	s.attempts++
	if s.fail {
		return errors.New("injected checkpoint persistence failure")
	}
	s.value = *value
	return nil
}
func (*criticalCheckpointStore) String() string { return "test-only checkpoint store" }

func TestCriticalCheckpointFailedPersistenceRemainsRetryable(t *testing.T) {
	store := &criticalCheckpointStore{value: CheckpointContext{Name: "local-critical", Timestamp: 1, Version: utils.FcvCheckpoint.CurrentVersion}, fail: true}
	manager := &CheckpointManager{delegate: store}
	if _, _, err := manager.Get(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Update(200); err == nil {
		t.Fatal("expected persistence failure")
	}
	if manager.GetInMemory().Timestamp != 1 || store.value.Timestamp != 1 {
		t.Fatal("failed persistence advanced checkpoint")
	}
	store.fail = false
	if err := manager.Update(200); err != nil {
		t.Fatal(err)
	}
	if manager.GetInMemory().Timestamp != 200 || store.value.Timestamp != 200 || store.attempts != 2 {
		t.Fatal("same-position retry failed")
	}
	restarted := &CheckpointManager{delegate: store}
	got, _, err := restarted.Get()
	if err != nil || got.Timestamp != 200 {
		t.Fatal("restart did not load persisted checkpoint")
	}
}
