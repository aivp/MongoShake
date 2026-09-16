package tunnel

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

type criticalRecoverySink struct {
	calls   int
	err     error
	started chan struct{}
	release chan struct{}
	ended   chan struct{}
}

func (s *criticalRecoverySink) WriteBatch(batch [][]byte) error {
	s.calls++
	if s.started != nil {
		close(s.started)
		<-s.release
		defer close(s.ended)
	}
	return s.err
}
func (*criticalRecoverySink) MaxMessageBytes() int { return 18 * 1024 * 1024 }
func (*criticalRecoverySink) Close() error         { return nil }

func TestCriticalAcknowledgedDeadlineRejectsLateACK(t *testing.T) {
	s := &criticalRecoverySink{started: make(chan struct{}), release: make(chan struct{}), ended: make(chan struct{})}
	w := &AcknowledgedKafkaWriter{sink: s, maxMessages: 2, maxBytes: 524288, sendTimeout: 20 * time.Millisecond}
	m := criticalAckMessage(1, 4)
	start := time.Now()
	if w.Send(m) != ReplyFenced || time.Since(start) > time.Second {
		close(s.release)
		t.Fatal("blocked producer did not reach its deadline")
	}
	status := w.DeliveryStatus()
	if status.Failure == nil || status.Failure.Category != "delivery_timeout" || status.LastBrokerACK != 0 || status.ConfirmedRecords != 0 {
		close(s.release)
		t.Fatalf("timeout status=%+v", status)
	}
	close(s.release)
	<-s.ended
	if w.Send(m) != ReplyFenced || w.Send(criticalAckMessage(5, 1)) != ReplyFenced || s.calls != 1 {
		t.Fatal("late completion allowed a retry or a later batch")
	}
	status = w.DeliveryStatus()
	if status.LastBrokerACK != 0 || status.ConfirmedRecords != 0 {
		t.Fatal("late broker response advanced ACK")
	}
}

func TestCriticalAcknowledgedFirstFailureIsStableAndConcurrent(t *testing.T) {
	s := &criticalRecoverySink{err: sarama.ProducerErrors{&sarama.ProducerError{Err: sarama.ErrRequestTimedOut}}}
	w := &AcknowledgedKafkaWriter{sink: s, maxMessages: 128, maxBytes: 524288}
	if w.Send(criticalAckMessage(1, 3)) != ReplyFenced {
		t.Fatal("expected delivery failure")
	}
	first, _ := json.Marshal(w.DeliveryStatus().Failure)
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 100; j++ {
				status := w.DeliveryStatus()
				if status.Failure == nil || status.Failure.Category != "ambiguous" || status.Failure.WorkerRecords != 3 {
					t.Error("first cause missing")
				}
				_, _ = json.Marshal(status)
			}
		}()
	}
	for i := 0; i < 10; i++ {
		if w.Send(criticalAckMessage(4, 1)) != ReplyFenced {
			t.Fatal("later send was accepted")
		}
	}
	readers.Wait()
	last, _ := json.Marshal(w.DeliveryStatus().Failure)
	if string(first) != string(last) || s.calls != 1 {
		t.Fatal("first cause was overwritten or failed batch replayed")
	}
}
