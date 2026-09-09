package tunnel

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type criticalAckSink struct {
	batches  [][][]byte
	started  chan struct{}
	release  chan struct{}
	failCall int
	closed   bool
}

func (s *criticalAckSink) WriteBatch(batch [][]byte) error {
	copyBatch := make([][]byte, len(batch))
	for i, payload := range batch {
		copyBatch[i] = append([]byte(nil), payload...)
	}
	s.batches = append(s.batches, copyBatch)
	if s.started != nil {
		s.started <- struct{}{}
		<-s.release
	}
	if len(s.batches) == s.failCall {
		return errors.New("uncertain delivery after a possible partial append")
	}
	return nil
}
func (*criticalAckSink) MaxMessageBytes() int { return 18 * 1024 * 1024 }
func (s *criticalAckSink) Close() error       { s.closed = true; return nil }

func criticalAckMessage(start, count int) *WMessage {
	m := &WMessage{TMessage: &TMessage{}}
	for i := start; i < start+count; i++ {
		p := &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Timestamp: primitive.Timestamp{T: 100, I: uint32(i)}, Operation: "u", Namespace: "ds_production.CarrierDevice",
			Query: bson.D{{Key: "_id", Value: "binding-A"}}, Object: bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: i % 2}}}},
		}}
		m.ParsedLogs = append(m.ParsedLogs, p)
		raw, _ := bson.Marshal(p.ParsedLog)
		m.RawLogs = append(m.RawLogs, raw)
	}
	return m
}

func criticalAckWriter(s *criticalAckSink, max int) *AcknowledgedKafkaWriter {
	return &AcknowledgedKafkaWriter{sink: s, maxMessages: max, maxBytes: 512 * 1024}
}

func TestCriticalAcknowledgedWaitsForBroker(t *testing.T) {
	s := &criticalAckSink{started: make(chan struct{}, 1), release: make(chan struct{})}
	w := criticalAckWriter(s, 128)
	m := criticalAckMessage(1, 3)
	done := make(chan int64, 1)
	go func() { done <- w.Send(m) }()
	select {
	case <-s.started:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}
	status := w.DeliveryStatus()
	if status.LastBrokerACK != 0 || status.InFlightRecords != 3 || status.ConfirmedRecords != 0 {
		close(s.release)
		<-done
		t.Fatalf("premature success: %+v", status)
	}
	select {
	case <-done:
		close(s.release)
		t.Fatal("Send completed before Kafka ACK")
	default:
	}
	close(s.release)
	if ack := <-done; ack != utils.TimeStampToInt64(m.ParsedLogs[2].Timestamp) {
		t.Fatalf("ack=%d", ack)
	}
	if status = w.DeliveryStatus(); status.ConfirmedRecords != 3 || status.InFlightRecords != 0 || status.Fenced {
		t.Fatalf("status=%+v", status)
	}
}

func TestCriticalAcknowledgedBatchAndPayload(t *testing.T) {
	for _, max := range []int{1, 2, 128} {
		for _, format := range []string{"", "canonical_extended_json"} {
			s := &criticalAckSink{}
			w := criticalAckWriter(s, max)
			w.jsonFormat = format
			m := criticalAckMessage(1, 7)
			if w.Send(m) <= 0 {
				t.Fatal("send failed")
			}
			if len(s.batches) != (7+max-1)/max {
				t.Fatalf("max=%d batches=%d", max, len(s.batches))
			}
			index := 0
			for _, batch := range s.batches {
				for _, actual := range batch {
					var expected []byte
					if format == "" {
						expected, _ = json.Marshal(m.ParsedLogs[index].ParsedLog)
					} else {
						expected, _ = bson.MarshalExtJSON(m.ParsedLogs[index].ParsedLog, true, true)
					}
					if !bytes.Equal(actual, expected) {
						t.Fatalf("wire payload changed at %d", index)
					}
					index++
				}
			}
		}
	}
}

func TestCriticalAcknowledgedFailureFencesReplayAndLaterBatches(t *testing.T) {
	s := &criticalAckSink{}
	w := criticalAckWriter(s, 2)
	initial := criticalAckMessage(1, 1)
	previous := w.Send(initial)
	s.failCall = 3 // first chunk succeeds; second chunk's result is uncertain.
	m := criticalAckMessage(2, 5)
	if w.Send(m) != ReplyFenced {
		t.Fatal("expected terminal delivery fence")
	}
	for _, retry := range []*WMessage{m, criticalAckMessage(7, 1), {TMessage: &TMessage{Tag: MsgProbe}}} {
		if w.Send(retry) != ReplyFenced {
			t.Fatal("fence did not reject retry/probe/later event")
		}
	}
	status := w.DeliveryStatus()
	if len(s.batches) != 3 || status.LastBrokerACK != previous || !status.Fenced || status.ConfirmedRecords != 3 {
		t.Fatalf("unsafe partial result: calls=%d status=%+v", len(s.batches), status)
	}
}

func TestCriticalAcknowledgedLargeRecordAndByteBounds(t *testing.T) {
	s := &criticalAckSink{}
	w := criticalAckWriter(s, 128)
	w.maxBytes = 1024
	m := criticalAckMessage(1, 3)
	m.ParsedLogs[1].Object = bson.D{{Key: "large", Value: strings.Repeat("x", 1024*1024)}}
	if w.Send(m) <= 0 {
		t.Fatal("supported large record must not be rejected by the batch target")
	}
	if len(s.batches) != 3 || len(s.batches[1]) != 1 || len(s.batches[1][0]) < 1024*1024 {
		t.Fatalf("batches=%d", len(s.batches))
	}

	s = &criticalAckSink{}
	w = criticalAckWriter(s, 128)
	w.maxBytes = 450
	if w.Send(criticalAckMessage(1, 7)) <= 0 {
		t.Fatal("byte-bounded send failed")
	}
	for _, batch := range s.batches {
		n := 0
		for _, payload := range batch {
			n += len(payload)
		}
		if n > w.maxBytes && len(batch) != 1 {
			t.Fatalf("batch bytes=%d", n)
		}
	}
}

func TestCriticalAcknowledgedInvalidRecordsNeverSkipped(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*WMessage)
	}{
		{"nan_standard_json", func(m *WMessage) { m.ParsedLogs[0].Object = bson.D{{Key: "value", Value: math.NaN()}} }},
		{"nil_parsed", func(m *WMessage) { m.ParsedLogs[0] = nil }},
		{"zero_timestamp", func(m *WMessage) { m.ParsedLogs[0].Timestamp = primitive.Timestamp{} }},
		{"out_of_order", func(m *WMessage) { m.ParsedLogs[0], m.ParsedLogs[1] = m.ParsedLogs[1], m.ParsedLogs[0] }},
		{"missing_raw", func(m *WMessage) { m.RawLogs = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &criticalAckSink{}
			w := criticalAckWriter(s, 128)
			m := criticalAckMessage(1, 2)
			tc.mutate(m)
			if w.Send(m) != ReplyFenced || len(s.batches) != 0 || w.DeliveryStatus().LastBrokerACK != 0 {
				t.Fatal("invalid message was accepted or skipped")
			}
		})
	}
	t.Run("canonical_nan_preserved", func(t *testing.T) {
		w := criticalAckWriter(&criticalAckSink{}, 128)
		w.jsonFormat = "canonical_extended_json"
		m := criticalAckMessage(1, 1)
		m.ParsedLogs[0].Object = bson.D{{Key: "value", Value: math.NaN()}}
		if w.Send(m) <= 0 {
			t.Fatal("canonical extended JSON supports NaN")
		}
	})
}

func TestCriticalAcknowledgedCloseAndProbe(t *testing.T) {
	s := &criticalAckSink{}
	w := criticalAckWriter(s, 1)
	ack := w.Send(criticalAckMessage(1, 1))
	if w.Send(&WMessage{TMessage: &TMessage{Tag: MsgProbe}}) != ack || len(s.batches) != 1 {
		t.Fatal("probe changed delivery")
	}
	if err := w.Close(); err != nil || !s.closed {
		t.Fatal("close failed")
	}
	if w.Send(criticalAckMessage(2, 1)) != ReplyFenced || len(s.batches) != 1 {
		t.Fatal("closed writer sent data")
	}
}
