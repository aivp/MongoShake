package tunnel

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/tunnel/kafka"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
)

type acknowledgedKafkaSink interface {
	WriteBatch([][]byte) error
	MaxMessageBytes() int
	Close() error
}

// AcknowledgedKafkaWriter deliberately has no detached delivery queue. Send
// completes only after the entire worker message has been accepted by Kafka.
type AcknowledgedKafkaWriter struct {
	// Atomic fields must stay naturally aligned for 32-bit builds.
	lastAck, inFlight, confirmedRecords, confirmedBatches int64
	RemoteAddr                                            string
	PartitionId                                           int
	fenced                                                int32
	sendMu                                                sync.Mutex
	sink                                                  acknowledgedKafkaSink
	jsonFormat                                            string
	maxMessages, maxBytes                                 int
	sendTimeout                                           time.Duration
	failure                                               atomic.Value
	workerFirst, workerLast                               int64
	workerRecords                                         int
}

type KafkaFailureStatus struct {
	kafka.DeliveryFailure
	At            string `json:"at"`
	WorkerFirst   int64  `json:"worker_first_oplog"`
	WorkerLast    int64  `json:"worker_last_oplog"`
	WorkerRecords int    `json:"worker_records"`
	LastBrokerACK int64  `json:"last_broker_ack"`
}

type KafkaDeliveryStatus struct {
	Acknowledged     bool                `json:"acknowledged"`
	LastBrokerACK    int64               `json:"last_broker_ack"`
	InFlightRecords  int64               `json:"in_flight_records"`
	ConfirmedRecords int64               `json:"confirmed_records"`
	ConfirmedBatches int64               `json:"confirmed_batches"`
	Fenced           bool                `json:"fenced"`
	Failure          *KafkaFailureStatus `json:"failure,omitempty"`
}

func (w *AcknowledgedKafkaWriter) DeliveryStatus() KafkaDeliveryStatus {
	status := KafkaDeliveryStatus{
		Acknowledged: true, LastBrokerACK: atomic.LoadInt64(&w.lastAck),
		InFlightRecords:  atomic.LoadInt64(&w.inFlight),
		ConfirmedRecords: atomic.LoadInt64(&w.confirmedRecords),
		ConfirmedBatches: atomic.LoadInt64(&w.confirmedBatches), Fenced: atomic.LoadInt32(&w.fenced) != 0,
	}
	if failure := w.failure.Load(); failure != nil {
		copy := failure.(KafkaFailureStatus)
		copy.Errors = append([]kafka.DeliveryErrorDetail(nil), copy.Errors...)
		status.Failure = &copy
	}
	return status
}

func (*AcknowledgedKafkaWriter) Name() string             { return "kafka" }
func (*AcknowledgedKafkaWriter) AckRequired() bool        { return true }
func (*AcknowledgedKafkaWriter) ParsedLogsRequired() bool { return true }
func (w *AcknowledgedKafkaWriter) Fenced() bool           { return atomic.LoadInt32(&w.fenced) != 0 }

func (w *AcknowledgedKafkaWriter) Prepare() bool {
	c := conf.Options
	if !c.KafkaAcknowledged {
		return false
	}
	if err := c.NormalizeKafkaDelivery(); err != nil {
		LOG.Error("acknowledged Kafka configuration: %v", err)
		return false
	}
	w.maxMessages, w.maxBytes, w.jsonFormat = c.KafkaBatchMaxMessages, c.KafkaBatchMaxBytes, c.TunnelJsonFormat
	w.sendTimeout = time.Duration(c.KafkaSendTimeoutMS) * time.Millisecond
	flush := time.Duration(c.KafkaBatchFlushMS) * time.Millisecond
	if !c.KafkaBatchEnabled {
		w.maxMessages, flush = 1, 0
	}
	sink, err := kafka.NewAcknowledgedSyncWriter(c.TunnelMongoSslRootCaFile, w.RemoteAddr, w.PartitionId, w.maxMessages, flush)
	if err != nil {
		LOG.Error("acknowledged Kafka create: %v", err)
		return false
	}
	if err := sink.Start(); err != nil {
		LOG.Error("acknowledged Kafka start: %v", err)
		return false
	}
	w.sink = sink
	LOG.Info("acknowledged Kafka enabled: partition=%d batch=%t max_messages=%d max_bytes=%d flush_ms=%d", w.PartitionId, c.KafkaBatchEnabled, w.maxMessages, w.maxBytes, flush/time.Millisecond)
	return true
}

func (w *AcknowledgedKafkaWriter) fence(reason error, category string) int64 {
	if w.failure.Load() == nil {
		details := kafka.DescribeDeliveryError(reason)
		if category != "delivery" {
			details.Category, details.Retryable = category, category == "delivery_timeout"
			// These reasons are generated locally and contain no oplog payload.
			details.Errors = []kafka.DeliveryErrorDetail{{Type: category, Message: reason.Error(), Partition: int32(w.PartitionId)}}
		}
		failure := KafkaFailureStatus{DeliveryFailure: details, At: time.Now().UTC().Format(time.RFC3339Nano),
			WorkerFirst: w.workerFirst, WorkerLast: w.workerLast, WorkerRecords: w.workerRecords,
			LastBrokerACK: atomic.LoadInt64(&w.lastAck)}
		w.failure.Store(failure)
		encoded, _ := json.Marshal(failure)
		// Preserve the first cause even with file-only or buffered application logging.
		fmt.Fprintf(os.Stderr, "acknowledged Kafka FENCED: %s\n", encoded)
	}
	atomic.StoreInt32(&w.fenced, 1)
	return ReplyFenced
}

func (w *AcknowledgedKafkaWriter) Send(message *WMessage) int64 {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if atomic.LoadInt32(&w.fenced) != 0 {
		return ReplyFenced
	}
	w.workerFirst, w.workerLast, w.workerRecords = 0, 0, 0
	if message == nil || message.TMessage == nil {
		return w.fence(fmt.Errorf("missing worker message"), "invalid_input")
	}
	w.workerFirst, w.workerLast, w.workerRecords = 0, 0, len(message.ParsedLogs)
	for _, log := range message.ParsedLogs {
		if log != nil {
			ts := utils.TimeStampToInt64(log.Timestamp)
			if w.workerFirst == 0 {
				w.workerFirst = ts
			}
			w.workerLast = ts
		}
	}
	if message.Tag&MsgProbe != 0 && (len(message.RawLogs) != 0 || len(message.ParsedLogs) != 0) {
		return w.fence(fmt.Errorf("probe unexpectedly contains oplog records"), "invalid_input")
	}
	if message.Tag&MsgProbe != 0 || len(message.RawLogs) == 0 && len(message.ParsedLogs) == 0 {
		return atomic.LoadInt64(&w.lastAck)
	}
	if w.sink == nil || w.maxMessages < 1 || w.maxBytes < 1 || len(message.RawLogs) != len(message.ParsedLogs) {
		return w.fence(fmt.Errorf("unprepared writer or mismatched raw/parsed records"), "invalid_input")
	}
	previous := atomic.LoadInt64(&w.lastAck)
	for _, log := range message.ParsedLogs {
		if log == nil {
			return w.fence(fmt.Errorf("missing parsed oplog"), "invalid_input")
		}
		ts := utils.TimeStampToInt64(log.Timestamp)
		if ts <= 0 || ts < previous {
			return w.fence(fmt.Errorf("invalid or regressing oplog timestamp: current=%d previous=%d", ts, previous), "invalid_input")
		}
		previous = ts
	}
	atomic.StoreInt64(&w.inFlight, int64(len(message.ParsedLogs)))
	defer atomic.StoreInt64(&w.inFlight, 0)
	batch := make([][]byte, 0, w.maxMessages)
	batchBytes := 0
	timeout := w.sendTimeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	deadline := time.Now().Add(timeout)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		// Sarama may have delivered a prefix even when it returns an error.
		// Never retry the whole application batch or advance its checkpoint.
		if err := w.writeBatchBefore(batch, deadline); err != nil {
			return err
		}
		atomic.AddInt64(&w.confirmedRecords, int64(len(batch)))
		atomic.AddInt64(&w.confirmedBatches, 1)
		for i := range batch {
			batch[i] = nil
		}
		batch, batchBytes = batch[:0], 0
		return nil
	}
	for _, log := range message.ParsedLogs {
		var payload []byte
		var err error
		if w.jsonFormat == "canonical_extended_json" {
			payload, err = bson.MarshalExtJSON(log.ParsedLog, true, true)
		} else if w.jsonFormat == "" {
			payload, err = json.Marshal(log.ParsedLog)
		} else {
			err = fmt.Errorf("unsupported JSON format")
		}
		if err != nil {
			return w.fence(fmt.Errorf("oplog encoding failed (%T)", err), "encoding")
		}
		// This is the existing per-record limit, not the batch byte target.
		// Sarama performs its additional wire-envelope size validation too.
		if len(payload) > w.sink.MaxMessageBytes() {
			return w.fence(fmt.Errorf("encoded oplog exceeds configured Kafka message size limit"), "message_too_large")
		}
		if len(batch) > 0 && (len(batch) == w.maxMessages || len(payload) > w.maxBytes-batchBytes) {
			if err := flush(); err != nil {
				return w.fenceDelivery(err)
			}
		}
		batch = append(batch, payload)
		batchBytes += len(payload)
		if len(batch) == w.maxMessages || batchBytes >= w.maxBytes {
			if err := flush(); err != nil {
				return w.fenceDelivery(err)
			}
		}
	}
	if err := flush(); err != nil {
		return w.fenceDelivery(err)
	}
	atomic.StoreInt64(&w.lastAck, previous)
	return previous
}

var errKafkaDeliveryDeadline = fmt.Errorf("worker batch delivery deadline exceeded; append outcome unknown")

func (w *AcknowledgedKafkaWriter) fenceDelivery(err error) int64 {
	if err == errKafkaDeliveryDeadline {
		return w.fence(err, "delivery_timeout")
	}
	return w.fence(err, "delivery")
}

func (w *AcknowledgedKafkaWriter) writeBatchBefore(batch [][]byte, deadline time.Time) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return errKafkaDeliveryDeadline
	}
	result := make(chan error, 1)
	sink := w.sink
	go func() { result <- sink.WriteBatch(batch) }()
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		// The request may still finish; fence and let the collector exit instead of
		// submitting another batch or advancing the checkpoint from a late result.
		return errKafkaDeliveryDeadline
	}
}

// Close serializes with Send; it cannot acknowledge a failed/uncertain batch.
func (w *AcknowledgedKafkaWriter) Close() error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	atomic.StoreInt32(&w.fenced, 1)
	if w.sink == nil {
		return nil
	}
	err := w.sink.Close()
	w.sink = nil
	return err
}
