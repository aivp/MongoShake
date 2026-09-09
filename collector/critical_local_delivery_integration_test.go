package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/tunnel"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const criticalLocalBroker = "127.0.0.1:39092"

func criticalLocalTopic(t *testing.T, maxBytes string) string {
	t.Helper()
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_0
	config.Net.DialTimeout = 2 * time.Second
	admin, err := sarama.NewClusterAdmin([]string{criticalLocalBroker}, config)
	if err != nil {
		t.Fatal(err)
	}
	topic := fmt.Sprintf("codex-critical-production-path-%d", time.Now().UnixNano())
	detail := &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}
	if maxBytes != "" {
		detail.ConfigEntries = map[string]*string{"max.message.bytes": &maxBytes}
	}
	if err := admin.CreateTopic(topic, detail, false); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.DeleteTopic(topic); err != nil {
			t.Errorf("local topic cleanup: %v", err)
		}
		admin.Close()
	})
	return topic
}

type criticalExitAfterDeliveryWriter struct{ tunnel.Writer }

func (w *criticalExitAfterDeliveryWriter) Send(message *tunnel.WMessage) int64 {
	ack := w.Writer.Send(message)
	if ack > 0 && len(message.RawLogs) > 0 {
		os.Exit(23)
	}
	return ack
}

func TestCriticalLocalCrashAfterBrokerACKAndRestart(t *testing.T) {
	if os.Getenv("CRITICAL_LOCAL_KAFKA_TEST") != "1" {
		t.Skip("opt-in isolated local Kafka only")
	}
	if action := os.Getenv("CRITICAL_LOCAL_CRASH_CHILD"); action != "" {
		if action != "crash" && action != "recover" {
			t.Fatal("invalid local child action")
		}
		topic, storeURL := os.Getenv("CRITICAL_LOCAL_CRASH_TOPIC"), os.Getenv("CRITICAL_LOCAL_CRASH_STORE")
		u, err := url.Parse(storeURL)
		if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(topic, "codex-critical-production-path-") {
			t.Fatal("child targets must be isolated local test resources")
		}
		w, _ := criticalLocalWorker(t, topic, true, 128)
		conf.Options.CheckpointStorageCollection = storeURL
		w.syncer.ckptManager = ckpt.NewCheckpointManager("local-critical-test", 1)
		if _, _, err := w.syncer.ckptManager.Get(); err != nil {
			t.Fatal(err)
		}
		if action == "crash" {
			w.writeController.tunnel = &criticalExitAfterDeliveryWriter{Writer: w.writeController.tunnel}
		}
		logs := criticalLocalLogs(1, 3)
		w.Offer(logs)
		w.transfer(<-w.queue)
		w.syncer.checkpoint(true, 0)
		if action == "crash" {
			t.Fatal("expected process exit immediately after real Kafka ACK")
		}
		return
	}
	topic := criticalLocalTopic(t, "")
	s, persisted := criticalCheckpointFixture(t)
	storeURL := conf.Options.CheckpointStorageCollection
	run := func(action string) (int, string) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCriticalLocalCrashAfterBrokerACKAndRestart$", "-test.timeout=18s")
		cmd.Env = append(os.Environ(), "CRITICAL_LOCAL_CRASH_CHILD="+action, "CRITICAL_LOCAL_CRASH_TOPIC="+topic, "CRITICAL_LOCAL_CRASH_STORE="+storeURL)
		output, err := cmd.CombinedOutput()
		if err == nil {
			return 0, string(output)
		}
		if exit, ok := err.(*exec.ExitError); ok {
			return exit.ExitCode(), string(output)
		}
		t.Fatalf("child execution: %v %s", err, output)
		return -1, ""
	}
	if code, output := run("crash"); code != 23 {
		t.Fatalf("expected deliberate process exit 23, got %d: %s", code, output)
	}
	logs := criticalLocalLogs(1, 3)
	criticalAssertLocalRecords(t, topic, logs)
	if persisted() != 1 {
		t.Fatal("crash after broker ACK incorrectly advanced durable checkpoint")
	}
	if code, output := run("recover"); code != 0 {
		t.Fatalf("restart failed: %d %s", code, output)
	}
	criticalAssertLocalRecords(t, topic, append(logs, logs...))
	if persisted() != utils.TimeStampToInt64(logs[2].Parsed.Timestamp) {
		t.Fatal("restart did not checkpoint the complete replayed interval")
	}
	got, _, err := s.ckptManager.Get()
	if err != nil || got.Timestamp != persisted() {
		t.Fatal("restart checkpoint could not be reloaded")
	}
	t.Log("REAL subprocess exited after Kafka ACK but before controller/checkpoint advance: restart used old checkpoint, replayed all records without gaps; duplicates remain a CFS acceptance requirement")
}

func criticalLocalWorker(t *testing.T, topic string, batching bool, maxMessages int) (*Worker, func() int64) {
	t.Helper()
	s, persisted := criticalCheckpointFixture(t)
	conf.Options.Tunnel, conf.Options.TunnelMessage, conf.Options.SyncMode = "kafka", "json", "incr"
	conf.Options.TunnelAddress = []string{topic + "@" + criticalLocalBroker}
	conf.Options.MongoUrls = []string{"mongodb://unused-local-test-source"}
	conf.Options.IncrSyncWorker, conf.Options.IncrSyncTunnelWriteThread, conf.Options.TunnelKafkaPartitionNumber = 1, 1, 1
	conf.Options.IncrSyncWorkerBatchQueueSize = 64
	conf.Options.IncrSyncMongoFetchMethod, conf.Options.IncrSyncWorkerOplogCompressor = "oplog", "none"
	conf.Options.KafkaBatchEnabled, conf.Options.KafkaBatchMaxMessages = batching, maxMessages
	if err := conf.Options.NormalizeKafkaDelivery(); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(s, 0)
	s.batcher.workerGroup = []*Worker{w}
	w.writeController = NewWriteController(w)
	if w.writeController == nil {
		t.Fatal("actual production writer did not initialize")
	}
	writer, ok := w.writeController.tunnel.(*tunnel.AcknowledgedKafkaWriter)
	if !ok {
		t.Fatal("factory did not select acknowledged writer")
	}
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	})
	return w, persisted
}

func criticalLocalLogs(start, count int) []*oplog.GenericOplog {
	logs := make([]*oplog.GenericOplog, count)
	for i := range logs {
		logs[i] = &oplog.GenericOplog{Parsed: &oplog.PartialLog{ParsedLog: oplog.ParsedLog{
			Timestamp: primitive.Timestamp{T: 100, I: uint32(start + i)}, Operation: "u", Namespace: "ds_production.CarrierDevice",
			Query:  bson.D{{Key: "_id", Value: "binding-A"}},
			Object: bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: i % 2}, {Key: "payload", Value: strings.Repeat("x", 512)}}}},
		}}}
	}
	return logs
}

func criticalAssertLocalRecords(t *testing.T, topic string, logs []*oplog.GenericOplog) {
	t.Helper()
	c := sarama.NewConfig()
	c.Version = sarama.V0_11_0_0
	client, err := sarama.NewClient([]string{criticalLocalBroker}, c)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	end, err := client.GetOffset(topic, 0, sarama.OffsetNewest)
	if err != nil || end != int64(len(logs)) {
		t.Fatalf("Kafka records=%d want=%d err=%v", end, len(logs), err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	partition, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	defer partition.Close()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for i, log := range logs {
		select {
		case msg := <-partition.Messages():
			expected, _ := json.Marshal(log.Parsed.ParsedLog)
			if msg == nil || msg.Offset != int64(i) || !bytes.Equal(msg.Value, expected) {
				t.Fatalf("payload/order mismatch at %d", i)
			}
		case <-timer.C:
			t.Fatalf("consumer timed out at %d", i)
		}
	}
}

func TestCriticalLocalProductionWorkerAndCheckpoint(t *testing.T) {
	if os.Getenv("CRITICAL_LOCAL_KAFKA_TEST") != "1" {
		t.Skip("opt-in isolated local Kafka only")
	}
	for _, batching := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%t", batching), func(t *testing.T) {
			topic := criticalLocalTopic(t, "")
			w, persisted := criticalLocalWorker(t, topic, batching, 128)
			logs := criticalLocalLogs(1, 1024)
			w.Offer(logs)
			w.syncer.checkpoint(true, 0)
			if persisted() != 1 {
				t.Fatal("queued worker batch advanced checkpoint")
			}
			start := time.Now()
			w.transfer(<-w.queue)
			elapsed := time.Since(start)
			want := utils.TimeStampToInt64(logs[len(logs)-1].Parsed.Timestamp)
			if atomic.LoadInt64(&w.ack) != want || len(w.listUnACK) != 0 {
				t.Fatal("actual worker ACK did not complete")
			}
			w.syncer.checkpoint(true, 0)
			if persisted() != want {
				t.Fatal("actual worker checkpoint mismatch")
			}
			criticalAssertLocalRecords(t, topic, logs)
			t.Logf("REAL production factory/controller/worker/Kafka path: batch=%t records=1024 elapsed=%s; ordered payloads and persisted checkpoint match", batching, elapsed)
		})
	}
}

func TestCriticalLocalBrokerRejectionFencesWithoutCheckpointAdvance(t *testing.T) {
	if os.Getenv("CRITICAL_LOCAL_KAFKA_TEST") != "1" {
		t.Skip("opt-in isolated local Kafka only")
	}
	topic := criticalLocalTopic(t, "8192")
	w, persisted := criticalLocalWorker(t, topic, true, 2)
	initial := criticalLocalLogs(1, 1)
	w.Offer(initial)
	w.transfer(<-w.queue)
	w.syncer.checkpoint(true, 0)
	previous := persisted()
	logs := criticalLocalLogs(2, 5)
	logs[2].Parsed.Object = bson.D{{Key: "large", Value: strings.Repeat("x", 16384)}}
	w.Offer(logs)
	if reply := w.writeController.Send(<-w.queue, tunnel.MsgNormal); reply != tunnel.ReplyFenced {
		t.Fatalf("broker rejection reply=%d", reply)
	}
	if w.writeController.LatestLsnAck != previous {
		t.Fatal("failed batch advanced controller ACK")
	}
	w.syncer.checkpoint(true, 0)
	w.syncer.checkpoint(true, utils.TimeStampToInt64(primitive.Timestamp{T: 100, I: 99}))
	if persisted() != previous {
		t.Fatal("failed batch advanced durable checkpoint")
	}
	if w.writeController.Send(logs, tunnel.MsgNormal) != tunnel.ReplyFenced || w.writeController.Send(criticalLocalLogs(7, 1), tunnel.MsgNormal) != tunnel.ReplyFenced {
		t.Fatal("fence allowed replay/later records")
	}
	criticalAssertLocalRecords(t, topic, append(initial, logs[:2]...))
	t.Log("REAL Kafka rejected an oversized record after an accepted prefix: no later sends, no whole-batch replay, durable checkpoint unchanged")
}

func TestCriticalLocalLargeRecordPreservesExistingLimit(t *testing.T) {
	if os.Getenv("CRITICAL_LOCAL_KAFKA_TEST") != "1" {
		t.Skip("opt-in isolated local Kafka only")
	}
	topic := criticalLocalTopic(t, "20971520")
	w, persisted := criticalLocalWorker(t, topic, true, 128)
	logs := criticalLocalLogs(1, 3)
	logs[1].Parsed.Object = bson.D{{Key: "large", Value: strings.Repeat("x", 2*1024*1024)}}
	w.Offer(logs)
	w.transfer(<-w.queue)
	w.syncer.checkpoint(true, 0)
	if persisted() != utils.TimeStampToInt64(logs[2].Parsed.Timestamp) {
		t.Fatal("large-record checkpoint failed")
	}
	criticalAssertLocalRecords(t, topic, logs)
	written := w.writeController.tunnel.(*tunnel.AcknowledgedKafkaWriter).DeliveryStatus()
	if written.ConfirmedBatches != 3 {
		t.Fatalf("large record was not isolated into its own batch: %+v", written)
	}
	t.Log("REAL Kafka accepted a 2 MiB record with a 512 KiB batch target; all three records retained exact payload and order")
}
