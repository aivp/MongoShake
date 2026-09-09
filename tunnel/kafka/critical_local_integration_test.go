package kafka

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestCriticalLocalKafkaOrderingAndReplay(t *testing.T) {
	if os.Getenv("CRITICAL_LOCAL_KAFKA_TEST") != "1" {
		t.Skip("opt-in local Kafka 3.6.2 only; no remote broker parameter exists")
	}
	const address = "127.0.0.1:39092"
	const total = 4096
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_0
	config.Net.ReadTimeout = 3 * time.Second
	config.Net.DialTimeout = 3 * time.Second
	config.Admin.Timeout = 5 * time.Second
	admin, err := sarama.NewClusterAdmin([]string{address}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	stem := fmt.Sprintf("codex-critical-validation-%d", time.Now().UnixNano())
	topics := []string{stem + "-legacy", stem + "-batch"}
	for _, topic := range topics {
		if err = admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
			t.Fatal(err)
		}
		defer func(topic string) {
			if err := admin.DeleteTopic(topic); err != nil {
				t.Errorf("local topic cleanup: %v", err)
			}
		}(topic)
	}
	payloads := make([][]byte, total)
	for i := range payloads {
		payloads[i] = []byte(fmt.Sprintf("%08d:%s", i, bytes.Repeat([]byte("x"), 503)))
	}
	w, err := NewSyncWriter("", topics[0]+"@"+address, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Start(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for _, payload := range payloads {
		if err = w.SimpleWrite(payload); err != nil {
			w.Close()
			t.Fatal(err)
		}
	}
	legacyElapsed := time.Since(start)
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := sarama.NewSyncProducer([]string{address}, criticalCandidateConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	for base := 0; base < total; base += 128 {
		batch := make([]*sarama.ProducerMessage, 128)
		for i := range batch {
			batch[i] = &sarama.ProducerMessage{Topic: topics[1], Partition: 0, Key: sarama.StringEncoder("synthetic-record"), Value: sarama.ByteEncoder(payloads[base+i])}
		}
		if err = p.SendMessages(batch); err != nil {
			p.Close()
			t.Fatal(err)
		}
	}
	batchElapsed := time.Since(start)
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	// Producer idempotence does not deduplicate application replay across a new
	// producer session. This intentionally replays the oldest event after all new ones.
	p, err = sarama.NewSyncProducer([]string{address}, criticalCandidateConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = p.SendMessage(&sarama.ProducerMessage{Topic: topics[1], Partition: 0, Key: sarama.StringEncoder("synthetic-record"), Value: sarama.ByteEncoder(payloads[0])})
	if err != nil {
		p.Close()
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	consumer, err := sarama.NewConsumer([]string{address}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	for index, topic := range topics {
		partition, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
		if err != nil {
			t.Fatal(err)
		}
		expected := total
		if index == 1 {
			expected++
		}
		timer := time.NewTimer(15 * time.Second)
		for i := 0; i < expected; i++ {
			select {
			case message := <-partition.Messages():
				if message == nil || !bytes.Equal(message.Value, payloads[i%total]) || message.Offset != int64(i) {
					partition.Close()
					timer.Stop()
					t.Fatalf("topic=%s order/payload mismatch at index=%d", topic, i)
				}
			case <-timer.C:
				partition.Close()
				t.Fatalf("topic=%s timed out at index=%d", topic, i)
			}
		}
		timer.Stop()
		if err := partition.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("LOCAL Kafka 3.6.2, %d x 512-byte records: legacy=%s (%.0f/s), batch128=%s (%.0f/s); all payloads and offsets verified in order", total, legacyElapsed, float64(total)/legacyElapsed.Seconds(), batchElapsed, float64(total)/batchElapsed.Seconds())
	t.Log("REPRODUCED on a real local broker: reopening an idempotent producer accepts replay of the oldest application event after the latest event")
}
