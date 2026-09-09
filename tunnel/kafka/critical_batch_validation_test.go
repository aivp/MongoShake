package kafka

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

const criticalValidationTopic = "local-critical-validation"

// Test-only candidate settings. Nothing in the production Start path uses this.
func criticalCandidateConfig(t *testing.T) *sarama.Config {
	t.Helper()
	wrapped, err := NewConfig("")
	if err != nil {
		t.Fatal(err)
	}
	c := wrapped.Config
	c.Version = sarama.V0_11_0_0
	c.Producer.Idempotent = true
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Net.MaxOpenRequests = 1
	c.Producer.Retry.Max = 3
	c.Producer.Flush.Messages = 128
	c.Producer.Flush.Frequency = 5 * time.Millisecond
	c.Producer.Flush.MaxMessages = 128
	c.Producer.MaxMessageBytes = 512 * 1024
	c.Net.DialTimeout = 2 * time.Second
	c.Net.ReadTimeout = 2 * time.Second
	c.Net.WriteTimeout = 2 * time.Second
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	return c
}

func criticalMockBroker(t *testing.T, version int16) *sarama.MockBroker {
	t.Helper()
	b := sarama.NewMockBroker(t, 0)
	b.SetLatency(time.Millisecond)
	b.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":       sarama.NewMockMetadataResponse(t).SetBroker(b.Addr(), 0).SetLeader(criticalValidationTopic, 0, 0),
		"ProduceRequest":        sarama.NewMockProduceResponse(t).SetVersion(version),
		"InitProducerIDRequest": sarama.NewMockWrapper(&sarama.InitProducerIDResponse{ProducerID: 1}),
	})
	return b
}

func criticalProduceRequests(b *sarama.MockBroker) int {
	n := 0
	for _, pair := range b.History() {
		if _, ok := pair.Request.(*sarama.ProduceRequest); ok {
			n++
		}
	}
	return n
}

func TestCriticalBatchProtocolAndRoundTrips(t *testing.T) {
	const total = 1024
	payload := bytes.Repeat([]byte("x"), 512)
	var baselineRequests, batchRequests int
	var baselineDuration, batchDuration time.Duration
	t.Run("actual_legacy_SimpleWrite", func(t *testing.T) {
		b := criticalMockBroker(t, 2)
		defer b.Close()
		w, err := NewSyncWriter("", criticalValidationTopic+"@"+b.Addr(), 0)
		if err != nil {
			t.Fatal(err)
		}
		w.config.Config.Net.ReadTimeout = 2 * time.Second
		if err = w.Start(); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		for i := 0; i < total; i++ {
			if err = w.SimpleWrite(payload); err != nil {
				t.Fatal(err)
			}
		}
		baselineDuration = time.Since(start)
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		baselineRequests = criticalProduceRequests(b)
		if baselineRequests != total {
			t.Fatalf("requests=%d want=%d", baselineRequests, total)
		}
	})
	t.Run("candidate_idempotent_batch_128", func(t *testing.T) {
		b := criticalMockBroker(t, 3)
		defer b.Close()
		p, err := sarama.NewSyncProducer([]string{b.Addr()}, criticalCandidateConfig(t))
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		for base := 0; base < total; base += 128 {
			batch := make([]*sarama.ProducerMessage, 128)
			for i := range batch {
				batch[i] = &sarama.ProducerMessage{Topic: criticalValidationTopic, Partition: 0, Key: sarama.StringEncoder(fmt.Sprint(base + i)), Value: sarama.ByteEncoder(payload)}
			}
			if err = p.SendMessages(batch); err != nil {
				t.Fatal(err)
			}
		}
		batchDuration = time.Since(start)
		if err = p.Close(); err != nil {
			t.Fatal(err)
		}
		batchRequests = criticalProduceRequests(b)
		if batchRequests < total/128 || batchRequests >= baselineRequests/4 {
			t.Fatalf("unexpected request count: %d", batchRequests)
		}
	})
	// Timing is reported, not used as a flaky pass/fail condition. MockBroker
	// does not implement disk durability, real broker deduplication, or CFS.
	t.Logf("SIMULATED 1ms response latency, %d x 512-byte events: legacy requests=%d elapsed=%s; batch requests=%d elapsed=%s", total, baselineRequests, baselineDuration, batchRequests, batchDuration)
}

func TestCriticalCandidateRejectsUnsafeSettings(t *testing.T) {
	for _, name := range []string{"old_protocol", "multiple_inflight", "no_ack", "no_retry"} {
		t.Run(name, func(t *testing.T) {
			c := criticalCandidateConfig(t)
			switch name {
			case "old_protocol":
				c.Version = sarama.V0_10_0_0
			case "multiple_inflight":
				c.Net.MaxOpenRequests = 5
			case "no_ack":
				c.Producer.RequiredAcks = sarama.NoResponse
			case "no_retry":
				c.Producer.Retry.Max = 0
			}
			if c.Validate() == nil {
				t.Fatal("unsafe idempotent-producer setting accepted")
			}
		})
	}
}
