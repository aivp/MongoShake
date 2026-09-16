package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestCriticalProducerRecoveryAndExhaustion(t *testing.T) {
	for _, exhaust := range []bool{false, true} {
		name := "temporary_rejection_then_recover"
		if exhaust {
			name = "retry_budget_exhausted"
		}
		t.Run(name, func(t *testing.T) {
			b := sarama.NewMockBroker(t, 0)
			defer b.Close()
			failure := sarama.NewMockProduceResponse(t).SetVersion(3).SetError(criticalValidationTopic, 0, sarama.ErrNotEnoughReplicas)
			var produce sarama.MockResponse = failure
			if !exhaust {
				produce = sarama.NewMockSequence(failure, failure, sarama.NewMockProduceResponse(t).SetVersion(3))
			}
			b.SetHandlerByMap(map[string]sarama.MockResponse{
				"MetadataRequest":       sarama.NewMockMetadataResponse(t).SetBroker(b.Addr(), 0).SetLeader(criticalValidationTopic, 0, 0),
				"InitProducerIDRequest": sarama.NewMockWrapper(&sarama.InitProducerIDResponse{ProducerID: 1}),
				"ProduceRequest":        produce,
			})
			w, err := NewAcknowledgedSyncWriter("", criticalValidationTopic+"@"+b.Addr(), 0, 2, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			// Keep the configured fixture backoff short; wall time is bounded separately.
			w.config.Config.Producer.Retry.Backoff = time.Millisecond
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			err = w.WriteBatch([][]byte{[]byte("first"), []byte("second")})
			requests := criticalProduceRequests(b)
			if !exhaust {
				if err != nil || requests != 3 {
					t.Fatalf("recovery failed: requests=%d err=%v", requests, err)
				}
			} else {
				if err == nil || requests != 11 {
					t.Fatalf("retry bound failed: requests=%d err=%v", requests, err)
				}
				details := DescribeDeliveryError(err)
				if details.Category != "transient" || details.FailedRecords != 2 || details.Errors[0].Code == nil || *details.Errors[0].Code != 19 {
					t.Fatalf("root cause not preserved: %+v", details)
				}
			}
		})
	}
}
