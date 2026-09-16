package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"

	"github.com/Shopify/sarama"
)

func TestCriticalDeliveryErrorDetails(t *testing.T) {
	for _, tc := range []struct {
		name, category string
		err            error
		retryable      bool
	}{
		{"metadata", "transient", sarama.ErrNotLeaderForPartition, true},
		{"timeout", "ambiguous", sarama.ErrRequestTimedOut, true},
		{"partial_append", "ambiguous", sarama.ErrNotEnoughReplicasAfterAppend, true},
		{"producer_epoch", "producer_state", sarama.ErrOutOfOrderSequenceNumber, true},
		{"too_large", "permanent", sarama.ErrMessageSizeTooLarge, false},
		{"authorization", "permanent", sarama.ErrTopicAuthorizationFailed, false},
		{"eof", "ambiguous", io.EOF, true},
		{"reset", "ambiguous", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, true},
		{"unknown", "unknown", errors.New("password=do-not-print"), false},
		{"nil", "unknown", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := sarama.ProducerErrors{&sarama.ProducerError{Err: tc.err,
				Msg: &sarama.ProducerMessage{Partition: 0, Key: sarama.StringEncoder("SECRET-KEY"), Value: sarama.StringEncoder("SECRET-PAYLOAD")}}}
			got := DescribeDeliveryError(fmt.Errorf("batch: %w", batch))
			if got.Category != tc.category || got.Retryable != tc.retryable || got.FailedRecords != 1 || len(got.Errors) != 1 {
				t.Fatalf("details=%+v", got)
			}
			data, _ := json.Marshal(got)
			for _, secret := range []string{"SECRET-KEY", "SECRET-PAYLOAD", "do-not-print"} {
				if strings.Contains(string(data), secret) {
					t.Fatal("diagnostic leaked message or credentials")
				}
			}
			if code, ok := tc.err.(sarama.KError); ok && (got.Errors[0].Code == nil || *got.Errors[0].Code != int16(code)) {
				t.Fatal("underlying Kafka code was lost")
			}
			if tc.name == "reset" && !strings.Contains(got.Errors[0].Message, "reset") {
				t.Fatal("underlying network cause was lost")
			}
		})
	}
}

func TestCriticalDeliveryErrorBoundedAndMixed(t *testing.T) {
	errors := make(sarama.ProducerErrors, 100)
	for i := range errors {
		errors[i] = &sarama.ProducerError{Err: sarama.ErrRequestTimedOut}
	}
	errors[99].Err = sarama.ErrMessageSizeTooLarge
	got := DescribeDeliveryError(errors)
	if got.Retryable || got.Category != "permanent" || got.FailedRecords != 100 || len(got.Errors) != 8 {
		t.Fatalf("mixed/large error batch=%+v", got)
	}
	if got.Errors[7].Code == nil || *got.Errors[7].Code != 10 {
		t.Fatal("bounded diagnostics hid the permanent root cause")
	}
}
