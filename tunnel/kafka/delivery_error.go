package kafka

import (
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"

	"github.com/Shopify/sarama"
)

// DeliveryFailure never includes ProducerMessage keys, values or credentials.
type DeliveryFailure struct {
	Category      string                `json:"category"`
	Retryable     bool                  `json:"retryable"`
	FailedRecords int                   `json:"failed_records"`
	Errors        []DeliveryErrorDetail `json:"errors"`
}

type DeliveryErrorDetail struct {
	Type      string `json:"type"`
	Code      *int16 `json:"code,omitempty"`
	Message   string `json:"message"`
	Partition int32  `json:"partition"`
}

// DescribeDeliveryError unwraps the per-record causes hidden by ProducerErrors.Error().
// Retryable describes the cause, not permission to replay an ambiguous batch.
func DescribeDeliveryError(err error) DeliveryFailure {
	result := DeliveryFailure{Category: "transient", Retryable: true}
	add := func(cause error, partition int32) {
		category, retryable, detail := describeCause(cause)
		higherPriority := failurePriority(category) > failurePriority(result.Category)
		if higherPriority {
			result.Category = category
		}
		result.Retryable = result.Retryable && retryable
		result.FailedRecords++
		detail.Partition = partition
		if len(result.Errors) < 8 {
			result.Errors = append(result.Errors, detail)
		} else if higherPriority {
			result.Errors[len(result.Errors)-1] = detail
		}
	}
	var batch sarama.ProducerErrors
	if errors.As(err, &batch) && len(batch) > 0 {
		for _, failure := range batch {
			if failure == nil {
				add(nil, -1)
				continue
			}
			partition := int32(-1)
			if failure.Msg != nil {
				partition = failure.Msg.Partition
			}
			add(failure.Err, partition)
		}
	} else {
		add(err, -1)
	}
	return result
}

func failurePriority(category string) int {
	switch category {
	case "permanent":
		return 5
	case "unknown":
		return 4
	case "producer_state":
		return 3
	case "ambiguous":
		return 2
	default:
		return 1
	}
}

func describeCause(err error) (string, bool, DeliveryErrorDetail) {
	detail := DeliveryErrorDetail{Type: fmt.Sprintf("%T", err), Message: "unclassified delivery error; inspect the producer and broker", Partition: -1}
	var code sarama.KError
	if errors.As(err, &code) {
		n := int16(code)
		detail.Code, detail.Message = &n, code.Error()
		switch code {
		case sarama.ErrUnknownTopicOrPartition, sarama.ErrLeaderNotAvailable,
			sarama.ErrNotLeaderForPartition, sarama.ErrBrokerNotAvailable,
			sarama.ErrReplicaNotAvailable, sarama.ErrNotEnoughReplicas:
			return "transient", true, detail
		case sarama.ErrRequestTimedOut, sarama.ErrNetworkException,
			sarama.ErrNotEnoughReplicasAfterAppend, sarama.ErrKafkaStorageError:
			return "ambiguous", true, detail
		case sarama.ErrOutOfOrderSequenceNumber, sarama.ErrDuplicateSequenceNumber,
			sarama.ErrInvalidProducerEpoch, sarama.ErrUnknownProducerID:
			return "producer_state", true, detail
		case sarama.ErrInvalidMessage, sarama.ErrInvalidMessageSize, sarama.ErrMessageSizeTooLarge,
			sarama.ErrMessageSetSizeTooLarge, sarama.ErrInvalidRequiredAcks, sarama.ErrInvalidTopic,
			sarama.ErrTopicAuthorizationFailed, sarama.ErrClusterAuthorizationFailed,
			sarama.ErrUnsupportedVersion, sarama.ErrUnsupportedForMessageFormat,
			sarama.ErrSASLAuthenticationFailed, sarama.ErrInvalidTimestamp:
			return "permanent", false, detail
		default:
			return "unknown", false, detail
		}
	}
	var network net.Error
	if errors.As(err, &network) {
		// Do not format arbitrary wrapped errors: they can contain URLs or payloads.
		detail.Message = fmt.Sprintf("network delivery failure (timeout=%t); broker append outcome unknown", network.Timeout())
		var errno syscall.Errno
		if errors.As(err, &errno) {
			detail.Message += ": " + errno.Error()
		}
		return "ambiguous", true, detail
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		detail.Message = "connection closed before delivery was confirmed"
		return "ambiguous", true, detail
	}
	return "unknown", false, detail
}
