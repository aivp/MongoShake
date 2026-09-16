package collector

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/alibaba/MongoShake/v2/tunnel"
)

// A failed Kafka batch must never reach the graceful checkpoint/drain path.
// The supervisor may restart from the last durable checkpoint (at-least-once).
func (worker *Worker) exitFencedKafka() {
	exitCode := 75
	status := tunnel.KafkaDeliveryStatus{Fenced: true}
	if writer, ok := worker.writeController.tunnel.(interface {
		DeliveryStatus() tunnel.KafkaDeliveryStatus
	}); ok {
		status = writer.DeliveryStatus()
		if status.Failure != nil && !status.Failure.Retryable {
			exitCode = 78
		}
	}
	diagnostic, _ := json.Marshal(status)
	fmt.Fprintf(os.Stderr, "critical Kafka worker=%d exiting code=%d without advancing checkpoint; restart uses the existing durable checkpoint and may replay records: %s\n", worker.id, exitCode, diagnostic)
	os.Exit(exitCode)
}
