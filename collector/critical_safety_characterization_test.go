package collector

import (
	"testing"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/tunnel"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Matches KafkaWriter's queue-acceptance contract without starting a broker.
type criticalQueueOnlyWriter struct{ queued int }

func (*criticalQueueOnlyWriter) AckRequired() bool        { return false }
func (*criticalQueueOnlyWriter) Prepare() bool            { return true }
func (*criticalQueueOnlyWriter) ParsedLogsRequired() bool { return false }
func (*criticalQueueOnlyWriter) Name() string             { return "kafka" }
func (w *criticalQueueOnlyWriter) Send(m *tunnel.WMessage) int64 {
	w.queued += len(m.RawLogs)
	return tunnel.ReplyOK
}

func TestCriticalCharacterizationAckAheadOfDelivery(t *testing.T) {
	queue := &criticalQueueOnlyWriter{}
	worker := &Worker{syncer: &OplogSyncer{replMetric: &utils.ReplicationMetric{}}}
	controller := &WriteController{worker: worker, tunnel: queue}
	ts := primitive.Timestamp{T: 100, I: 7}
	logs := []*oplog.GenericOplog{{Parsed: &oplog.PartialLog{
		ParsedLog: oplog.ParsedLog{Timestamp: ts, Operation: "u", Namespace: "ds_production.CarrierDevice"},
	}}}
	ack := controller.Send(logs, tunnel.MsgNormal)
	if ack != utils.TimeStampToInt64(ts) || controller.LatestLsnAck != ack || queue.queued != 1 {
		t.Fatalf("unexpected acceptance: ack=%d latest=%d queued=%d", ack, controller.LatestLsnAck, queue.queued)
	}
	t.Logf("REPRODUCED: upper-layer ACK advanced to %d with zero broker deliveries", ack)
}

type criticalFencedWriter struct{ criticalQueueOnlyWriter }

func (*criticalFencedWriter) Fenced() bool { return true }

func TestCriticalFencedControllerDoesNotEncodeOrSendAgain(t *testing.T) {
	w := &criticalFencedWriter{}
	controller := &WriteController{tunnel: w, LatestLsnAck: 100}
	// This malformed input would panic during encoding if the fence were late.
	if controller.Send([]*oplog.GenericOplog{nil}, tunnel.MsgNormal) != tunnel.ReplyFenced || w.queued != 0 || controller.LatestLsnAck != 100 {
		t.Fatal("fenced controller attempted more work")
	}
}
