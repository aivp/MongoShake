package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/tunnel"
	"github.com/alibaba/MongoShake/v2/tunnel/kafka"
)

type criticalExitWriter struct {
	criticalFencedWriter
	retryable bool
}

func (w *criticalExitWriter) DeliveryStatus() tunnel.KafkaDeliveryStatus {
	return tunnel.KafkaDeliveryStatus{Fenced: true, LastBrokerACK: 100,
		Failure: &tunnel.KafkaFailureStatus{DeliveryFailure: kafka.DeliveryFailure{Category: "injected", Retryable: w.retryable}}}
}

func TestCriticalFenceExitNoCheckpointAdvance(t *testing.T) {
	if child := os.Getenv("CRITICAL_FENCE_EXIT_CHILD"); child != "" {
		storeURL := os.Getenv("CRITICAL_FENCE_EXIT_STORE")
		u, err := url.Parse(storeURL)
		if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || (child != "75" && child != "78") {
			t.Fatal("child must only use the isolated local checkpoint fixture")
		}
		conf.Options = conf.Configuration{KafkaAcknowledged: true, CheckpointStorage: utils.VarCheckpointStorageApi, CheckpointStorageCollection: storeURL}
		s := mockCheckpointSyncer(1)
		s.replMetric = &utils.ReplicationMetric{}
		s.ckptManager = ckpt.NewCheckpointManager("local-fence-exit", 1)
		if _, _, err := s.ckptManager.Get(); err != nil {
			t.Fatal(err)
		}
		w := s.batcher.workerGroup[0]
		w.syncer = s
		w.ack, w.unack, w.pendingKafkaBatches = 100, 200, 1
		w.writeController = &WriteController{worker: w, tunnel: &criticalExitWriter{retryable: child == "75"}, LatestLsnAck: 100}
		// Nil would panic if the fenced controller tried to encode the batch again.
		w.transfer([]*oplog.GenericOplog{nil})
		s.checkpoint(true, 300)
		fmt.Println("UNSAFE_RETURN_FROM_FENCE")
		return
	}
	for _, code := range []int{75, 78} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var writes int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					atomic.AddInt32(&writes, 1)
				}
				json.NewEncoder(w).Encode(ckpt.CheckpointContext{Name: "local-fence-exit", Timestamp: 100, Version: utils.FcvCheckpoint.CurrentVersion})
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCriticalFenceExitNoCheckpointAdvance$")
			cmd.Env = append(os.Environ(), fmt.Sprintf("CRITICAL_FENCE_EXIT_CHILD=%d", code), "CRITICAL_FENCE_EXIT_STORE="+server.URL)
			output, err := cmd.CombinedOutput()
			exited, ok := err.(*exec.ExitError)
			if !ok || exited.ExitCode() != code || ctx.Err() != nil {
				t.Fatalf("expected prompt exit=%d, err=%v output=%s", code, err, output)
			}
			if atomic.LoadInt32(&writes) != 0 || strings.Contains(string(output), "UNSAFE_RETURN_FROM_FENCE") || !strings.Contains(string(output), "without advancing checkpoint") {
				t.Fatalf("unsafe exit: writes=%d output=%s", writes, output)
			}
		})
	}
}
