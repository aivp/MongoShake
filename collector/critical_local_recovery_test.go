package collector

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"go.mongodb.org/mongo-driver/bson"
)

func TestCriticalLocalFencedWorkerExitAndRecovery(t *testing.T) {
	if os.Getenv("CRITICAL_LOCAL_KAFKA_TEST") != "1" {
		t.Skip("opt-in isolated local Kafka only")
	}
	logs := criticalLocalLogs(1, 5)
	logs[2].Parsed.Object = bson.D{{Key: "large", Value: strings.Repeat("x", 16384)}}
	if action := os.Getenv("CRITICAL_LOCAL_RECOVERY_CHILD"); action != "" {
		topic, storeURL := os.Getenv("CRITICAL_LOCAL_RECOVERY_TOPIC"), os.Getenv("CRITICAL_LOCAL_RECOVERY_STORE")
		u, err := url.Parse(storeURL)
		if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" ||
			!strings.HasPrefix(topic, "codex-critical-production-path-") || (action != "reject" && action != "recover") {
			t.Fatal("child targets must be isolated local test resources")
		}
		w, _ := criticalLocalWorker(t, topic, true, 2)
		conf.Options.CheckpointStorageCollection = storeURL
		w.syncer.ckptManager = ckpt.NewCheckpointManager("local-critical-test", 1)
		if _, _, err := w.syncer.ckptManager.Get(); err != nil {
			t.Fatal(err)
		}
		w.Offer(logs)
		w.transfer(<-w.queue)
		w.syncer.checkpoint(true, 0)
		if action == "reject" {
			t.Fatal("fenced worker returned instead of exiting")
		}
		return
	}
	topic := criticalLocalTopic(t, "8192")
	_, persisted := criticalCheckpointFixture(t)
	storeURL := conf.Options.CheckpointStorageCollection
	run := func(action string, expected int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCriticalLocalFencedWorkerExitAndRecovery$", "-test.timeout=18s")
		cmd.Env = append(os.Environ(), "CRITICAL_LOCAL_RECOVERY_CHILD="+action, "CRITICAL_LOCAL_RECOVERY_TOPIC="+topic, "CRITICAL_LOCAL_RECOVERY_STORE="+storeURL)
		output, err := cmd.CombinedOutput()
		code := 0
		if err != nil {
			if exited, ok := err.(*exec.ExitError); ok {
				code = exited.ExitCode()
			} else {
				t.Fatalf("child execution: %v %s", err, output)
			}
		}
		if ctx.Err() != nil || code != expected {
			t.Fatalf("child %s: exit=%d want=%d output=%s", action, code, expected, output)
		}
		if action == "reject" && (!strings.Contains(string(output), `"code":10`) || !strings.Contains(string(output), "without advancing checkpoint")) {
			t.Fatalf("missing first-cause diagnostic: %s", output)
		}
	}
	run("reject", 78)
	if persisted() != 1 {
		t.Fatal("broker rejection advanced the durable checkpoint")
	}
	criticalAssertLocalRecords(t, topic, logs[:2])
	c := sarama.NewConfig()
	c.Version = sarama.V0_11_0_0
	admin, err := sarama.NewClusterAdmin([]string{criticalLocalBroker}, c)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	limit := "65536"
	if err := admin.AlterConfig(sarama.TopicResource, topic, map[string]*string{"max.message.bytes": &limit}, false); err != nil {
		t.Fatal(err)
	}
	run("recover", 0)
	criticalAssertLocalRecords(t, topic, append(logs[:2:2], logs...))
	if persisted() != utils.TimeStampToInt64(logs[len(logs)-1].Parsed.Timestamp) {
		t.Fatal("recovered worker did not checkpoint the complete interval")
	}
	t.Log("REAL broker rejection: worker exited 78, checkpoint unchanged; after fixing this local topic and starting a new process, all five records arrived and the accepted prefix was replayed")
}
