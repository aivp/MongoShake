package conf

import (
	"os"
	"testing"

	utils "github.com/alibaba/MongoShake/v2/common"
	nimo "github.com/gugemichael/nimo4go"
)

func criticalDeliveryConfig() Configuration {
	return Configuration{KafkaAcknowledged: true, Tunnel: "kafka", TunnelMessage: "json", SyncMode: "incr",
		MongoUrls: []string{"mongodb://local-test-only"}, IncrSyncWorker: 1, IncrSyncTunnelWriteThread: 1, TunnelKafkaPartitionNumber: 1,
		IncrSyncMongoFetchMethod: "oplog", IncrSyncWorkerOplogCompressor: utils.VarIncrSyncWorkerOplogCompressorNone}
}

func TestCriticalDeliveryConfig(t *testing.T) {
	c := criticalDeliveryConfig()
	if err := c.NormalizeKafkaDelivery(); err != nil || c.KafkaBatchMaxMessages != 128 || c.KafkaBatchMaxBytes != 524288 || c.KafkaBatchFlushMS != 5 || c.KafkaBatchEnabled {
		t.Fatalf("defaults=%+v err=%v", c, err)
	}
	legacy := Configuration{}
	if err := legacy.NormalizeKafkaDelivery(); err != nil || legacy.KafkaAcknowledged || legacy.KafkaBatchMaxMessages != 0 {
		t.Fatalf("legacy defaults changed: %+v, %v", legacy, err)
	}
	for _, mutate := range []func(*Configuration){
		func(c *Configuration) { c.KafkaAcknowledged = false; c.KafkaBatchEnabled = true },
		func(c *Configuration) { c.IncrSyncWorker = 2 },
		func(c *Configuration) { c.TunnelKafkaPartitionNumber = 2 },
		func(c *Configuration) { c.IncrSyncTunnelWriteThread = 2 },
		func(c *Configuration) { c.MongoUrls = append(c.MongoUrls, "mongodb://another-test") },
		func(c *Configuration) { c.TunnelMessage = "raw" },
		func(c *Configuration) { c.Tunnel = "direct" },
		func(c *Configuration) { c.SyncMode = "all" },
		func(c *Configuration) { c.IncrSyncMongoFetchMethod = "change_stream" },
		func(c *Configuration) { c.IncrSyncTunnelKafkaDebug = "debug.log" },
		func(c *Configuration) { c.KafkaBatchMaxMessages = -1 },
		func(c *Configuration) { c.KafkaBatchMaxBytes = -1 },
		func(c *Configuration) { c.KafkaBatchFlushMS = -1 },
		func(c *Configuration) { c.KafkaBatchMaxMessages = 1025 },
		func(c *Configuration) { c.KafkaBatchMaxBytes = 33554433 },
		func(c *Configuration) { c.KafkaBatchFlushMS = 1001 },
		func(c *Configuration) { c.TunnelJsonFormat = "unknown" },
	} {
		c := criticalDeliveryConfig()
		mutate(&c)
		if err := c.NormalizeKafkaDelivery(); err == nil {
			t.Fatalf("accepted unsafe config: %+v", c)
		}
	}
}

func TestCriticalDeliveryConfigLoader(t *testing.T) {
	c := criticalDeliveryConfig()
	file, err := os.Open("../../tools/critical-validation/critical-kafka-delivery.conf.example")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	loader := nimo.NewConfigLoader(file)
	if err := loader.Load(&c); err != nil {
		t.Fatal(err)
	}
	if !c.KafkaAcknowledged || c.KafkaBatchEnabled || c.KafkaBatchMaxMessages != 128 || c.KafkaBatchMaxBytes != 524288 || c.KafkaBatchFlushMS != 5 {
		t.Fatalf("config not loaded: %+v", c)
	}
}
