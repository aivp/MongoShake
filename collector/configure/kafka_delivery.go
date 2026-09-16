package conf

import (
	"fmt"

	utils "github.com/alibaba/MongoShake/v2/common"
)

// NormalizeKafkaDelivery only changes the opt-in path. Existing configurations
// continue using their existing writer and acknowledgement contract.
func (c *Configuration) NormalizeKafkaDelivery() error {
	if !c.KafkaAcknowledged {
		if c.KafkaBatchEnabled {
			return fmt.Errorf("Kafka batching requires incr_sync.tunnel.kafka.acknowledged=true")
		}
		return nil
	}
	if c.Tunnel != utils.VarTunnelKafka || c.TunnelMessage != utils.VarTunnelMessageJson || c.SyncMode != utils.VarSyncModeIncr {
		return fmt.Errorf("acknowledged Kafka currently requires tunnel=kafka, tunnel.message=json and sync_mode=incr")
	}
	if c.IncrSyncWorker != 1 || c.TunnelKafkaPartitionNumber != 1 || c.IncrSyncTunnelWriteThread != 1 || len(c.MongoUrls) != 1 {
		return fmt.Errorf("acknowledged Kafka currently requires one Mongo source, worker, partition and write thread")
	}
	if c.IncrSyncMongoFetchMethod != utils.VarIncrSyncMongoFetchMethodOplog {
		return fmt.Errorf("acknowledged Kafka currently supports only oplog fetching")
	}
	if c.IncrSyncTunnelKafkaDebug != "" || c.IncrSyncWorkerOplogCompressor != utils.VarIncrSyncWorkerOplogCompressorNone {
		return fmt.Errorf("acknowledged Kafka cannot use a debug sink or oplog compression")
	}
	if c.TunnelJsonFormat != "" && c.TunnelJsonFormat != "canonical_extended_json" {
		return fmt.Errorf("unsupported acknowledged Kafka JSON format")
	}
	if c.KafkaBatchMaxMessages == 0 {
		c.KafkaBatchMaxMessages = 128
	}
	if c.KafkaBatchMaxBytes == 0 {
		c.KafkaBatchMaxBytes = 512 * 1024
	}
	if c.KafkaBatchFlushMS == 0 {
		c.KafkaBatchFlushMS = 5
	}
	if c.KafkaBatchMaxMessages < 1 || c.KafkaBatchMaxMessages > 1024 {
		return fmt.Errorf("Kafka batch.max_messages must be between 1 and 1024")
	}
	if c.KafkaBatchMaxBytes < 1 || c.KafkaBatchMaxBytes > 32*1024*1024 {
		return fmt.Errorf("Kafka batch.max_bytes must be between 1 and 33554432; larger single records are sent alone")
	}
	if c.KafkaBatchFlushMS < 1 || c.KafkaBatchFlushMS > 1000 {
		return fmt.Errorf("Kafka batch.flush_ms must be between 1 and 1000")
	}
	return c.NormalizeKafkaRecovery()
}

// NormalizeKafkaRecovery also supports constructing a producer without a collector.
func (c *Configuration) NormalizeKafkaRecovery() error {
	if c.TunnelKafkaVersion == "" {
		c.TunnelKafkaVersion = "0.11.0.0"
	}
	if c.KafkaSendTimeoutMS == 0 {
		c.KafkaSendTimeoutMS = 120000
	}
	if c.KafkaSendTimeoutMS < 1000 || c.KafkaSendTimeoutMS > 600000 {
		return fmt.Errorf("Kafka send_timeout_ms must be between 1000 and 600000")
	}
	if c.KafkaProducerMaxMessage < 0 || c.KafkaProducerMaxMessage > 18*1024*1024 {
		return fmt.Errorf("Kafka producer.max_message_bytes must be between 0 and 18874368; 0 preserves the existing limit")
	}
	return nil
}
