package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
)

func TestCriticalProductionProducerConfiguration(t *testing.T) {
	legacy, err := NewSyncWriter("", "local-test@127.0.0.1:1", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{1, 128} {
		flush := 5 * time.Millisecond
		if count == 1 {
			flush = 0
		}
		writer, err := NewAcknowledgedSyncWriter("", "local-test@127.0.0.1:1", 0, count, flush)
		if err != nil {
			t.Fatal(err)
		}
		c := writer.config.Config
		if c.Producer.Retry.Max != 10 || c.Producer.Retry.Backoff != 500*time.Millisecond || c.Metadata.RefreshFrequency != 3*time.Minute || c.Metadata.Timeout != 10*time.Second {
			t.Fatal("bounded recovery settings missing")
		}
		if !c.Producer.Idempotent || c.Producer.RequiredAcks != sarama.WaitForAll || c.Net.MaxOpenRequests != 1 || c.Producer.Retry.Max < 1 || !c.Version.IsAtLeast(sarama.V0_11_0_0) {
			t.Fatal("unsafe producer contract")
		}
		if c.Producer.MaxMessageBytes != legacy.config.Config.Producer.MaxMessageBytes || c.Producer.MaxMessageBytes != 18*1024*1024 {
			t.Fatal("existing large-record support was reduced")
		}
		if c.Producer.Flush.MaxMessages != count || c.Producer.Flush.Frequency != flush || writer.partition != legacy.partition {
			t.Fatal("batch or partition contract changed")
		}
	}
	for _, count := range []int{0, -1, 1025} {
		if _, err := NewAcknowledgedSyncWriter("", "local-test@127.0.0.1:1", 0, count, 5*time.Millisecond); err == nil {
			t.Fatal("invalid batch limit accepted")
		}
	}
	if _, err := NewAcknowledgedSyncWriter("", "local-test@127.0.0.1:1", 0, 128, 0); err == nil {
		t.Fatal("a partial batch could wait forever without a flush interval")
	}
}

func TestCriticalRecoveryConfigurationOverrides(t *testing.T) {
	previous := conf.Options
	t.Cleanup(func() { conf.Options = previous })
	conf.Options = conf.Configuration{TunnelKafkaVersion: "2.6.0", KafkaProducerMaxMessage: 1024 * 1024}
	w, err := NewAcknowledgedSyncWriter("", "local-test@127.0.0.1:1", 0, 128, 5*time.Millisecond)
	if err != nil || w.config.Config.Version != sarama.V2_6_0_0 || w.MaxMessageBytes() != 1024*1024 {
		t.Fatalf("explicit producer settings were ignored: %v", err)
	}
	legacy, err := NewSyncWriter("", "local-test@127.0.0.1:1", 0)
	if err != nil || legacy.config.Config.Version != sarama.V0_10_0_0 || legacy.MaxMessageBytes() != 18*1024*1024 || legacy.config.Config.Producer.Retry.Max != 3 {
		t.Fatal("legacy producer settings changed")
	}
	for _, version := range []string{"0.10.0.0", "not-a-version"} {
		conf.Options.TunnelKafkaVersion = version
		if _, err := NewAcknowledgedSyncWriter("", "local-test@127.0.0.1:1", 0, 128, 5*time.Millisecond); err == nil {
			t.Fatalf("accepted unsafe protocol %s", version)
		}
	}
}
