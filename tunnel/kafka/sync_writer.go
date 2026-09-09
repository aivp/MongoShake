package kafka

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

type SyncWriter struct {
	brokers   []string
	topic     string
	partition int32
	producer  sarama.SyncProducer

	config *Config
}

func NewSyncWriter(rootCaFile, address string, partitionId int) (*SyncWriter, error) {
	c, err := NewConfig(rootCaFile)
	if err != nil {
		return nil, err
	}

	topic, brokers, err := parse(address)
	if err != nil {
		return nil, err
	}

	s := &SyncWriter{
		brokers:   brokers,
		topic:     topic,
		partition: int32(partitionId),
		config:    c,
	}

	return s, nil
}

func (s *SyncWriter) Start() error {
	producer, err := sarama.NewSyncProducer(s.brokers, s.config.Config)
	if err != nil {
		return err
	}
	s.producer = producer
	return nil
}

// NewAcknowledgedSyncWriter preserves the existing payload limit and manual
// partition mapping. The 0.11 wire protocol enables idempotence on Kafka 3.6.
func NewAcknowledgedSyncWriter(rootCaFile, address string, partitionId, maxMessages int, flush time.Duration) (*SyncWriter, error) {
	if maxMessages < 1 || maxMessages > 1024 || flush < 0 || maxMessages > 1 && flush == 0 {
		return nil, fmt.Errorf("invalid acknowledged Kafka batch count or flush interval")
	}
	s, err := NewSyncWriter(rootCaFile, address, partitionId)
	if err != nil {
		return nil, err
	}
	c := s.config.Config
	c.Version = sarama.V0_11_0_0
	c.Producer.Idempotent = true
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Net.MaxOpenRequests = 1
	c.Producer.Retry.Max = 3
	c.Producer.Flush.Messages = maxMessages
	c.Producer.Flush.MaxMessages = maxMessages
	c.Producer.Flush.Frequency = flush
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SyncWriter) WriteBatch(inputs [][]byte) error {
	batch := make([]*sarama.ProducerMessage, len(inputs))
	for i, input := range inputs {
		batch[i] = &sarama.ProducerMessage{
			Topic: s.topic, Partition: s.partition,
			Key:   sarama.ByteEncoder(strconv.FormatInt(time.Now().UnixNano(), 16)),
			Value: sarama.ByteEncoder(input),
		}
	}
	return s.producer.SendMessages(batch)
}

func (s *SyncWriter) MaxMessageBytes() int { return s.config.Config.Producer.MaxMessageBytes }

func (s *SyncWriter) SimpleWrite(input []byte) error {
	return s.send(input)
}

func (s *SyncWriter) send(input []byte) error {
	// use timestamp as key
	key := strconv.FormatInt(time.Now().UnixNano(), 16)

	msg := &sarama.ProducerMessage{
		Topic:     s.topic,
		Partition: s.partition,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(input),
	}
	_, _, err := s.producer.SendMessage(msg)
	return err
}

func (s *SyncWriter) Close() error {
	return s.producer.Close()
}
