package tunnel

import "testing"

// Characterization, not a safety acceptance: this proves the existing gap.
func TestCriticalCharacterizationKafkaAcceptsWithoutBroker(t *testing.T) {
	w := &KafkaWriter{
		encoderNr: 1,
		inputChan: []chan *WMessage{make(chan *WMessage, 1)},
	}
	m := &WMessage{TMessage: &TMessage{RawLogs: [][]byte{[]byte("not-sent-to-kafka")}}}
	if result := w.Send(m); result != ReplyOK {
		t.Fatalf("Send returned %d", result)
	}
	if w.AckRequired() || len(w.inputChan[0]) != 1 || w.writer != nil {
		t.Fatal("expected an accepted in-memory message with no broker writer")
	}
	t.Log("REPRODUCED: Send succeeds without any Kafka writer; message exists only in RAM")
}
