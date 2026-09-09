package validation

import (
	"errors"
	"reflect"
	"testing"
)

type event struct {
	seq   int
	value string
}

var bindingEvents = []event{{1, "carrier-A"}, {2, ""}, {3, "carrier-B"}}

// These models are NOT wired into MongoShake or CFS.
type state struct {
	seq   int
	value string
}

func (s *state) apply(e event, versionGuard bool) {
	if versionGuard && e.seq <= s.seq {
		return
	}
	s.seq, s.value = e.seq, e.value
}

func TestCriticalBindingOrderAndReplay(t *testing.T) {
	cases := []struct {
		name   string
		events []event
		want   string
	}{
		{"bind", bindingEvents[:1], "carrier-A"},
		{"unbind", bindingEvents[:2], ""},
		{"rebind", bindingEvents, "carrier-B"},
		{"duplicate", append(append([]event{}, bindingEvents...), bindingEvents[2]), "carrier-B"},
		{"out_of_order_old_unbind", append(append([]event{}, bindingEvents...), bindingEvents[1]), "carrier-B"},
		{"replay_interrupted_after_old_bind", append(append([]event{}, bindingEvents...), bindingEvents[0]), "carrier-B"},
		{"status_zero_one_replay", []event{{1, "0"}, {2, "1"}, {1, "0"}}, "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := state{}
			for _, e := range tc.events {
				s.apply(e, true)
			}
			if s.value != tc.want {
				t.Fatalf("got %q, want %q", s.value, tc.want)
			}
		})
	}
}

func TestCriticalCharacterizationWholeBatchRetryRegressesBinding(t *testing.T) {
	s := state{}
	for _, e := range bindingEvents {
		s.apply(e, false)
	}
	// The broker accepted the first attempt, but its response was lost. The
	// retry starts at the old bind and the process fails before the rebind.
	s.apply(bindingEvents[0], false)
	if s.value != "carrier-A" {
		t.Fatal("counterexample was not reproduced")
	}
	t.Log("REPRODUCED: whole-batch replay can restore carrier-A after carrier-B without a version guard")
}

type guardedSender struct {
	checkpoint int
	failed     bool
	send       func([]event) error
}

func (s *guardedSender) write(batch []event) error {
	if s.failed {
		return errors.New("sender fenced after uncertain delivery")
	}
	if err := s.send(batch); err != nil {
		s.failed = true
		return err
	}
	if len(batch) > 0 {
		s.checkpoint = batch[len(batch)-1].seq
	}
	return nil
}

func TestCriticalBatchFailureDoesNotAdvanceOrSendNext(t *testing.T) {
	var attempts [][]event
	s := &guardedSender{checkpoint: 0, send: func(batch []event) error {
		attempts = append(attempts, append([]event{}, batch...))
		return errors.New("ambiguous partial delivery")
	}}
	if s.write(bindingEvents) == nil || s.checkpoint != 0 {
		t.Fatal("failed batch advanced checkpoint")
	}
	if s.write([]event{{4, "carrier-C"}}) == nil || len(attempts) != 1 {
		t.Fatal("later data overtook an uncertain batch")
	}
}

func TestCriticalBatchAckPrecedesCheckpoint(t *testing.T) {
	s := &guardedSender{}
	s.send = func(batch []event) error {
		if s.checkpoint != 0 || !reflect.DeepEqual(batch, bindingEvents) {
			t.Fatal("checkpoint advanced before the original ordered batch was acknowledged")
		}
		return nil
	}
	if err := s.write(bindingEvents); err != nil || s.checkpoint != 3 {
		t.Fatalf("write=%v checkpoint=%d", err, s.checkpoint)
	}
}
