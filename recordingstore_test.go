package pace_test

import (
	"context"
	"sync"

	"github.com/jaeminst/pace"
)

// recordingStore counts operations and, crucially, counts any that arrive after
// Close. A store that is used after being closed is the failure mode the
// shutdown ordering exists to prevent.
type recordingStore struct {
	mu     sync.Mutex
	closed bool
	after  int
	saves  int
	loads  int
}

func (s *recordingStore) Save(context.Context, string, pace.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.closed {
		s.after++
	}
	return nil
}

func (s *recordingStore) Load(context.Context, string) (pace.State, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	if s.closed {
		s.after++
	}
	return pace.State{}, false, nil
}

func (s *recordingStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *recordingStore) opsAfterClose() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.after
}

func (s *recordingStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

func (s *recordingStore) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}
