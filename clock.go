package pace

import "time"

// Clock abstracts wall-clock time. Implement it to control time in tests.
type Clock interface {
	Now() time.Time
}

type stdClock struct{}

func (stdClock) Now() time.Time { return time.Now() }
