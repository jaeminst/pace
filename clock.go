package pace

import "time"

// Clock abstracts wall-clock time. Implement it to control time in tests.
//
// It has one method deliberately. pace may later recognise optional extensions
// — a timer source, say — by type assertion, in the same way [BatchStateStore]
// extends [StateStore]; an implementation that provides only Now will keep
// working, because pace would never require the extension. So there is nothing
// to pre-emptively widen this to before the v1 freeze.
//
// Note that the token bucket schedules its own waits against the real clock,
// since golang.org/x/time/rate owns that timer and takes no time argument. A
// fake Clock therefore drives expiry, restore, and every timestamp pace
// records, but not how long [Client.Wait] actually blocks.
type Clock interface {
	Now() time.Time
}

type stdClock struct{}

func (stdClock) Now() time.Time { return time.Now() }
