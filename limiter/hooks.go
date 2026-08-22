package limiter

// hooks are test seams: points where a test needs to observe or pause the
// Limiter to make a race deterministic rather than time it.
//
// The set is nil in production and every call site checks for that, so the cost
// is an atomic load and a predictable branch, sitting next to work that dwarfs
// both — a mutex acquisition, a database write, an HTTP round-trip. Keeping
// them in one named type rather than as loose function fields on Limiter makes
// it obvious what they are and keeps the production struct describing
// production.
//
// The pointer is atomic because New starts the GC goroutine before a test can
// install anything, so the write genuinely races the read.
//
// They exist because the alternative is sleeping. A test that waits 20ms for a
// goroutine to reach a particular line is both slower than it needs to be and
// wrong under load, which is exactly when CI runs.
type hooks struct {
	// getOrCreate fires in userFor's cold path, before the write lock, so a
	// test can force two goroutines to race for the same new user.
	getOrCreate func()

	// beforeWait fires immediately before a caller blocks for a token, so a
	// test can act once the goroutine is genuinely waiting.
	beforeWait func()

	// afterSweep fires at the end of each GC sweep, so a test can wait for one
	// to have happened instead of guessing how long the ticker takes.
	afterSweep func()

	// shuttingDown fires once Shutdown has closed the door to new requests but
	// before it has cancelled anything, which is the only window in which the
	// "refused because shutting down" branch is reachable.
	shuttingDown func()

	// beforeQuotaTake fires immediately before a SharedQuota call, so a test
	// can drive a shutdown or a breaker transition into that window without
	// timing it.
	beforeQuotaTake func()
}

// fireGetOrCreate and friends keep the nil checks in one place.
func (l *Limiter) fireGetOrCreate() {
	if h := l.hooks.Load(); h != nil && h.getOrCreate != nil {
		h.getOrCreate()
	}
}

func (l *Limiter) fireBeforeWait() {
	if h := l.hooks.Load(); h != nil && h.beforeWait != nil {
		h.beforeWait()
	}
}

func (l *Limiter) fireAfterSweep() {
	if h := l.hooks.Load(); h != nil && h.afterSweep != nil {
		h.afterSweep()
	}
}

func (l *Limiter) fireShuttingDown() {
	if h := l.hooks.Load(); h != nil && h.shuttingDown != nil {
		h.shuttingDown()
	}
}

func (l *Limiter) fireBeforeQuotaTake() {
	if h := l.hooks.Load(); h != nil && h.beforeQuotaTake != nil {
		h.beforeQuotaTake()
	}
}
