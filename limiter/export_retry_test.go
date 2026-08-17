package limiter

// PurgeResults runs the cached-result purge without waiting for the GC tick.
func PurgeResults(l *Limiter) { l.purgeResults() }
