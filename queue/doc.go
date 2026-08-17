// Package queue configures the durable request queue.
//
// A durable request is recorded before it is sent and its result is recorded
// after, so a restart mid-flight neither loses it nor sends it twice by
// accident. [Config] is where that behaviour is tuned — how many attempts, how
// long a lease, what to do with a job whose outcome nobody knows — and it is
// supplied as github.com/jaeminst/pace/limiter.Config.Queue.
//
// The queue itself lives with the Limiter, because running a job means paying
// for a rate-limit token and making the request. What lives here is what a
// caller decides about it: the retry policy, the ambiguity policy, and the
// shape of a job that will never be retried.
package queue
