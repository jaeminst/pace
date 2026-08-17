package runner

import (
	"context"
	"sync"
	"time"

	"github.com/jaeminst/pace/internal/store"
)

// batchFactor is how many due jobs a single poll fetches per worker. A small
// multiple keeps the workers fed without loading the whole backlog.
const batchFactor = 4

// replay recovers the queue at startup.
//
// It first decides the fate of jobs left mid-flight by a previous process, then
// drains whatever is due through the same bounded path the background poller
// uses. It deliberately does not spawn a goroutine per pending job: a large
// backlog would otherwise become an equally large burst of goroutines, each
// holding a request and a body buffer.
func (q *Queue) replay() {
	defer q.replayWg.Done()
	q.recoverStranded()
	q.runDue()
}

// recoverStranded classifies jobs whose intent to send was committed but whose
// outcome was never recorded.
//
// The process that owned them is gone, so the server may or may not have acted.
// Jobs that are unsafe to repeat are parked here; the rest are simply left for
// the poller, which treats an expired lease as eligible.
func (q *Queue) recoverStranded() {
	ctx, cancel := context.WithTimeout(q.ctx, q.cfg.StoreTimeout)
	defer cancel()

	jobs, err := q.cfg.Store.Pending(ctx)
	if err != nil {
		if q.ctx.Err() == nil {
			q.cfg.Logger.Warn("pace: replay: load pending", "err", err)
		}
		return
	}
	for _, j := range jobs {
		if j.State != store.StateSending {
			continue
		}
		if q.cfg.RepeatSafe(j.Method) {
			continue
		}
		q.kill(j, "outcome unknown after restart and the request is not safe to repeat")
	}
}

// poll drives background retries. One goroutine looks for jobs that have become
// due and hands them to a bounded set of workers.
//
// The previous implementation spawned one goroutine per pending job at startup
// and never looked again: a fifty-thousand-job backlog became fifty thousand
// goroutines, each holding a request and a body buffer, and nothing retried
// afterwards until the next restart.
func (q *Queue) poll() {
	defer q.workerWg.Done()

	ticker := time.NewTicker(q.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-q.ctx.Done():
			return
		case <-ticker.C:
		}
		q.runDue()
		q.cfg.AfterPoll()
	}
}

// runDue claims and executes whatever is due, never running more than Workers
// at a time.
func (q *Queue) runDue() {
	ctx, cancel := context.WithTimeout(q.ctx, q.cfg.StoreTimeout)
	jobs, err := q.cfg.Store.Due(ctx, q.cfg.Now().UnixNano(), q.cfg.Workers*batchFactor)
	cancel()
	if err != nil {
		if q.ctx.Err() == nil {
			q.cfg.Logger.Warn("pace: durable: poll", "err", err)
		}
		return
	}

	var wg sync.WaitGroup
	for _, j := range jobs {
		select {
		case q.slots <- struct{}{}:
		case <-q.ctx.Done():
			wg.Wait()
			return
		}
		wg.Go(func() {
			defer func() { <-q.slots }()
			q.cfg.Dispatch(q.ctx, j)
		})
	}
	wg.Wait()
}
