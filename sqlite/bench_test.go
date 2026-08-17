package sqlite

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
)

func benchStore(b *testing.B) *Store {
	b.Helper()
	s, err := OpenStore(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

// BenchmarkDurableCycle measures one durable job end to end at the storage
// layer: record it, claim it, record its outcome. Three commits, which is what
// the journal mode is paid for.
func BenchmarkDurableCycle(b *testing.B) {
	s := benchStore(b)
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		id := fmt.Sprintf("job-%d", i)
		i++
		if err := s.Enqueue(ctx, Job{ID: id, UserID: "u", Method: "GET", Path: "/", Headers: http.Header{}}, int64(i)); err != nil {
			b.Fatal(err)
		}
		if ok, err := s.Claim(ctx, id, "w", int64(i), int64(i)+1_000_000); err != nil || !ok {
			b.Fatalf("claim = (%v, %v)", ok, err)
		}
		if err := s.Complete(ctx, id, Result{StatusCode: 200, Headers: http.Header{}}, int64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLoadDuringWrites is the number the connection layout decides. A
// single shared connection puts every read behind whatever write is committing;
// user lookups on the request path should not queue behind the GC sweep.
func BenchmarkLoadDuringWrites(b *testing.B) {
	s := benchStore(b)
	ctx := context.Background()
	if err := s.Save(ctx, "hot", 1, 1); err != nil {
		b.Fatal(err)
	}

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Go(func() {
		batch := make([]UserState, 256)
		for i := range batch {
			batch[i] = UserState{UserID: fmt.Sprintf("bulk-%d", i), Tokens: 1, LastUsed: 1}
		}
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.SaveBatch(ctx, batch)
		}
	})
	b.Cleanup(func() {
		close(stop)
		writer.Wait()
	})

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := s.Load(ctx, "hot"); err != nil {
			b.Fatal(err)
		}
	}
}
