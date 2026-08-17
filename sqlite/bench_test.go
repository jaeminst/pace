package sqlite

import (
	"context"
	"fmt"
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
