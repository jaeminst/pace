package runner

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/jaeminst/pace/sqlite"
)

func benchJobs(b *testing.B) *Jobs {
	b.Helper()
	db, err := sqlite.OpenStore(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return NewJobs(db)
}

// BenchmarkDurableCycle measures one durable job end to end at the storage
// layer: record it, claim it, record its outcome. Three commits, which is what
// the journal mode is paid for.
func BenchmarkDurableCycle(b *testing.B) {
	j := benchJobs(b)
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		id := fmt.Sprintf("job-%d", i)
		i++
		if err := j.Enqueue(ctx, Job{ID: id, UserID: "u", Method: "GET", Path: "/", Headers: http.Header{}}, int64(i)); err != nil {
			b.Fatal(err)
		}
		if ok, err := j.Claim(ctx, id, "w", int64(i), int64(i)+1_000_000); err != nil || !ok {
			b.Fatalf("claim = (%v, %v)", ok, err)
		}
		if err := j.Complete(ctx, id, Result{StatusCode: 200, Headers: http.Header{}}, int64(i)); err != nil {
			b.Fatal(err)
		}
	}
}
