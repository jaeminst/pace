package sqlite

import (
	"context"
	"testing"
)

// TestStateOperationsAfterCloseReportErrors is the user-state half of a check
// the queue keeps for its own methods: every entry point must report a closed
// database rather than pretend the write landed.
func TestStateOperationsAfterCloseReportErrors(t *testing.T) {
	s, err := OpenStore(tempDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := s.Save(ctx, "u", 1, 1); err == nil {
		t.Error("Save on a closed store reported success")
	}
	if err := s.SaveBatch(ctx, []UserState{{UserID: "u"}}); err == nil {
		t.Error("SaveBatch on a closed store reported success")
	}
	if _, _, err := s.Load(ctx, "u"); err == nil {
		t.Error("Load on a closed store reported success")
	}
}
