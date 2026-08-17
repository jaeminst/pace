package sqlite

import (
	"context"
	"testing"
)

func TestSaveBatchRollsBackOnFailure(t *testing.T) {
	s := walStore(t)
	ctx := context.Background()
	if err := s.SaveBatch(ctx, []UserState{{UserID: "alice", Tokens: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, `DROP TABLE user_state`); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveBatch(ctx, []UserState{{UserID: "bob", Tokens: 2}}); err == nil {
		t.Error("SaveBatch reported success with user_state missing")
	}
}

func TestSaveBatchEmptyIsNoOp(t *testing.T) {
	s := walStore(t)
	if err := s.SaveBatch(context.Background(), nil); err != nil {
		t.Errorf("SaveBatch(nil) = %v, want nil", err)
	}
}
