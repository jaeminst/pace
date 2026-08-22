package registry

import "testing"

// FuzzShardIndex checks the inlined FNV-1a against the standard library's, so
// the comment claiming it is a faithful reimplementation has evidence behind
// it. A divergence would not break correctness — any hash distributes keys —
// but it would quietly invalidate the reason the inline version exists.
func FuzzShardIndex(f *testing.F) {
	f.Add("alice", uint32(255))
	f.Add("", uint32(255))
	f.Add("user-\xff\xfe", uint32(0))
	f.Add("日本語のユーザー", uint32(1023))

	f.Fuzz(func(t *testing.T, s string, mask uint32) {
		const (
			offset32 = 2166136261
			prime32  = 16777619
		)
		// The reference: FNV-1a over the raw bytes, exactly as hash/fnv does it.
		want := uint32(offset32)
		for _, b := range []byte(s) {
			want ^= uint32(b)
			want *= prime32
		}
		if got := shardIndex(s, mask); got != want&mask {
			t.Errorf("shardIndex(%q, %d) = %d, want %d", s, mask, got, want&mask)
		}
	})
}
