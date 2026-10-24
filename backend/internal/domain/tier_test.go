package domain

import "testing"

// TestWorkerTier_Valid pins the accepted tier set. The empty string is absent,
// not a valid tier, so callers can branch on it before validating.
func TestWorkerTier_Valid(t *testing.T) {
	tests := []struct {
		name string
		tier WorkerTier
		want bool
	}{
		{"fast", TierFast, true},
		{"standard", TierStandard, true},
		{"deep", TierDeep, true},
		{"empty is absent", WorkerTier(""), false},
		{"unknown", WorkerTier("turbo"), false},
		{"case sensitive", WorkerTier("Fast"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tier.Valid(); got != tt.want {
				t.Fatalf("WorkerTier(%q).Valid() = %v, want %v", tt.tier, got, tt.want)
			}
		})
	}
}
