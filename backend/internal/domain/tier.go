package domain

// WorkerTier is a request-scoped speed/depth hint for a spawned worker. The
// empty value means absent; callers must not treat it as a valid tier.
type WorkerTier string

const (
	// TierFast favors the cheapest, quickest model and effort.
	TierFast WorkerTier = "fast"
	// TierStandard keeps the project or role defaults.
	TierStandard WorkerTier = "standard"
	// TierDeep favors the most capable model and effort.
	TierDeep WorkerTier = "deep"
)

// Valid reports whether t is one of the known worker tiers. The empty string
// is not valid.
func (t WorkerTier) Valid() bool {
	switch t {
	case TierFast, TierStandard, TierDeep:
		return true
	default:
		return false
	}
}
