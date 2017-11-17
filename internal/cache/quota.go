package cache

const (
	QUOTA_BLOCK_PRIO_WRITTEN   = iota
	QUOTA_BLOCK_PRIO_READ      = iota
	QUOTA_BLOCK_PRIO_READAHEAD = iota
)

// A QuotaService manages requests for blocks and inodes.
type QuotaService interface {
	// Request a number of blocks.
	//
	// Returns the number of blocks which were granted. This operation may
	// be expensive in terms of run time if the QuotaService has to request
	// and execute eviction of some lower priority blocks if not enough
	// blocks are available.
	//
	// The number of granted blocks may be any number between zero and
	// nblocks (both inclusive). The requester must not use more than the
	// number of granted blocks.
	//
	// It may request additional blocks at a later time. Blocks stay
	// assigned until released with ReleaseBlocks.
	RequestBlocks(nblocks uint64, priority int) (granted uint64)

	// Release a number of blocks.
	//
	// This operation always succeeds. The blocks are handed back to the
	// unused pool.
	ReleaseBlocks(nblocks uint64)
}

type QuotaInfo struct {
	BlocksTotal uint64
	BlocksUsed  uint64
	InodesTotal uint64
	InodesUsed  uint64
}
