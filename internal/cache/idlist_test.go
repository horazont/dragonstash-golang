package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testIDList() *IDList {
	result := NewEmptyIDList()
	result.AddBlock(1, 1024)
	return result
}

func TestCount(t *testing.T) {
	list := testIDList()
	assert.Equal(t, list.Count(), uint64(1024))
}

func TestAlloc(t *testing.T) {
	list := testIDList()

	for i := uint64(1); i < uint64(1024); i += uint64(1) {
		allocated, err := list.Alloc()
		assert.Nil(t, err)
		assert.Equal(t, allocated, i)
		assert.Equal(t, list.Count(), uint64(1024)-i)
	}
}

func TestAllocOutOfIDs(t *testing.T) {
	list := NewEmptyIDList()
	list.AddBlock(1, 1)

	allocated, err := list.Alloc()
	assert.Nil(t, err)
	assert.Equal(t, allocated, uint64(1))
	assert.Equal(t, list.Count(), uint64(0))

	allocated, err = list.Alloc()
	assert.NotNil(t, err)
}

func TestReleaseSequential(t *testing.T) {
	list := testIDList()

	list.Release(1025)
	assert.Equal(t, list.Count(), uint64(1025))
	assert.Equal(t, len(list.segments), 1)
	assert.Equal(t, list.segments[0].start, uint64(1))
	assert.Equal(t, list.segments[0].end, uint64(1025))
}

func TestReleaseOutOfSequence(t *testing.T) {
	list := testIDList()

	list.Release(1026)
	assert.Equal(t, list.Count(), uint64(1025))
	assert.Equal(t, len(list.segments), 2)
	assert.Equal(t, list.segments[0].start, uint64(1))
	assert.Equal(t, list.segments[1].start, uint64(1026))
	assert.Equal(t, list.segments[1].end, uint64(1026))
}

func TestReleasePreSequence(t *testing.T) {
	list := testIDList()

	list.Alloc()
	list.Alloc()

	list.Release(1)
	assert.Equal(t, list.Count(), uint64(1023))
	assert.Equal(t, len(list.segments), 2)
	assert.Equal(t, list.segments[0].start, uint64(1))
	assert.Equal(t, list.segments[0].end, uint64(1))
	assert.Equal(t, list.segments[1].start, uint64(3))
	assert.Equal(t, list.segments[1].end, uint64(1024))
}

func TestReleaseAtStartOfSegment(t *testing.T) {
	list := testIDList()

	list.Alloc()
	list.Alloc()

	list.Release(2)
	assert.Equal(t, list.Count(), uint64(1023))
	assert.Equal(t, len(list.segments), 1)
	assert.Equal(t, list.segments[0].start, uint64(2))
	assert.Equal(t, list.segments[0].end, uint64(1024))
}

func TestReleaseBetween(t *testing.T) {
	list := testIDList()

	list.Release(1026)
	list.Release(1025)
	assert.Equal(t, list.Count(), uint64(1026))
	assert.Equal(t, len(list.segments), 1)
	assert.Equal(t, list.segments[0].start, uint64(1))
	assert.Equal(t, list.segments[0].end, uint64(1026))
}

func TestReleaseConflictStartOfSegment(t *testing.T) {
	list := testIDList()
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()

	list.Release(1)
	t.Error("no error raised")
}

func TestReleaseConflictInsideSegment(t *testing.T) {
	list := testIDList()
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()

	list.Release(1026)
	list.Release(1027)

	list.Release(1023)
	t.Error("no error raised")
}

func TestReleaseConflictEndOfSegment(t *testing.T) {
	list := testIDList()
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()

	list.Release(1026)
	list.Release(1027)

	list.Release(1024)
	t.Error("no error raised")
}

func TestAddBlockNonConsecutiveBehind(t *testing.T) {
	list := NewEmptyIDList()

	err := list.AddBlock(0, 99)
	assert.Nil(t, err)

	err = list.AddBlock(200, 299)
	assert.Nil(t, err)

	assert.Equal(t, list.Count(), uint64(200))
	assert.Equal(t, len(list.segments), 2)
	assert.Equal(t, list.segments[0].start, uint64(0))
	assert.Equal(t, list.segments[0].end, uint64(99))
	assert.Equal(t, list.segments[1].start, uint64(200))
	assert.Equal(t, list.segments[1].end, uint64(299))
}

func TestAddBlockNonConsecutiveInFrontOf(t *testing.T) {
	list := NewEmptyIDList()

	err := list.AddBlock(200, 299)
	assert.Nil(t, err)

	err = list.AddBlock(0, 99)
	assert.Nil(t, err)

	assert.Equal(t, list.Count(), uint64(200))
	assert.Equal(t, len(list.segments), 2)
	assert.Equal(t, list.segments[0].start, uint64(0))
	assert.Equal(t, list.segments[0].end, uint64(99))
	assert.Equal(t, list.segments[1].start, uint64(200))
	assert.Equal(t, list.segments[1].end, uint64(299))
}

func TestAddBlockConsecutiveBehind(t *testing.T) {
	list := NewEmptyIDList()

	err := list.AddBlock(0, 99)
	assert.Nil(t, err)

	err = list.AddBlock(100, 199)
	assert.Nil(t, err)

	assert.Equal(t, list.Count(), uint64(200))
	assert.Equal(t, len(list.segments), 1)
	assert.Equal(t, list.segments[0].start, uint64(0))
	assert.Equal(t, list.segments[0].end, uint64(199))
}

func TestAddBlockConsecutiveInFrontOf(t *testing.T) {
	list := NewEmptyIDList()

	err := list.AddBlock(100, 199)
	assert.Nil(t, err)

	err = list.AddBlock(0, 99)
	assert.Nil(t, err)

	assert.Equal(t, list.Count(), uint64(200))
	assert.Equal(t, len(list.segments), 1)
	assert.Equal(t, list.segments[0].start, uint64(0))
	assert.Equal(t, list.segments[0].end, uint64(199))
}

func TestAddBlockInBetween(t *testing.T) {
	list := NewEmptyIDList()

	err := list.AddBlock(0, 99)
	assert.Nil(t, err)

	err = list.AddBlock(200, 299)
	assert.Nil(t, err)

	err = list.AddBlock(100, 199)
	assert.Nil(t, err)

	assert.Equal(t, list.Count(), uint64(300))
	assert.Equal(t, len(list.segments), 1)
	assert.Equal(t, list.segments[0].start, uint64(0))
	assert.Equal(t, list.segments[0].end, uint64(299))
}

func TestAddBlockConflictOverlapStart(t *testing.T) {
	list := NewEmptyIDList()

	err := list.AddBlock(0, 99)
	assert.Nil(t, err)

	err = list.AddBlock(200, 299)
	assert.Nil(t, err)

	err = list.AddBlock(199, 200)
	assert.NotNil(t, err)

	assert.Equal(t, list.Count(), uint64(200))
	assert.Equal(t, len(list.segments), 2)
	assert.Equal(t, list.segments[0].start, uint64(0))
	assert.Equal(t, list.segments[0].end, uint64(99))
	assert.Equal(t, list.segments[1].start, uint64(200))
	assert.Equal(t, list.segments[1].end, uint64(299))
}

func TestAddBlockConflictOverlapEnd(t *testing.T) {
	list := NewEmptyIDList()

	err := list.AddBlock(0, 99)
	assert.Nil(t, err)

	err = list.AddBlock(200, 299)
	assert.Nil(t, err)

	err = list.AddBlock(90, 100)
	assert.NotNil(t, err)

	assert.Equal(t, list.Count(), uint64(200))
	assert.Equal(t, len(list.segments), 2)
	assert.Equal(t, list.segments[0].start, uint64(0))
	assert.Equal(t, list.segments[0].end, uint64(99))
	assert.Equal(t, list.segments[1].start, uint64(200))
	assert.Equal(t, list.segments[1].end, uint64(299))
}

func TestAddBlockContained(t *testing.T) {
	list := NewEmptyIDList()

	err := list.AddBlock(400, 499)
	assert.Nil(t, err)

	err = list.AddBlock(200, 299)
	assert.Nil(t, err)

	err = list.AddBlock(0, 99)
	assert.Nil(t, err)

	err = list.AddBlock(210, 220)
	assert.NotNil(t, err)

	assert.Equal(t, list.Count(), uint64(300))
	assert.Equal(t, len(list.segments), 3)
	assert.Equal(t, list.segments[0].start, uint64(0))
	assert.Equal(t, list.segments[0].end, uint64(99))
	assert.Equal(t, list.segments[1].start, uint64(200))
	assert.Equal(t, list.segments[1].end, uint64(299))
	assert.Equal(t, list.segments[2].start, uint64(400))
	assert.Equal(t, list.segments[2].end, uint64(499))
}

func TestAddBlockAround(t *testing.T) {
	list := NewEmptyIDList()

	err := list.AddBlock(400, 499)
	assert.Nil(t, err)

	err = list.AddBlock(200, 299)
	assert.Nil(t, err)

	err = list.AddBlock(0, 99)
	assert.Nil(t, err)

	err = list.AddBlock(199, 300)
	assert.NotNil(t, err)

	assert.Equal(t, list.Count(), uint64(300))
	assert.Equal(t, len(list.segments), 3)
	assert.Equal(t, list.segments[0].start, uint64(0))
	assert.Equal(t, list.segments[0].end, uint64(99))
	assert.Equal(t, list.segments[1].start, uint64(200))
	assert.Equal(t, list.segments[1].end, uint64(299))
	assert.Equal(t, list.segments[2].start, uint64(400))
	assert.Equal(t, list.segments[2].end, uint64(499))
}
