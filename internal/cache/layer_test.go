package cache

import "testing"

func assertEqualInt64(t *testing.T, a int64, b int64) {
	if a != b {
		t.Errorf("%d != %d", a, b)
	}
}

func TestAlignRead(t *testing.T) {
	var block_size int64 = 4096

	t.Run("fully aligned", func(t *testing.T) {
		var pos int64 = block_size
		var length int64 = block_size

		new_pos, new_len, offs := alignRead(pos, length, block_size)

		assertEqualInt64(t, pos, new_pos)
		assertEqualInt64(t, length, new_len)
		assertEqualInt64(t, offs, 0)
	})

	t.Run("unaligned position, block length", func(t *testing.T) {
		var pos int64 = 128
		var length int64 = block_size

		new_pos, new_len, offs := alignRead(pos, length, block_size)

		assertEqualInt64(t, new_pos, 0)
		assertEqualInt64(t, new_len, block_size*2)
		assertEqualInt64(t, offs, 128)
	})

	t.Run("unaligned position, block length after shift", func(t *testing.T) {
		var pos int64 = 128
		var length int64 = block_size - 128

		new_pos, new_len, offs := alignRead(pos, length, block_size)

		assertEqualInt64(t, new_pos, 0)
		assertEqualInt64(t, new_len, block_size)
		assertEqualInt64(t, offs, 128)
	})

	t.Run("aligned position, unaligned length", func(t *testing.T) {
		var pos int64 = 4096
		var length int64 = 63

		new_pos, new_len, offs := alignRead(pos, length, block_size)

		assertEqualInt64(t, new_pos, 4096)
		assertEqualInt64(t, new_len, 4096)
		assertEqualInt64(t, offs, 0)
	})

	t.Run("fully unaligned", func(t *testing.T) {
		var pos int64 = block_size*3 + 37
		var length int64 = 63

		new_pos, new_len, offs := alignRead(pos, length, block_size)

		assertEqualInt64(t, new_pos, block_size*3)
		assertEqualInt64(t, new_len, 4096)
		assertEqualInt64(t, offs, 37)
	})
}
