package cache

import "testing"

func assertEqualStr(t *testing.T, s1 string, s2 string) {
	if s1 != s2 {
		t.Errorf("%#v != %#v", s1, s2)
	}
}

func TestInodePath(t *testing.T) {
	root := &inode{
		name:     "",
		children: make(map[string]*inode),
	}
	child1 := &inode{
		name:     "foo",
		parent:   root,
		children: make(map[string]*inode),
	}
	root.children["foo"] = child1
	child11 := &inode{
		name:   "d1",
		parent: child1,
	}
	child1.children["d1"] = child11
	child2 := &inode{
		name:   "bar",
		parent: root,
	}
	root.children["bar"] = child2

	t.Run("root", func(t *testing.T) {
		assertEqualStr(t, root.path(), "")
	})

	t.Run("subdir of root", func(t *testing.T) {
		assertEqualStr(t, child1.path(), "foo")
		assertEqualStr(t, child2.path(), "bar")
	})

	t.Run("second subdir", func(t *testing.T) {
		assertEqualStr(t, child11.path(), "foo/d1")
	})
}

func TestTruncateRead(t *testing.T) {
	t.Run("aligned read", func(t *testing.T) {
		assertEqualInt64(
			t,
			truncateRead(
				0,
				8192,
				0,
			),
			4096,
		)

		assertEqualInt64(
			t,
			truncateRead(
				0,
				4096*4,
				2,
			),
			4096*3,
		)

		assertEqualInt64(
			t,
			truncateRead(
				4096,
				4096*4,
				2,
			),
			4096*2,
		)
	})

	t.Run("unaligned read", func(t *testing.T) {
		assertEqualInt64(
			t,
			truncateRead(
				13,
				4100,
				0,
			),
			4096-13,
		)

		assertEqualInt64(
			t,
			truncateRead(
				7491,
				1000,
				1,
			),
			701,
		)
	})
}
