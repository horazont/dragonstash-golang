Inode Format
############

All numeric data is in **little endian**!

*iff* is not a typo and means "if and only if".

Inodes consist of an instance of the common inode format and zero or one of the
extension formats defined below. The extension format used depends on the
type/format of the inode as specified in ``mode`` (see ``man 2 stat``, search
for ``st_mode``).

Common inode format
===================

Inode info is stored in the following format:

1. 3 bytes magic number: ``0x69, 0x6e, 0x6f``  (== ASCII "``ino``")
2. 1 byte version number

Version 0x01
------------

1. 4 bytes ``mode``
2. 4 bytes ``uid``
3. 4 bytes ``gid``
4. 1 byte ``perms_modified`` flag (currently unused)
5. 8 bytes ``mtime``
6. 8 bytes ``atime``
7. 8 bytes ``ctime``
8. 1 byte ``times_modified`` flag (currently unused)
9. 8 bytes ``size``

Extension Formats
=================

The common inode format may be followed by one of the extension formats, based
on ``mode``.

Link inode extension format
---------------------------

The link inode extension is used *iff* ``mode&syscall.S_IFMT == syscall.S_IFLNK``.

1. 3 bytes magic number: ``0x4c, 0x4e, 0x4b`` (== ASCII "``LNK``")
2. 1 byte version number

Version 0x01
~~~~~~~~~~~~

1. 4 bytes ``length``
2. ``length`` bytes ``dest`` (link destination)

Note: version 1 does support link destinations with up to 2048 bytes. Longer
destinations will not be loaded.

Directory inode extension format
--------------------------------

The directory inode extension is used *iff* ``mode&syscall.S_IFMT == syscall.S_IFDIR``.

1. 3 bytes magic number: ``0x44, 0x49, 0x52`` (== ASCII "``DIR``")
2. 1 byte version number

Version 0x01
~~~~~~~~~~~~

1. 4 bytes ``nchildren``
2. ``nchildren`` times:

   a. 4 bytes ``length``
   b. ``length`` bytes ``name`` (directory entry name)

Note: version 1 does support up to 65535 children and up to 1024 bytes per entry
name.

File inode extension format
---------------------------

The directory inode extension is used *iff* ``mode&syscall.S_IFMT ==
syscall.S_IFREG``. Unlike other inodes which store all their information inside
the inode itself, file inodes are accompanied by a ``.data`` file which
contains the actual file data.

1. 3 bytes magic number: ``0x52, 0x45, 0x47`` (== ASCII "``REG``")
2. 1 byte version number

Version 0x01
~~~~~~~~~~~~

1. 8 bytes ``blocks_used``
2. padding up to (and excluding) offset ``0x80`` in the file. the number of
   padding bytes depends on the common inode version.
3. blockmap v1 (see below)

Blockmap v1
~~~~~~~~~~~

The blockmap consists of a blockinfo entry for each block in the file described
by the inode. A block is 4096 bytes long. Each blockinfo entry has 16 bits.
Thus, per 4096 bytes of described file, there will be one blockinfo entry with 2
bytes in size (overhead ~0.05% + static overhead of 128 bytes for inode info).

Each blockinfo entry is a uint16. It is composed as follows (the higher the bit
number, the greater the significance):

1. bit 15: dirty flag (currently unused) (``FLAG_DIRTY``)
2. bit 14–12: reserved flags (``FLAG_RSVD0`` .. ``FLAG_RSVD2``)
3. bit 11–8: reserved
4. bit 7–0: saturating access counter (``ACTR``)

The access counter (``ACTR``) is increased on each access of the block. When it
reaches its maximum value (255), it is not reset to zero.

In the future, the implementation may right-shift all access counters in a file
when an access counter reaches the maximum value to keep a good relative view on
the use of blocks. Counters with a value of 1 will retain that value despite
right-shifting.

The ``ACTR`` is non-zero *iff* the block is in fact available in the data file.