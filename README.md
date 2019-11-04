
**NOTE:** This repository is only for archive purposes. DragonStash has
since been [ported to C++](https://github.com/horazont/dragonstash-cpp).
This repository will see no further development.

DragonStash -- a Caching FUSE Overlay File System
===

**NOTE:** This repository is only for archive purposes. DragonStash has
since been [ported to C++](https://github.com/horazont/dragonstash-cpp).
This repository will see no further development.

`DragonStash` is a FUSE file system which implements a *transparent cache*
over any other mounted file system or SFTP server. It helps you produce a
Dragon’s stash of the finest media (or whatever is available at the source
you’re using), automatically.

Building
---

    $ make build

This project uses [``dep``](https://github.com/golang/dep) for dependencies.

Roadmap
---

`DragonStash` is in development. This means that a lot of things don’t work
yet. Here’s the roadmap and the current status.

**Done & Working**:

* Local directory tree as source file system (NB: you could mount something with
  sshfs and point dragonstash at that)
* Transparent caching of inodes (directories, symlinks, file metadata)
* Transparent block-wise caching of file contents
* Return EIO if cached data is missing and source isn’t available

**Planned**:

This list is in no particular order.

* Limit on number of blocks (4096 bytes each) used for the cache, evict unused blocks
* Support for fallocate to discard cached data
* SFTP server as source file system and automatic fallback
* Proper command-line interface for mounting, without requiring a rebuild to use
  the fallback
* Online write support
* Offline write support
* Online locking support
* (Unsafe) offline locking support
* Custom read-ahead / pre-caching styles
* Some interface to request files/directories to be cached/evicted
