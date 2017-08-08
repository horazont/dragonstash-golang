DragonStash -- a Caching FUSE Overlay File System
#################################################

``DragonStash`` is a FUSE file system which implements a *transparent cache*
over any other mounted file system or SFTP server. It helps you produce a
Dragon’s stash of the finest media (or whatever is available at the source
you’re using), automatically.

Planned features
================

Stage 1
-------

* Local file system subtree as source
* Transparent caching of directories, symlinks, and files
* LRU-style eviction of old data when a quota is reached
* Use of cached data when source is offline ("not ready") as far as possible
  (throw Input/Output error otherwise: this is what normal devices do when data
  is not available e.g. due to corruption)


Stage 2
-------

* Block-wise caching instead of full-file caching
* Support for online writes and other changes to the file system


Stage 3
-------

* Direct use of SFTP as source (without sshfs inbetween)


Stage 4
-------

* Support for offline writes and other changes to the file system
