package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/horazont/dragonstash/internal/cache"
	"github.com/horazont/dragonstash/internal/filecache"
	"github.com/horazont/dragonstash/internal/frontend"
	"github.com/horazont/dragonstash/internal/layer"
)

func writeMemProfile(fn string, sigs <-chan os.Signal) {
	i := 0
	for range sigs {
		fn := fmt.Sprintf("%s-%d.memprof", fn, i)
		i++

		log.Printf("Writing mem profile to %s\n", fn)
		f, err := os.Create(fn)
		if err != nil {
			log.Printf("Create: %v", err)
			continue
		}
		pprof.WriteHeapProfile(f)
		if err := f.Close(); err != nil {
			log.Printf("close %v", err)
		}
	}
}

func main() {
	cpuprofile := flag.String("profile", "", "record cpu profile.")
	memprofile := flag.String("mem-profile", "", "record memory profile.")
	flag.Parse()
	if flag.NArg() < 3 {
		fmt.Printf("usage: %s SOURCE CACHE MOUNTPOINT\n", path.Base(os.Args[0]))
		fmt.Printf("\noptions:\n")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if *cpuprofile != "" {
		fmt.Printf("Writing cpu profile to %s\n", *cpuprofile)
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Println(err)
			os.Exit(3)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		log.Printf("send SIGUSR1 to %d to dump memory profile", os.Getpid())
		profSig := make(chan os.Signal, 1)
		signal.Notify(profSig, syscall.SIGUSR1)
		go writeMemProfile(*memprofile, profSig)
	}
	if *cpuprofile != "" || *memprofile != "" {
		fmt.Fprintf(
			os.Stderr,
			"Note: You must unmount gracefully, otherwise the profile file(s) will stay empty!\n",
		)
	}

	cachedir := flag.Arg(1)
	mountpoint := flag.Arg(2)

	filecache := filecache.NewFileCache(cachedir)
	filecache.SetBlocksTotal(16)

	// back_fs := localfs.NewLocalFileSystem(flag.Arg(0))
	back_fs := layer.NewDefaultFileSystem()
	cache_layer := cache.NewCacheLayer(filecache, back_fs)
	front_fs := frontend.NewDragonStashFS(cache_layer)

	opts := &nodefs.Options{
		// These options are to be compatible with libfuse defaults,
		// making benchmarking easier.
		NegativeTimeout: time.Second,
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
	}
	// Enable ClientInodes so hard links work
	pathFsOpts := &pathfs.PathNodeFsOptions{ClientInodes: true}
	pathFs := pathfs.NewPathNodeFs(front_fs, pathFsOpts)

	conn := nodefs.NewFileSystemConnector(pathFs.Root(), opts)

	mOpts := &fuse.MountOptions{
		AllowOther: false,
		Name:       "test",
		FsName:     "test",
		Debug:      true,
	}
	state, err := fuse.NewServer(conn.RawFS(), mountpoint, mOpts)
	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Mounted!")
	state.Serve()

	filecache.Close()
}
