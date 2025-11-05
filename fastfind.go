package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

type ExitCode = int
type FD = int

const (
	ExitError ExitCode = 1
	ExitOk    ExitCode = 0
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s DIR [ -size ]\n", filepath.Base(os.Args[0]))
	os.Exit(1)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(ExitError)
}

type Record struct {
	Path  string
	Type  rune
	Size  int64
	Error error
}

type Finder struct {
	group *errgroup.Group
	out   chan<- Record
}

func type2rune(t os.FileMode) rune {
	switch t.Type() {
	case os.ModeDir:
		return 'd'
	case os.ModeSymlink:
		return 'l'
	case os.ModeDevice:
		return 'D'
	case os.ModeNamedPipe:
		return 'p'
	case os.ModeSocket:
		return 'S'
	case os.ModeCharDevice:
		return 'c'
	case os.ModeIrregular:
		return '?'
	default:
		return 'f'
	}
}

func (finder *Finder) walk(ctx context.Context, path string, dir FD) error {
	f := os.NewFile(uintptr(dir), path)
	defer f.Close()
	entries, err := f.ReadDir(-1)
	if err != nil {
		return fmt.Errorf("ReadDir failed for %s: %w", path, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		record := Record{
			Path: path + "/" + name,
			Type: type2rune(entry.Type()),
		}

		var subdir FD
		if record.Type == 'd' {
			subdir, err = unix.Openat(dir, name, unix.O_DIRECTORY, 0)
			if err != nil {
				record.Error = fmt.Errorf("openat failed: %w", err)
			}
		}

		if getSize && record.Type == 'f' {
			var stat unix.Stat_t
			err = unix.Fstatat(dir, name, &stat, unix.AT_SYMLINK_NOFOLLOW)
			if err != nil {
				record.Error = fmt.Errorf("fstatat failed: %w", err)
			} else {
				record.Size = stat.Size
			}
		}

		select {
		case finder.out <- record:
		case <-ctx.Done():
			return ctx.Err()
		}
		if record.Type == 'd' && record.Error == nil {
			went := finder.group.TryGo(func() error {
				return finder.walk(ctx, record.Path, subdir)
			})
			if !went {
				err = finder.walk(ctx, record.Path, subdir)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

var quote_replacer *strings.Replacer = strings.NewReplacer(`"`, `\"`, "\n", `\n`, "\t", `\t`, "\r", `\r`, "\\", "\\\\")

func quote(s string) string {
	if !strings.ContainsAny(s, "\"\n\r\t,") {
		return s
	}
	return `"` + quote_replacer.Replace(s) + `"`
}

var getSize bool

func main() {

	flag.BoolVar(&getSize, "size", false, "get sizes")
	flag.Usage = usage
	flag.Parse()

	var path string
	switch len(flag.Args()) {
	case 0:
		path = "."
	case 1:
		path = flag.Args()[0]
	default:
		usage()
	}
	path = strings.TrimRight(path, "/")

	// set up a top-level context with ^C handling
	ctx, cancel := context.WithCancel(context.Background())
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
	go func() {
		<-signalChannel
		cancel()
	}()

	const limit = 512

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(limit)

	records := make(chan Record, limit*4)

	finder := Finder{
		group: g,
		out:   records,
	}

	root, err := unix.Open(path, unix.O_DIRECTORY, 0)
	if err != nil {
		fail(err)
	}

	g.Go(func() error {
		err := finder.walk(ctx, path, root)
		return err
	})

	go func() {
		g.Wait()
		close(records)
	}()

	for {
		record, ok := <-records
		if !ok {
			break
		}

		if getSize {
			if record.Error != nil {
				fmt.Printf("%s\t%c\t\t%s\n", quote(record.Path), record.Type, quote(record.Error.Error()))
			} else if record.Type == 'f' {
				fmt.Printf("%s\t%c\t%d\n", quote(record.Path), record.Type, record.Size)
			} else {
				fmt.Printf("%s\t%c\n", quote(record.Path), record.Type)
			}
		} else {
			if record.Error != nil {
				fmt.Printf("%s\t%c\t%s\n", quote(record.Path), record.Type, quote(record.Error.Error()))
			} else {
				fmt.Printf("%s\t%c\n", quote(record.Path), record.Type)
			}
		}

	}

	err = g.Wait()
	if err != nil {
		fail(err)
	}

	os.Exit(ExitOk)
}
