package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

type ExitCode = int

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
	Path   string
	Type   rune
	Size   int64
	MTime  time.Time
	Errors []error
}

type Finder struct {
	group *errgroup.Group
	out   chan<- Record
	stat  bool
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

func childPath(base, name string) string {
	if base == "." {
		return "." + string(os.PathSeparator) + name
	}
	return filepath.Join(base, name)
}

func joinErrors(errors []error) string {
	var builder strings.Builder
	for i, err := range errors {
		if i != 0 {
			builder.WriteString("; ")
		}
		builder.WriteString(err.Error())
	}
	return builder.String()
}

func main() {

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

	flag.BoolVar(&finder.stat, "stat", false, "get file metadata")
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
	path = filepath.Clean(path)

	root, err := openDirHandle(path)
	if err != nil {
		fail(err)
	}

	g.Go(func() error {
		finder.walk(ctx, path, root)
		return nil
	})

	go func() {
		g.Wait()
		close(records)
	}()

	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	row := make([]string, 0, 16)

	row = append(row, "Path", "Type")
	if finder.stat {
		row = append(row, "Size", "MTime")
	}
	row = append(row, "Error")

	writer.Write(row)
	if err != nil {
		fail(err)
	}

	ok := true

	for {
		record, ok := <-records
		if !ok {
			break
		}

		row = row[:0]

		row = append(row, record.Path)
		row = append(row, string(record.Type))

		if finder.stat {
			if len(record.Errors) == 0 && record.Type == 'f' {
				row = append(row, strconv.FormatInt(record.Size, 10))
			} else {
				row = append(row, "")
			}
			if !record.MTime.IsZero() {
				row = append(row, record.MTime.Format("2006-01-02 15:04:05.999999999 -0700"))
			} else {
				row = append(row, "")
			}
		}

		if len(record.Errors) != 0 {
			row = append(row, joinErrors(record.Errors))
			ok = false
		}

		for len(row) > 1 && row[len(row)-1] == "" {
			row = row[:len(row)-1]
		}

		err := writer.Write(row)
		if err != nil {
			fail(err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		fail(err)
	}

	err = g.Wait()
	if err != nil {
		fail(err)
	}

	if !ok {
		os.Exit(ExitError)
	}
}
