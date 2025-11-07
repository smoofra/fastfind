//go:build !windows

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

type dirHandle = int

func openDirHandle(path string) (dirHandle, error) {
	return unix.Open(path, unix.O_DIRECTORY, 0)
}

func (finder *Finder) walk(ctx context.Context, path string, dir dirHandle) {
	f := os.NewFile(uintptr(dir), path)
	defer f.Close()

	record := Record{
		Path: path,
		Type: 'd',
	}

	if finder.stat {
		var stat unix.Stat_t
		err := unix.Fstat(int(dir), &stat)
		if err == nil {
			record.MTime = timeFromSpec(stat.Mtim)
		} else {
			record.Errors = append(record.Errors, fmt.Errorf("Fstat failed: %w", err))
		}
	}

	entries, err := f.ReadDir(-1)
	if err != nil {
		record.Errors = append(record.Errors, fmt.Errorf("ReadDir failed: %w", err))
	}
	select {
	case finder.out <- record:
	case <-ctx.Done():
		return
	}
	if err != nil {
		return
	}

	for _, entry := range entries {
		name := entry.Name()
		record := Record{
			Path: childPath(path, name),
			Type: type2rune(entry.Type()),
		}

		var subdir dirHandle
		if record.Type == 'd' {
			subdir, err = unix.Openat(int(dir), name, unix.O_DIRECTORY, 0)
			if err != nil {
				record.Errors = append(record.Errors, fmt.Errorf("openat failed: %w", err))
			}
		}

		if finder.stat && record.Type != 'd' {
			var stat unix.Stat_t
			err = unix.Fstatat(int(dir), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
			if err != nil {
				record.Errors = append(record.Errors, fmt.Errorf("fstatat failed: %w", err))
			} else {
				record.Size = stat.Size
				record.MTime = timeFromSpec(stat.Mtim)
			}
		}

		if record.Type == 'd' && len(record.Errors) == 0 {
			childPath := record.Path
			handle := subdir
			went := finder.group.TryGo(func() error {
				finder.walk(ctx, childPath, handle)
				return nil
			})
			if !went {
				finder.walk(ctx, childPath, handle)
			}
		} else {
			select {
			case finder.out <- record:
			case <-ctx.Done():
				return
			}
		}
	}
}

func timeFromSpec(ts unix.Timespec) time.Time {
	return time.Unix(int64(ts.Sec), int64(ts.Nsec))
}
