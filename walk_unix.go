//go:build !windows

package main

import (
	"context"
	"fmt"
	"os"

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
	entries, err := f.ReadDir(-1)
	if err != nil {
		record.Error = fmt.Errorf("ReadDir failed: %w", err)
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
				record.Error = fmt.Errorf("openat failed: %w", err)
			}
		}

		if getSize && record.Type == 'f' {
			var stat unix.Stat_t
			err = unix.Fstatat(int(dir), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
			if err != nil {
				record.Error = fmt.Errorf("fstatat failed: %w", err)
			} else {
				record.Size = stat.Size
			}
		}

		if record.Type == 'd' && record.Error == nil {
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
