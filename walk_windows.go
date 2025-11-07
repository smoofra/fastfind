//go:build windows

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

type dirHandle = windows.Handle

const (
	dirQueryInitialBuffer  = 64 * 1024
	dirQueryMaxBuffer      = 1 << 20
	fileFullDirectoryClass = 2 // FileFullDirectoryInformation
	fileInfoHeaderSize     = int(unsafe.Offsetof(fileFullDirInformation{}.FileName))
)

var (
	modntdll                 = windows.NewLazySystemDLL("ntdll.dll")
	procNtQueryDirectoryFile = modntdll.NewProc("NtQueryDirectoryFile")
	errDirBufferTooSmall     = errors.New("fastfind: directory entry exceeds query buffer")
)

type fileFullDirInformation struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    int64
	LastAccessTime  int64
	LastWriteTime   int64
	ChangeTime      int64
	EndOfFile       int64
	AllocationSize  int64
	FileAttributes  uint32
	FileNameLength  uint32
	EaSize          uint32
	FileName        [1]uint16
}

type dirEntry struct {
	name string
	mode os.FileMode
	size int64
}

func openDirHandle(path string) (dirHandle, error) {
	native := nativePath(path)
	p, err := windows.UTF16PtrFromString(native)
	if err != nil {
		return 0, err
	}

	handle, err := windows.CreateFile(
		p,
		windows.FILE_LIST_DIRECTORY|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return 0, err
	}
	return handle, nil
}

func (finder *Finder) walk(ctx context.Context, path string, dir dirHandle) error {
	defer windows.CloseHandle(dir)

	entries, err := enumerateDirectory(dir, path)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		child := childPath(path, entry.name)
		record := Record{
			Path: child,
			Type: type2rune(entry.mode),
		}

		var subdir dirHandle
		if record.Type == 'd' {
			subdir, err = openRelativeDirectory(dir, entry.name)
			if err != nil {
				record.Error = fmt.Errorf("open child directory failed: %w", err)
			}
		}

		if getSize && record.Type == 'f' {
			record.Size = entry.size
		}

		select {
		case finder.out <- record:
		case <-ctx.Done():
			return ctx.Err()
		}

		if record.Type == 'd' && record.Error == nil {
			childPathCopy := child
			handle := subdir
			went := finder.group.TryGo(func() error {
				return finder.walk(ctx, childPathCopy, handle)
			})
			if !went {
				if err := finder.walk(ctx, childPathCopy, handle); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func enumerateDirectory(handle dirHandle, path string) ([]dirEntry, error) {
	bufSize := dirQueryInitialBuffer
	if bufSize < fileInfoHeaderSize+2 {
		bufSize = fileInfoHeaderSize + 2
	}
	buffer := make([]byte, bufSize)
	restart := true
	entries := make([]dirEntry, 0, 128)

	for {
		n, status, err := ntQueryDirectory(handle, buffer, restart)
		if err == io.EOF {
			break
		}
		if errors.Is(err, errDirBufferTooSmall) {
			if bufSize >= dirQueryMaxBuffer {
				return nil, fmt.Errorf("NtQueryDirectoryFile(%s): entry larger than %d bytes", path, bufSize)
			}
			bufSize *= 2
			if bufSize > dirQueryMaxBuffer {
				bufSize = dirQueryMaxBuffer
			}
			buffer = make([]byte, bufSize)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("NtQueryDirectoryFile(%s) failed: %w", path, err)
		}

		restart = false
		chunk := buffer[:n]
		offset := 0
		for offset < len(chunk) {
			if len(chunk[offset:]) < fileInfoHeaderSize {
				break
			}
			info := (*fileFullDirInformation)(unsafe.Pointer(&chunk[offset]))
			nameBytes := int(info.FileNameLength)
			if nameBytes < 0 || nameBytes > len(chunk[offset:])-fileInfoHeaderSize {
				break
			}
			nameLen := nameBytes / 2
			nameSlice := unsafe.Slice(&info.FileName[0], nameLen)
			name := string(utf16.Decode(nameSlice))
			if name != "." && name != ".." {
				entries = append(entries, dirEntry{
					name: name,
					mode: attributesToMode(info.FileAttributes),
					size: info.EndOfFile,
				})
			}
			if info.NextEntryOffset == 0 {
				break
			}
			offset += int(info.NextEntryOffset)
		}

		if status == windows.STATUS_NO_MORE_FILES {
			break
		}
	}

	return entries, nil
}

func ntQueryDirectory(handle dirHandle, buffer []byte, restart bool) (uint32, windows.NTStatus, error) {
	if len(buffer) == 0 {
		return 0, 0, errDirBufferTooSmall
	}

	var iosb windows.IO_STATUS_BLOCK
	restartFlag := uintptr(0)
	if restart {
		restartFlag = 1
	}

	r0, _, _ := procNtQueryDirectoryFile.Call(
		uintptr(handle),
		0,
		0,
		0,
		uintptr(unsafe.Pointer(&iosb)),
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(uint32(len(buffer))),
		uintptr(fileFullDirectoryClass),
		0,
		0,
		restartFlag,
	)
	status := windows.NTStatus(r0)

	switch status {
	case windows.STATUS_SUCCESS, windows.STATUS_BUFFER_OVERFLOW, windows.STATUS_BUFFER_TOO_SMALL, windows.STATUS_INFO_LENGTH_MISMATCH:
		if iosb.Information == 0 {
			return 0, status, errDirBufferTooSmall
		}
		return uint32(iosb.Information), status, nil
	case windows.STATUS_NO_MORE_FILES:
		return 0, status, io.EOF
	default:
		return 0, status, status.Errno()
	}
}

func openRelativeDirectory(parent dirHandle, name string) (dirHandle, error) {
	return ntCreateRelative(parent, name, windows.FILE_LIST_DIRECTORY|windows.SYNCHRONIZE, windows.FILE_ATTRIBUTE_DIRECTORY, windows.FILE_DIRECTORY_FILE|windows.FILE_SYNCHRONOUS_IO_NONALERT|windows.FILE_OPEN_FOR_BACKUP_INTENT)
}

func ntCreateRelative(parent dirHandle, name string, access uint32, attributes uint32, options uint32) (dirHandle, error) {
	name16, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}

	var unicode windows.NTUnicodeString
	windows.RtlInitUnicodeString(&unicode, name16)

	oa := windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: windows.Handle(parent),
		ObjectName:    &unicode,
		Attributes:    windows.OBJ_CASE_INSENSITIVE,
	}

	var iosb windows.IO_STATUS_BLOCK
	var handle windows.Handle
	err = windows.NtCreateFile(
		&handle,
		access,
		&oa,
		&iosb,
		nil,
		attributes,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		options,
		0,
		0,
	)
	if err != nil {
		return 0, err
	}
	return handle, nil
}

func attributesToMode(attrs uint32) os.FileMode {
	if attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return os.ModeSymlink
	}
	mode := os.FileMode(0)
	if attrs&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir
	}
	if attrs&windows.FILE_ATTRIBUTE_DEVICE != 0 {
		mode |= os.ModeDevice
	}
	return mode
}

func nativePath(p string) string {
	if p == "" {
		return "."
	}
	if strings.HasPrefix(p, `\\?\`) || strings.HasPrefix(p, `\??\`) {
		return p
	}
	cleaned := filepath.Clean(p)
	cleaned = filepath.FromSlash(cleaned)
	if filepath.IsAbs(cleaned) {
		if strings.HasPrefix(cleaned, `\\`) {
			return `\\?\UNC\` + cleaned[2:]
		}
		return `\\?\` + cleaned
	}
	return cleaned
}
