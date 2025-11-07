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
	"time"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

type dirHandle = windows.Handle

const (
	dirQueryInitialBuffer  = 64 * 1024
	dirQueryMaxBuffer      = 1 << 30
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
	name  string
	mode  os.FileMode
	size  int64
	mtime time.Time
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

func (finder *Finder) walk(ctx context.Context, path string, dir dirHandle) {
	var entry dirEntry
	var info windows.ByHandleFileInformation
	err := windows.GetFileInformationByHandle(dir, &info)
	if err == nil {
		entry.mtime = filetimeToTime(info.LastWriteTime)
	} else {
		record := Record{
			Path:   path,
			Type:   'd',
			Errors: []error{fmt.Errorf("GetFileInformationByHandle failed: %w", err)},
		}
		select {
		case finder.out <- record:
		case <-ctx.Done():
			return
		}
	}

	finder._walk(ctx, path, dir, entry)
}

func (finder *Finder) _walk(ctx context.Context, path string, dir dirHandle, entry dirEntry) {
	defer windows.CloseHandle(dir)

	entries, err := enumerateDirectory(dir)
	record := Record{
		Path:  path,
		Type:  'd',
		MTime: entry.mtime,
	}
	if err != nil {
		record.Errors = append(record.Errors, err)
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
		record := Record{
			Path:  childPath(path, entry.name),
			Type:  type2rune(entry.mode),
			MTime: entry.mtime,
		}

		var subdir dirHandle
		if record.Type == 'd' {
			subdir, err = openRelativeDirectory(dir, entry.name)
			if err != nil {
				record.Errors = append(record.Errors, err)
			}
		}

		if record.Type == 'f' {
			record.Size = entry.size
		}

		if record.Type == 'd' && len(record.Errors) == 0 {
			childPath := record.Path
			handle := subdir
			went := finder.group.TryGo(func() error {
				finder._walk(ctx, childPath, handle, entry)
				return nil
			})
			if !went {
				finder._walk(ctx, childPath, handle, entry)
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

func enumerateDirectory(handle dirHandle) ([]dirEntry, error) {
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
				return nil, fmt.Errorf("NtQueryDirectoryFile: entry larger than %d bytes", bufSize)
			}
			bufSize *= 2
			if bufSize > dirQueryMaxBuffer {
				bufSize = dirQueryMaxBuffer
			}
			buffer = make([]byte, bufSize)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("NtQueryDirectoryFile failed: %w", err)
		}

		restart = false
		chunk := buffer[:n]
		offset := 0
		for offset < len(chunk) {
			if len(chunk[offset:]) < fileInfoHeaderSize {
				return nil, fmt.Errorf("NtQueryDirectoryFile returned truncated data")
			}
			info := (*fileFullDirInformation)(unsafe.Pointer(&chunk[offset]))
			nameBytes := int(info.FileNameLength)
			if nameBytes < 0 || nameBytes > len(chunk[offset:])-fileInfoHeaderSize {
				return nil, fmt.Errorf("NtQueryDirectoryFile returned truncated data")
			}
			nameLen := nameBytes / 2
			nameSlice := unsafe.Slice(&info.FileName[0], nameLen)
			name := string(utf16.Decode(nameSlice))
			if name != "." && name != ".." {
				entries = append(entries, dirEntry{
					name:  name,
					mode:  attributesToMode(info.FileAttributes),
					size:  info.EndOfFile,
					mtime: ntFiletimeToTime(info.LastWriteTime),
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
	return ntCreateRelative(
		parent,
		name,
		(windows.FILE_LIST_DIRECTORY | windows.SYNCHRONIZE),
		windows.FILE_ATTRIBUTE_DIRECTORY,
		(windows.FILE_DIRECTORY_FILE | windows.FILE_SYNCHRONOUS_IO_NONALERT |
			windows.FILE_OPEN_FOR_BACKUP_INTENT))
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
		return 0, fmt.Errorf("NtCreateFile failed: %w", err)
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

func filetimeToTime(ft windows.Filetime) time.Time {
	nsec := ft.Nanoseconds()
	if nsec <= 0 {
		return time.Time{}
	}
	return time.Unix(0, nsec)
}

func ntFiletimeToTime(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	u := uint64(value)
	ft := windows.Filetime{
		LowDateTime:  uint32(u & 0xffffffff),
		HighDateTime: uint32(u >> 32),
	}
	return filetimeToTime(ft)
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
