//go:build linux

package backup

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

var sqliteSourceVFS sqlite3.Tsqlite3_vfs

// Keep the directory descriptor in the SQLite path. The Unix VFS otherwise
// resolves /proc links to a path that an app can replace during the copy.
// All locking, WAL access, and final-component O_NOFOLLOW checks stay in SQLite.
var registerSQLiteSource = sync.OnceValues(func() (string, error) {
	tls := libc.NewTLS()
	defer tls.Close()
	unix, err := libc.CString("unix")
	if err != nil {
		return "", ErrSource
	}
	defer libc.Xfree(tls, unix)
	base := sqlite3.Xsqlite3_vfs_find(tls, unix)
	if base == 0 {
		return "", ErrSource
	}
	name := "synod-pinned-source"
	cname, err := libc.CString(name)
	if err != nil {
		return "", ErrSource
	}
	libc.Xmemcpy(tls, uintptr(unsafe.Pointer(&sqliteSourceVFS)), base, uint64(unsafe.Sizeof(sqliteSourceVFS)))
	sqliteSourceVFS.FpNext = 0
	sqliteSourceVFS.FzName = cname
	*(*func(*libc.TLS, uintptr, uintptr, int32, uintptr) int32)(unsafe.Pointer(&sqliteSourceVFS.FxFullPathname)) = sqliteSourcePath
	if sqlite3.Xsqlite3_vfs_register(tls, uintptr(unsafe.Pointer(&sqliteSourceVFS)), 0) != sqlite3.SQLITE_OK {
		libc.Xfree(tls, cname)
		return "", ErrSource
	}
	// SQLite keeps the VFS and its name for the life of this process.
	return name, nil
})

func sqliteSourcePath(tls *libc.TLS, _ uintptr, input uintptr, size int32, output uintptr) int32 {
	path := libc.GoString(input)
	if !strings.HasPrefix(path, "/proc/self/fd/") || len(path) >= int(size) {
		return sqlite3.SQLITE_CANTOPEN
	}
	libc.Xstrncpy(tls, output, input, uint64(size))
	return sqlite3.SQLITE_OK
}

func sqliteSourceURI(parent *os.File, name string) (string, error) {
	vfs, err := registerSQLiteSource()
	if err != nil {
		return "", err
	}
	if name != filepath.Base(name) || name == "." || name == ".." {
		return "", ErrSource
	}
	location := url.URL{Scheme: "file", Path: fmt.Sprintf("/proc/self/fd/%d/%s", parent.Fd(), name)}
	query := url.Values{"mode": {"ro"}, "vfs": {vfs}, "_pragma": {"busy_timeout(1000)", "trusted_schema(OFF)"}}
	location.RawQuery = query.Encode()
	return location.String(), nil
}
