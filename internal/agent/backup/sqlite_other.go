//go:build !linux

package backup

import "os"

func sqliteSourceURI(*os.File, string) (string, error) { return "", ErrSQLitePlatform }
