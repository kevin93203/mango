// Package artifact records metadata for declared task output files.
package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/kevin93203/mango/internal/scheduler"
)

// Collect inspects declared output paths relative to base. Missing, unreadable,
// non-regular, and symlink-escaped outputs are represented as Exists=false so
// artifact bookkeeping does not change the task's exit status.
func Collect(base string, paths []string) ([]scheduler.Artifact, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	absBase, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	result := make([]scheduler.Artifact, 0, len(paths))
	for _, value := range paths {
		relative := filepath.Clean(filepath.FromSlash(strings.ReplaceAll(value, "\\", "/")))
		item := scheduler.Artifact{Path: filepath.ToSlash(relative)}
		path := filepath.Join(absBase, relative)
		if !within(absBase, path) {
			result = append(result, item)
			continue
		}
		info, statErr := os.Lstat(path)
		if statErr != nil {
			result = append(result, item)
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
			resolved, resolveErr := filepath.EvalSymlinks(path)
			if resolveErr != nil || !within(absBase, resolved) {
				result = append(result, item)
				continue
			}
			info, statErr = os.Stat(resolved)
			if statErr != nil {
				result = append(result, item)
				continue
			}
			path = resolved
		}
		if !info.Mode().IsRegular() {
			result = append(result, item)
			continue
		}
		file, openErr := os.Open(path)
		if openErr != nil {
			result = append(result, item)
			continue
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			result = append(result, item)
			continue
		}
		item.Exists = true
		item.Size = info.Size()
		item.SHA256 = hex.EncodeToString(hash.Sum(nil))
		result = append(result, item)
	}
	return result, nil
}

func within(base, path string) bool {
	relative, err := filepath.Rel(base, path)
	if err != nil {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
