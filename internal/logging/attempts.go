package logging

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// AttemptPath returns the path for one stream of one logical execution
// attempt. basePath is the corresponding aggregate execution log path.
func AttemptPath(basePath, ownerID string, attemptNumber int, stream string) string {
	if attemptNumber < 1 {
		attemptNumber = 1
	}
	if ownerID == "" {
		ownerID = "legacy"
	}
	return filepath.Join(filepath.Dir(basePath), "attempts", cleanPart(ownerID), strconv.Itoa(attemptNumber), stream+".log")
}

// ReadRetained reads a log and all of its numeric rotation files in logical
// order. RotatingWriter names the oldest retained file with the largest
// numeric suffix, so .N is read before .N-1 and the current file is read last.
// The boolean reports whether any matching file exists.
func ReadRetained(path string) (string, bool, error) {
	entries, err := os.ReadDir(filepath.Dir(path))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	base := filepath.Base(path)
	prefix := base + "."
	rotations := make([]retainedPart, 0)
	found := false
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			continue
		}
		if name == base {
			found = true
			continue
		}
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suffix := strings.TrimPrefix(name, prefix)
		number, parseErr := strconv.Atoi(suffix)
		if parseErr != nil || number < 1 {
			continue
		}
		found = true
		rotations = append(rotations, retainedPart{path: filepath.Join(filepath.Dir(path), name), number: number})
	}
	if !found {
		return "", false, nil
	}

	sort.Slice(rotations, func(i, j int) bool {
		return rotations[i].number > rotations[j].number
	})
	var data bytes.Buffer
	for _, part := range rotations {
		content, readErr := os.ReadFile(part.path)
		if readErr != nil {
			return "", true, fmt.Errorf("read rotated log %s: %w", part.path, readErr)
		}
		_, _ = data.Write(content)
	}
	current, readErr := os.ReadFile(path)
	if errors.Is(readErr, os.ErrNotExist) {
		return data.String(), true, nil
	}
	if readErr != nil {
		return "", true, fmt.Errorf("read log %s: %w", path, readErr)
	}
	_, _ = data.Write(current)
	return data.String(), true, nil
}

type retainedPart struct {
	path   string
	number int
}
