package logging

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type RotatingWriter struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	size     int64
	maxSize  int64
	maxFiles int
}

func Open(path string, maxSize int64, maxFiles int) (*RotatingWriter, error) {
	if maxSize <= 0 {
		maxSize = 100 << 20
	}
	if maxFiles <= 0 {
		maxFiles = 10
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &RotatingWriter{file: file, path: path, size: info.Size(), maxSize: maxSize, maxFiles: maxFiles}, nil
}

func (w *RotatingWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return 0, errors.New("log writer is closed")
	}
	if w.size > 0 && w.size+int64(len(data)) > w.maxSize {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}
	n, err := w.file.Write(data)
	w.size += int64(n)
	return n, err
}

func (w *RotatingWriter) rotateLocked() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	for i := w.maxFiles - 1; i >= 1; i-- {
		oldPath := fmt.Sprintf("%s.%d", w.path, i)
		newPath := fmt.Sprintf("%s.%d", w.path, i+1)
		if i == w.maxFiles-1 {
			_ = os.Remove(newPath)
		}
		if err := os.Rename(oldPath, newPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.Rename(w.path, w.path+".1"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w.file = file
	w.size = 0
	return nil
}

func (w *RotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

type Manager struct {
	root string
}

func NewManager(root string) *Manager {
	return &Manager{root: root}
}

func (m *Manager) Open(project, process, stream string, maxSize int64, maxFiles int) (*RotatingWriter, string, error) {
	if stream != "stdout" && stream != "stderr" {
		return nil, "", fmt.Errorf("invalid log stream %q", stream)
	}
	path := filepath.Join(m.root, cleanPart(project), cleanPart(process), stream+".log")
	writer, err := Open(path, maxSize, maxFiles)
	return writer, path, err
}

func (m *Manager) Path(project, process, stream string) string {
	return filepath.Join(m.root, cleanPart(project), cleanPart(process), stream+".log")
}

func ReadSince(path string, offset int64, maxBytes int) (string, int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", 0, nil
	}
	if err != nil {
		return "", offset, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", offset, err
	}
	if offset < 0 {
		offset = info.Size()
	} else if offset > info.Size() {
		// The file may have been truncated or rotated since the previous
		// read. Start at the current end so follow mode does not replay the
		// existing contents as if they were new log output.
		offset = info.Size()
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return "", offset, err
	}
	if maxBytes <= 0 {
		maxBytes = 64 << 10
	}
	buf := make([]byte, maxBytes)
	n, err := file.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", offset, err
	}
	return string(buf[:n]), offset + int64(n), nil
}

func Tail(path string, lines int) (string, error) {
	if lines <= 0 {
		lines = 100
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	all := make([]string, 0)
	for scanner.Scan() {
		all = append(all, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	if len(all) == 0 {
		return "", nil
	}
	return strings.Join(all, "\n") + "\n", nil
}

func cleanPart(value string) string {
	value = filepath.Base(value)
	if value == "." || value == string(filepath.Separator) || value == "" {
		return "_"
	}
	return value
}
