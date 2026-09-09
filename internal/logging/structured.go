package logging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type JSONLogger struct {
	mu       sync.Mutex
	file     *os.File
	redactor func(string) string
}

func NewJSONLogger(path string, redactor func(string) string) (*JSONLogger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &JSONLogger{file: file, redactor: redactor}, nil
}

func (l *JSONLogger) Log(level, message string, fields map[string]interface{}) error {
	if l == nil || l.file == nil {
		return nil
	}
	entry := map[string]interface{}{"timestamp": time.Now().UTC(), "level": level, "message": message}
	for key, value := range fields {
		entry[key] = value
	}
	if l.redactor != nil {
		for key, value := range entry {
			entry[key] = redactJSONValue(value, l.redactor)
		}
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if l.redactor != nil {
		data = []byte(l.redactor(string(data)))
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err = l.file.Write(append(data, '\n'))
	return err
}

func redactJSONValue(value interface{}, redactor func(string) string) interface{} {
	switch item := value.(type) {
	case string:
		return redactor(item)
	case map[string]interface{}:
		for key, nested := range item {
			item[key] = redactJSONValue(nested, redactor)
		}
	case []interface{}:
		for index, nested := range item {
			item[index] = redactJSONValue(nested, redactor)
		}
	}
	return value
}

func (l *JSONLogger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}
