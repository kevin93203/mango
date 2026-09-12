// Package secrets resolves runtime-only secret references and redacts their
// values before they reach logs, history, or control-plane responses.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Reference struct {
	Provider string
	Name     string
}

func (r Reference) String() string {
	return r.Provider + ":" + r.Name
}

func (r Reference) Validate() error {
	r.Provider = strings.TrimSpace(r.Provider)
	r.Name = strings.TrimSpace(r.Name)
	if r.Provider == "" || r.Name == "" {
		return errors.New("secret reference requires a provider and name")
	}
	switch r.Provider {
	case "from_env":
		if !envNamePattern.MatchString(r.Name) {
			return fmt.Errorf("environment secret name %q is invalid", r.Name)
		}
	case "from_file":
		if filepath.IsAbs(r.Name) && filepath.Clean(r.Name) == "." {
			return errors.New("secret file path is invalid")
		}
	default:
		return fmt.Errorf("unsupported secret provider %q", r.Provider)
	}
	return nil
}

// Parse accepts the legacy webhook form env:NAME and the equivalent file:PATH
// form. YAML environment references use the explicit from_env/from_file
// providers, but both forms share the same runtime resolver.
func Parse(value string) (Reference, error) {
	value = strings.TrimSpace(value)
	provider, name, ok := strings.Cut(value, ":")
	if !ok {
		return Reference{}, errors.New("secret reference must use provider:name")
	}
	switch strings.TrimSpace(provider) {
	case "env":
		provider = "from_env"
	case "file":
		provider = "from_file"
	}
	ref := Reference{Provider: strings.TrimSpace(provider), Name: strings.TrimSpace(name)}
	if err := ref.Validate(); err != nil {
		return Reference{}, err
	}
	return ref, nil
}

func resolveEnv(name string) (string, error) {
	if !envNamePattern.MatchString(name) {
		return "", fmt.Errorf("environment secret name %q is invalid", name)
	}
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", fmt.Errorf("environment secret %q is not set", name)
	}
	return value, nil
}

func resolveFile(ctx context.Context, name string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return "", fmt.Errorf("read secret file %q: %w", name, err)
	}
	value := strings.TrimRight(string(data), "\r\n")
	if value == "" {
		return "", fmt.Errorf("secret file %q is empty", name)
	}
	return value, nil
}

func Resolve(ctx context.Context, ref Reference) (string, error) {
	if err := ref.Validate(); err != nil {
		return "", err
	}
	var (
		value string
		err   error
	)
	switch ref.Provider {
	case "from_env":
		value, err = resolveEnv(ref.Name)
	case "from_file":
		value, err = resolveFile(ctx, ref.Name)
	default:
		return "", fmt.Errorf("unsupported secret provider %q", ref.Provider)
	}
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", errors.New("secret provider returned an empty value")
	}
	return value, nil
}

// ResolveMap resolves only references; plain values are copied unchanged.
func ResolveMap(ctx context.Context, values map[string]string, refs map[string]Reference) (map[string]string, *Redactor, error) {
	result := make(map[string]string, len(values)+len(refs))
	for key, value := range values {
		result[key] = value
	}
	redactor := &Redactor{}
	keys := make([]string, 0, len(refs))
	for key := range refs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, err := Resolve(ctx, refs[key])
		if err != nil {
			return nil, nil, fmt.Errorf("resolve secret for %s: %w", key, err)
		}
		result[key] = value
		redactor.Add(value)
	}
	return result, redactor, nil
}

type Redactor struct {
	mu     sync.RWMutex
	values []string
}

func (r *Redactor) Add(value string) {
	if r == nil || value == "" {
		return
	}
	r.mu.Lock()
	for _, existing := range r.values {
		if existing == value {
			r.mu.Unlock()
			return
		}
	}
	r.values = append(r.values, value)
	sort.Slice(r.values, func(i, j int) bool { return len(r.values[i]) > len(r.values[j]) })
	r.mu.Unlock()
}

func (r *Redactor) Merge(other *Redactor) {
	if r == nil || other == nil {
		return
	}
	other.mu.RLock()
	values := append([]string(nil), other.values...)
	other.mu.RUnlock()
	for _, value := range values {
		r.Add(value)
	}
}

func (r *Redactor) Redact(value string) string {
	if r == nil || value == "" {
		return value
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, secret := range r.values {
		value = strings.ReplaceAll(value, secret, "[REDACTED]")
	}
	return value
}

func (r *Redactor) Wrap(writer io.Writer) io.Writer {
	if r == nil {
		return writer
	}
	return redactWriter{redactor: r, writer: writer}
}

type redactWriter struct {
	redactor *Redactor
	writer   io.Writer
}

func (w redactWriter) Write(data []byte) (int, error) {
	redacted := w.redactor.Redact(string(data))
	_, err := io.WriteString(w.writer, redacted)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}
