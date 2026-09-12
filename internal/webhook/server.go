// Package webhook implements the optional authenticated HTTP trigger surface.
package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/secrets"
)

const (
	SignatureHeader   = "X-Mango-Signature"
	TimestampHeader   = "X-Mango-Timestamp"
	IdempotencyHeader = "Idempotency-Key"
)

type Config struct {
	Enabled            bool
	Listen             string
	MaxBodyBytes       int64
	ReplayWindow       time.Duration
	RateLimitPerMinute int
	SecretResolver     func(string) ([]byte, error)
	Now                func() time.Time
}

type Delivery struct {
	Webhook        config.EffectiveWebhook
	IdempotencyKey string
	Timestamp      time.Time
	BodySHA256     string
	BodySize       int64
}

type Result struct {
	RunID     string `json:"run_id"`
	Status    string `json:"status"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

type Trigger func(context.Context, Delivery) (Result, error)

type RequestError struct {
	Status  int
	Message string
}

func (e *RequestError) Error() string { return e.Message }

type rateWindow struct {
	Started time.Time
	Count   int
}

type Server struct {
	mu       sync.RWMutex
	config   Config
	defs     map[string]config.EffectiveWebhook
	limits   map[string]rateWindow
	trigger  Trigger
	server   *http.Server
	listener net.Listener
}

func New(cfg Config, trigger Trigger) *Server {
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:8787"
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 1 << 20
	}
	if cfg.ReplayWindow <= 0 {
		cfg.ReplayWindow = 5 * time.Minute
	}
	if cfg.RateLimitPerMinute <= 0 {
		cfg.RateLimitPerMinute = 60
	}
	if cfg.SecretResolver == nil {
		cfg.SecretResolver = ResolveSecret
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Server{config: cfg, defs: map[string]config.EffectiveWebhook{}, limits: map[string]rateWindow{}, trigger: trigger}
}

func (s *Server) Configure(cfg Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cfg.Listen == "" || cfg.MaxBodyBytes <= 0 || cfg.ReplayWindow <= 0 || cfg.RateLimitPerMinute <= 0 {
		return errors.New("invalid webhook server configuration")
	}
	if cfg.SecretResolver == nil {
		cfg.SecretResolver = ResolveSecret
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	s.config = cfg
	return nil
}

func (s *Server) Apply(definitions []config.EffectiveWebhook) error {
	defs := make(map[string]config.EffectiveWebhook, len(definitions))
	for _, definition := range definitions {
		if definition.Path == "" {
			return errors.New("webhook path is required")
		}
		if _, exists := defs[definition.Path]; exists {
			return fmt.Errorf("duplicate webhook path %q", definition.Path)
		}
		defs[definition.Path] = definition
	}
	s.mu.Lock()
	s.defs = defs
	s.mu.Unlock()
	return nil
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.handle)
}

func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	cfg := s.config
	if !cfg.Enabled {
		s.mu.Unlock()
		return nil
	}
	if s.server != nil {
		s.mu.Unlock()
		return errors.New("webhook server is already running")
	}
	server := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	s.mu.Unlock()
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen for webhooks: %w", err)
	}
	s.mu.Lock()
	s.server = server
	s.listener = listener
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.Shutdown(shutdownCtx)
		cancel()
	}()
	go func() {
		_ = server.Serve(listener)
	}()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	server := s.server
	s.server = nil
	s.listener = nil
	s.mu.Unlock()
	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}

func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, &RequestError{Status: http.StatusMethodNotAllowed, Message: "webhook requires POST"})
		return
	}
	s.mu.RLock()
	definition, found := s.defs[r.URL.Path]
	cfg := s.config
	s.mu.RUnlock()
	if !found {
		writeError(w, &RequestError{Status: http.StatusNotFound, Message: "webhook path not found"})
		return
	}
	now := cfg.Now()
	if !s.allow(r, definition.Path, now, cfg.RateLimitPerMinute) {
		writeError(w, &RequestError{Status: http.StatusTooManyRequests, Message: "webhook rate limit exceeded"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, cfg.MaxBodyBytes+1))
	if err != nil {
		writeError(w, &RequestError{Status: http.StatusBadRequest, Message: "read webhook body"})
		return
	}
	if int64(len(body)) > cfg.MaxBodyBytes {
		writeError(w, &RequestError{Status: http.StatusRequestEntityTooLarge, Message: "webhook body is too large"})
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get(IdempotencyHeader))
	if idempotencyKey == "" || len(idempotencyKey) > 255 {
		writeError(w, &RequestError{Status: http.StatusBadRequest, Message: "valid Idempotency-Key is required"})
		return
	}
	timestampText := strings.TrimSpace(r.Header.Get(TimestampHeader))
	timestampUnix, err := strconv.ParseInt(timestampText, 10, 64)
	if err != nil {
		writeError(w, &RequestError{Status: http.StatusUnauthorized, Message: "invalid webhook timestamp"})
		return
	}
	timestamp := time.Unix(timestampUnix, 0)
	if absDuration(now.Sub(timestamp)) > cfg.ReplayWindow {
		writeError(w, &RequestError{Status: http.StatusUnauthorized, Message: "webhook timestamp is outside replay window"})
		return
	}
	secret, err := cfg.SecretResolver(definition.SecretRef)
	if err != nil {
		writeError(w, &RequestError{Status: http.StatusServiceUnavailable, Message: "webhook secret is unavailable"})
		return
	}
	if !validSignature(r.Header.Get(SignatureHeader), secret, timestampText, body) {
		writeError(w, &RequestError{Status: http.StatusUnauthorized, Message: "invalid webhook signature"})
		return
	}
	if s.trigger == nil {
		writeError(w, &RequestError{Status: http.StatusServiceUnavailable, Message: "webhook trigger is unavailable"})
		return
	}
	digest := sha256.Sum256(body)
	result, err := s.trigger(r.Context(), Delivery{
		Webhook: definition, IdempotencyKey: idempotencyKey, Timestamp: timestamp,
		BodySHA256: hex.EncodeToString(digest[:]), BodySize: int64(len(body)),
	})
	if err != nil {
		if requestErr, ok := err.(*RequestError); ok {
			writeError(w, requestErr)
			return
		}
		writeError(w, &RequestError{Status: http.StatusInternalServerError, Message: "webhook trigger failed"})
		return
	}
	status := http.StatusAccepted
	if result.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, result)
}

func (s *Server) allow(r *http.Request, path string, now time.Time, limit int) bool {
	key := r.RemoteAddr + "\x00" + path
	s.mu.Lock()
	defer s.mu.Unlock()
	window := s.limits[key]
	if window.Started.IsZero() || now.Sub(window.Started) >= time.Minute {
		window = rateWindow{Started: now}
	}
	if window.Count >= limit {
		s.limits[key] = window
		return false
	}
	window.Count++
	s.limits[key] = window
	return true
}

func ResolveSecret(reference string) ([]byte, error) {
	ref, err := secrets.Parse(reference)
	if err != nil {
		return nil, errors.New("unsupported webhook secret reference")
	}
	value, err := secrets.Resolve(context.Background(), ref)
	if err != nil {
		return nil, err
	}
	return []byte(value), nil
}

func validSignature(value string, secret []byte, timestamp string, body []byte) bool {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "sha256=")
	supplied, err := hex.DecodeString(value)
	if err != nil || len(supplied) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = io.WriteString(mac, timestamp+".")
	_, _ = mac.Write(body)
	return hmac.Equal(supplied, mac.Sum(nil))
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}

func writeError(w http.ResponseWriter, err *RequestError) {
	writeJSON(w, err.Status, map[string]string{"error": err.Message})
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
