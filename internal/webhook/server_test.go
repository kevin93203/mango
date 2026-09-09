package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
)

func testWebhookServer(t *testing.T, cfg Config, trigger Trigger) *Server {
	t.Helper()
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Unix(1_800_000_000, 0) }
	}
	if cfg.SecretResolver == nil {
		cfg.SecretResolver = func(string) ([]byte, error) { return []byte("secret"), nil }
	}
	server := New(cfg, trigger)
	if err := server.Apply([]config.EffectiveWebhook{{
		Project: "demo", Name: "deploy", Path: "/hooks/deploy", TargetType: "workflow", Target: "release", SecretRef: "env:HOOK_SECRET",
	}}); err != nil {
		t.Fatal(err)
	}
	return server
}

func signedRequest(body, key string, now time.Time) *http.Request {
	timestamp := strconv.FormatInt(now.Unix(), 10)
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = io.WriteString(mac, timestamp+"."+body)
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/hooks/deploy", strings.NewReader(body))
	request.RemoteAddr = "127.0.0.1:12345"
	request.Header.Set(IdempotencyHeader, key)
	request.Header.Set(TimestampHeader, timestamp)
	request.Header.Set(SignatureHeader, "sha256="+hex.EncodeToString(mac.Sum(nil)))
	return request
}

func TestHandlerAcceptsAuthenticatedDeliveryAndDoesNotExposeBody(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var delivery Delivery
	server := testWebhookServer(t, Config{Now: func() time.Time { return now }}, func(_ context.Context, got Delivery) (Result, error) {
		delivery = got
		return Result{RunID: "run-1", Status: "queued"}, nil
	})
	body := `{"action":"deploy","token":"do-not-store"}`
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, signedRequest(body, "event-1", now))
	if response.Code != http.StatusAccepted || !strings.Contains(response.Body.String(), `"run_id":"run-1"`) {
		t.Fatalf("response = %d %s, want accepted result", response.Code, response.Body.String())
	}
	if delivery.IdempotencyKey != "event-1" || delivery.BodySize != int64(len(body)) || delivery.BodySHA256 == "" {
		t.Fatalf("delivery = %+v, want safe delivery metadata", delivery)
	}
	if strings.Contains(response.Body.String(), "do-not-store") {
		t.Fatalf("response = %s, must not echo webhook body", response.Body.String())
	}
}

func TestHandlerRejectsSignatureReplayBodyAndRateFailures(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	server := testWebhookServer(t, Config{
		Now: func() time.Time { return now }, ReplayWindow: time.Minute, MaxBodyBytes: 4, RateLimitPerMinute: 10,
	}, func(context.Context, Delivery) (Result, error) { return Result{RunID: "run-1", Status: "queued"}, nil })

	badSignature := signedRequest("okay", "event-1", now)
	badSignature.Header.Set(SignatureHeader, "sha256="+strings.Repeat("0", 64))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, badSignature)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("bad signature status = %d, want 401", response.Code)
	}

	tooOld := signedRequest("okay", "event-2", now.Add(-2*time.Minute))
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, tooOld)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("replay status = %d, want 401", response.Code)
	}

	tooLarge := signedRequest("large", "event-3", now)
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, tooLarge)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("body size status = %d, want 413", response.Code)
	}

	rateServer := testWebhookServer(t, Config{
		Now: func() time.Time { return now }, RateLimitPerMinute: 1,
	}, func(context.Context, Delivery) (Result, error) { return Result{RunID: "run-1", Status: "queued"}, nil })
	valid := signedRequest("okay", "event-4", now)
	response = httptest.NewRecorder()
	rateServer.Handler().ServeHTTP(response, valid)
	if response.Code != http.StatusAccepted {
		t.Fatalf("first rate-limited request status = %d, want 202", response.Code)
	}
	second := signedRequest("okay", "event-5", now)
	response = httptest.NewRecorder()
	rateServer.Handler().ServeHTTP(response, second)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want 429", response.Code)
	}
}

func TestHandlerReturnsOKForDuplicateTrigger(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	server := testWebhookServer(t, Config{Now: func() time.Time { return now }}, func(_ context.Context, delivery Delivery) (Result, error) {
		return Result{RunID: "existing", Status: "success", Duplicate: delivery.IdempotencyKey == "same"}, nil
	})
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, signedRequest("okay", "same", now))
	if response.Code != http.StatusOK {
		t.Fatalf("duplicate status = %d, want 200", response.Code)
	}
}

func TestStartBindsConfiguredLoopbackListener(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	server := testWebhookServer(t, Config{
		Enabled: true, Listen: "127.0.0.1:0", Now: func() time.Time { return now },
	}, func(_ context.Context, delivery Delivery) (Result, error) {
		return Result{RunID: "run-live", Status: "queued"}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())

	timestamp := strconv.FormatInt(now.Unix(), 10)
	body := `{"action":"deploy"}`
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = io.WriteString(mac, timestamp+"."+body)
	request, err := http.NewRequest(http.MethodPost, "http://"+server.Addr()+"/hooks/deploy", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(IdempotencyHeader, "live-1")
	request.Header.Set(TimestampHeader, timestamp)
	request.Header.Set(SignatureHeader, "sha256="+hex.EncodeToString(mac.Sum(nil)))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("live webhook status = %d, want 202", response.StatusCode)
	}
}
