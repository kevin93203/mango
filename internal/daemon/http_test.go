package daemon

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/scheduler"
)

func TestHTTPAPIUsesBearerAuthenticationAndVersionedRoutes(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.metrics.Add("mango_test_requests_total", 1, nil)
	handler := d.httpHandler("test-token")
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}
	authorized := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil)
	request.Header.Set("Authorization", "Bearer test-token")
	handler.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusOK || authorized.Header().Get("Content-Type") == "" {
		t.Fatalf("authorized response = %d %q", authorized.Code, authorized.Body.String())
	}
	if !containsJSONMetric(authorized.Body.Bytes(), "mango_test_requests_total") {
		t.Fatalf("metrics response = %q", authorized.Body.String())
	}
}

func TestHTTPExecutionRoutesAcceptReferencesAndExposeAmbiguousCandidates(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	for _, runID := range []string{
		"7f31a2c4-d9e0-4b11-9c8a-1234567890ab",
		"7f31a2c4-d9e1-4b11-9c8a-1234567890ab",
	} {
		d.scheduler.RecordExecution(scheduler.Record{
			RunID: runID, Project: "demo", TargetType: "task", Target: "job",
			Status: scheduler.StatusSuccess, Started: time.Now().UTC(), Finished: time.Now().UTC(),
		})
	}
	handler := d.httpHandler("test-token")

	request := httptest.NewRequest(http.MethodPost, "/api/v1/executions/7f31a2c4d9e0/get", nil)
	request.Header.Set("Authorization", "Bearer test-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || !containsString(response.Body.String(), "7f31a2c4-d9e0-4b11-9c8a-1234567890ab") {
		t.Fatalf("prefix execution response = %d %q", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/api/v1/executions/7f31a2c4/get", nil)
	request.Header.Set("Authorization", "Bearer test-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous execution status = %d, body %q", response.Code, response.Body.String())
	}
	var body struct {
		Code       string `json:"code"`
		Candidates []struct {
			Ref   string `json:"ref"`
			RunID string `json:"run_id"`
		} `json:"candidates"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Code != scheduler.RunReferenceAmbiguousCode || len(body.Candidates) != 2 {
		t.Fatalf("ambiguous HTTP body = %+v, want code %s and two candidates", body, scheduler.RunReferenceAmbiguousCode)
	}
}

func containsJSONMetric(data []byte, name string) bool {
	var value interface{}
	_ = json.Unmarshal(data, &value)
	return len(data) > 0 && string(data) != "" && name != "" && containsString(string(data), name)
}

func containsString(value, target string) bool {
	for index := 0; index+len(target) <= len(value); index++ {
		if value[index:index+len(target)] == target {
			return true
		}
	}
	return false
}
