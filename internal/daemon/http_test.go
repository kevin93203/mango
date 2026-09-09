package daemon

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
