package daemon

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/observability"
	"github.com/kevin93203/mango/internal/secrets"
)

func (d *Daemon) startHTTPServer(ctx context.Context, cfg config.HTTPServerConfig) error {
	if !cfg.Enabled {
		return nil
	}
	token := ""
	if cfg.TokenRef != "" {
		reference, err := parseSecretReference(cfg.TokenRef)
		if err != nil {
			return fmt.Errorf("parse http authentication token: %w", err)
		}
		value, err := secrets.Resolve(ctx, reference)
		if err != nil {
			return fmt.Errorf("resolve http authentication token: %w", err)
		}
		token = value
		d.redactor.Add(token)
	}
	if token == "" && (cfg.RequireAuth || !httpListenLoopback(cfg.Listen)) {
		return errors.New("http server requires an authentication token")
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen for HTTP API: %w", err)
	}
	server := &http.Server{Handler: d.httpHandler(token), ReadHeaderTimeout: 5 * time.Second}
	d.httpServer, d.httpListener = server, listener
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) && d.structuredLog != nil {
			_ = d.structuredLog.Log("error", "http server stopped", map[string]interface{}{"error": err.Error()})
		}
	}()
	return nil
}

// parseSecretReference is kept local to avoid exposing token values through
// configuration or API structs.
func parseSecretReference(value string) (secrets.Reference, error) {
	parts := strings.SplitN(strings.TrimSpace(value), ":", 2)
	if len(parts) != 2 {
		return secrets.Reference{}, errors.New("token_ref must use env:NAME or file:PATH")
	}
	provider := parts[0]
	if provider == "env" {
		provider = "from_env"
	}
	if provider == "file" {
		provider = "from_file"
	}
	return secrets.Reference{Provider: provider, Name: parts[1]}, nil
}

func httpListenLoopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (d *Daemon) httpHandler(token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" {
			provided := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
			if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
				writeHTTPJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
				return
			}
		}
		if !strings.HasPrefix(r.URL.Path, "/api/v1/") {
			writeHTTPJSON(w, http.StatusNotFound, map[string]string{"error": "unknown API path"})
			return
		}
		parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/"), "/"), "/")
		if len(parts) == 1 && parts[0] == "metrics" && r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = w.Write([]byte(d.metrics.Prometheus()))
			return
		}
		var method string
		var params interface{}
		status := http.StatusOK
		switch {
		case len(parts) == 1 && parts[0] == "health" && r.Method == http.MethodGet:
			method = "health"
		case len(parts) == 1 && parts[0] == "services" && r.Method == http.MethodGet:
			method, params = "service.ls", map[string]string{"project": r.URL.Query().Get("project")}
		case len(parts) == 3 && parts[0] == "services" && r.Method == http.MethodGet:
			method, params = "service.get", map[string]string{"key": parts[1] + "/" + parts[2]}
		case len(parts) == 4 && parts[0] == "services" && r.Method == http.MethodPost:
			method, params, status = "service."+parts[3], map[string]string{"key": parts[1] + "/" + parts[2]}, http.StatusAccepted
		case len(parts) == 1 && parts[0] == "events" && r.Method == http.MethodGet:
			method, params = "events.list", map[string]interface{}{"after_id": parseInt64Query(r, "after_id"), "limit": parseIntQuery(r, "limit"), "type": r.URL.Query().Get("type"), "project": r.URL.Query().Get("project"), "run_id": r.URL.Query().Get("run_id")}
		case len(parts) == 1 && parts[0] == "audit" && r.Method == http.MethodGet:
			method, params = "audit.ls", map[string]int{"limit": parseIntQuery(r, "limit")}
		case len(parts) == 1 && parts[0] == "executions" && r.Method == http.MethodGet:
			method, params = "execution.ls", map[string]interface{}{"status": r.URL.Query().Get("status"), "project": r.URL.Query().Get("project"), "target_type": r.URL.Query().Get("target_type"), "target": r.URL.Query().Get("target"), "limit": parseIntQuery(r, "limit"), "all": true}
		case len(parts) == 3 && parts[0] == "executions" && r.Method == http.MethodPost:
			method, params, status = "execution."+parts[2], map[string]string{"run_id": parts[1]}, http.StatusAccepted
		default:
			writeHTTPJSON(w, http.StatusNotFound, map[string]string{"error": "unknown API route"})
			return
		}
		request, err := ipc.NewRequest(method, params)
		if err != nil {
			writeHTTPJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		response := d.Handle(observability.WithActor(r.Context(), "http"), request)
		if !response.OK {
			code, message := "REQUEST_FAILED", "request failed"
			errorResponse := map[string]interface{}{}
			if response.Error != nil {
				code, message = response.Error.Code, response.Error.Message
				if len(response.Error.Candidates) > 0 {
					errorResponse["candidates"] = response.Error.Candidates
				}
			}
			errorResponse["code"] = code
			errorResponse["error"] = message
			writeHTTPJSON(w, http.StatusBadRequest, errorResponse)
			return
		}
		writeHTTPJSON(w, status, response.Data)
	})
}

func parseIntQuery(r *http.Request, name string) int {
	value, _ := strconv.Atoi(r.URL.Query().Get(name))
	return value
}

func parseInt64Query(r *http.Request, name string) int64 {
	value, _ := strconv.ParseInt(r.URL.Query().Get(name), 10, 64)
	return value
}

func writeHTTPJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
