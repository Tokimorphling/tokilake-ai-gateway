package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	tokilake "github.com/Tokimorphling/Tokilake/tokilake-core"
	"github.com/gorilla/websocket"
)

var (
	addr   string
	token  string
	logger *StdLogger
)

func main() {
	flag.StringVar(&addr, "addr", ":8080", "listen address")
	flag.StringVar(&token, "token", "sk-test", "expected auth token")
	flag.Parse()

	logger = &StdLogger{}

	auth := &SimpleAuthenticator{ExpectedToken: token}
	manager := tokilake.NewSessionManager()
	registry := &MemoryWorkerRegistry{workers: make(map[int]*workerEntry), manager: manager}
	gw := tokilake.NewGateway(auth, registry, nil, logger, manager)

	upgrader := websocket.Upgrader{
		Subprotocols: []string{"tokilake.v1"},
		CheckOrigin:  func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()

	connectHandler := func(w http.ResponseWriter, r *http.Request) {
		tokenKey, tkn, authErr := gw.AuthenticateConnectRequest(r.Context(), r)
		if authErr != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, authErr.Error()), http.StatusUnauthorized)
			return
		}

		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.SysError(fmt.Sprintf("upgrade failed: %v", err))
			return
		}
		defer wsConn.Close()

		remoteAddr := resolveRemoteAddr(r)
		if err := gw.HandleGatewayConnection(r.Context(), wsConn, tkn, tokenKey, remoteAddr); err != nil {
			logger.SysError(fmt.Sprintf("session closed: %v", err))
		}
	}
	mux.HandleFunc("/connect", connectHandler)
	mux.HandleFunc("/api/tokilake/connect", connectHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":   "ok",
			"sessions": manager.SessionCount(),
		})
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		namespace := r.URL.Query().Get("namespace")
		if namespace == "" {
			namespace = "test-worker"
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"read body failed"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var req struct {
			Model string `json:"model"`
		}
		json.Unmarshal(body, &req)
		if req.Model == "" {
			req.Model = "gpt-3.5-turbo"
		}

		tunnelReq := &tokilake.TunnelRequest{
			RouteKind: tokilake.TunnelRouteKindChatCompletions,
			Method:    r.Method,
			Path:      r.URL.Path,
			Model:     req.Model,
			Headers:   map[string]string{"Content-Type": r.Header.Get("Content-Type")},
			Body:      body,
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		resp, requestID, err := gw.DoTunnelRequestByNamespace(ctx, namespace, tunnelReq)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s","request_id":"%s"}`, err.Error(), requestID), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		for k, v := range resp.Header {
			if len(v) > 0 {
				w.Header().Set(k, v[0])
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	})
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		namespace := r.URL.Query().Get("namespace")
		if namespace == "" {
			namespace = "test-worker"
		}

		requestBody := []byte(`{"model":"gpt-3.5-turbo","messages":[{"role":"user","content":"Hello, this is a test message"}],"stream":false}`)
		tunnelReq := &tokilake.TunnelRequest{
			RouteKind: tokilake.TunnelRouteKindChatCompletions,
			Method:    http.MethodPost,
			Path:      "/v1/chat/completions",
			Model:     "gpt-3.5-turbo",
			Headers:   map[string]string{"Content-Type": "application/json"},
			Body:      requestBody,
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		resp, requestID, err := gw.DoTunnelRequestByNamespace(ctx, namespace, tunnelReq)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s","request_id":"%s"}`, err.Error(), requestID), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		w.WriteHeader(resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		w.Write(body)
	})

	srv := &http.Server{Addr: addr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.SysLog(fmt.Sprintf("tokilake server listening on %s", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.SysError(fmt.Sprintf("listen error: %v", err))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.SysLog("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func resolveRemoteAddr(r *http.Request) string {
	clientIP := r.Header.Get("X-Forwarded-For")
	if clientIP == "" {
		clientIP = r.Header.Get("X-Real-Ip")
	}
	clientIP = strings.TrimSpace(strings.Split(clientIP, ",")[0])
	if clientIP == "" {
		return r.RemoteAddr
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host == clientIP {
		return r.RemoteAddr
	}
	return clientIP
}

// SimpleAuthenticator accepts a static token.
type SimpleAuthenticator struct {
	ExpectedToken string
}

func (a *SimpleAuthenticator) AuthenticateTokenKey(_ context.Context, tokenKey string) (string, *tokilake.Token, error) {
	tokenKey = strings.TrimSpace(strings.TrimPrefix(tokenKey, "sk-"))
	expected := strings.TrimPrefix(a.ExpectedToken, "sk-")
	if tokenKey == "" || tokenKey != expected {
		return "", nil, fmt.Errorf("invalid token")
	}
	return tokenKey, &tokilake.Token{UserId: 1}, nil
}

// MemoryWorkerRegistry stores workers in memory.
type MemoryWorkerRegistry struct {
	mu      sync.RWMutex
	workers map[int]*workerEntry
	nextID  int
	manager *tokilake.SessionManager
}

type workerEntry struct {
	WorkerID  int
	Namespace string
	Models    []string
	LastSeen  time.Time
}

func (r *MemoryWorkerRegistry) RegisterWorker(_ context.Context, session *tokilake.GatewaySession, register *tokilake.RegisterMessage) (*tokilake.RegisterResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	namespace := strings.TrimSpace(register.Namespace)
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}

	if err := r.manager.ClaimNamespace(session, namespace); err != nil {
		return nil, err
	}

	r.nextID++
	wid := r.nextID
	r.workers[wid] = &workerEntry{
		WorkerID:  wid,
		Namespace: namespace,
		Models:    register.Models,
		LastSeen:  time.Now(),
	}
	r.manager.BindChannel(session, wid, wid, register.Group, register.Models, register.BackendType, 1, register.ConcurrencyLimit)
	logger.SysLog(fmt.Sprintf("worker registered: id=%d namespace=%s models=%v", wid, namespace, register.Models))

	return &tokilake.RegisterResult{
		WorkerID:  wid,
		ChannelID: wid,
		Namespace: namespace,
		Models:    register.Models,
		Status:    1,
	}, nil
}

func (r *MemoryWorkerRegistry) UpdateHeartbeat(_ context.Context, session *tokilake.GatewaySession, heartbeat *tokilake.HeartbeatMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.workers[session.WorkerID]; ok {
		e.LastSeen = time.Now()
		if len(heartbeat.CurrentModels) > 0 {
			e.Models = heartbeat.CurrentModels
		}
	}
	return nil
}

func (r *MemoryWorkerRegistry) SyncModels(_ context.Context, session *tokilake.GatewaySession, sync *tokilake.ModelsSyncMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.workers[session.WorkerID]; ok {
		e.Models = sync.Models
	}
	return nil
}

func (r *MemoryWorkerRegistry) CleanupWorker(_ context.Context, session *tokilake.GatewaySession) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, session.WorkerID)
	logger.SysLog(fmt.Sprintf("worker removed: id=%d", session.WorkerID))
	return nil
}

// StdLogger logs to stdout/stderr.
type StdLogger struct{}

func (l *StdLogger) SysLog(msg string)   { log.Printf("[INFO] %s", msg) }
func (l *StdLogger) SysError(msg string) { log.Printf("[ERROR] %s", msg) }
func (l *StdLogger) FatalLog(msg string) { log.Fatalf("[FATAL] %s", msg) }
