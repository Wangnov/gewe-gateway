package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gewe-gateway/internal/config"
	"gewe-gateway/internal/server"
	"gewe-gateway/internal/store"
)

type testEnv struct {
	handler http.Handler
	store   *store.Store
	api     *server.API
}

func newTestEnv(t *testing.T, opts ...func(*config.Config)) testEnv {
	t.Helper()

	cfg := config.Config{
		ListenAddr:       "127.0.0.1:0",
		DatabasePath:     filepath.Join(t.TempDir(), "gateway.db"),
		GatewayKey:       "test-key",
		InstanceLeaseTTL: 5 * time.Minute,
		ForwardTimeout:   2 * time.Second,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	sqliteStore, err := store.Open(cfg.DatabasePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = sqliteStore.Close()
	})

	api, err := server.New(cfg, sqliteStore)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() {
		_ = api.Close()
	})

	return testEnv{handler: api.Handler(), store: sqliteStore, api: api}
}

func postJSON(
	t *testing.T,
	handler http.Handler,
	path string,
	body any,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func registerInstance(t *testing.T, handler http.Handler, headerName string, callbackURL string) {
	t.Helper()

	rec := postJSON(t, handler, "/gateway/v1/instances/register", map[string]any{
		"instanceId":     "instance-a",
		"callbackUrl":    callbackURL,
		"callbackSecret": "callback-secret",
		"groups":         []string{"123@chatroom"},
		"pluginVersion":  "2026.3.13",
	}, map[string]string{
		headerName: "test-key",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("register instance failed: code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHealthz(t *testing.T) {
	env := newTestEnv(t)

	req := httptest.NewRequest(http.MethodGet, "/gateway/v1/healthz", nil)
	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got := rec.Body.String(); got == "" {
		t.Fatal("expected non-empty healthz body")
	}
}

func TestRegisterConflictOnSameGroup(t *testing.T) {
	env := newTestEnv(t)

	firstRec := postJSON(t, env.handler, "/gateway/v1/instances/register", map[string]any{
		"instanceId":     "instance-a",
		"callbackUrl":    "https://example.com/a",
		"callbackSecret": "secret-a",
		"groups":         []string{"123@chatroom"},
		"pluginVersion":  "2026.3.13",
	}, map[string]string{
		"X-Gateway-Key": "test-key",
	})
	if firstRec.Code != http.StatusOK {
		t.Fatalf("expected first register 200, got %d body=%s", firstRec.Code, firstRec.Body.String())
	}

	secondRec := postJSON(t, env.handler, "/gateway/v1/instances/register", map[string]any{
		"instanceId":     "instance-b",
		"callbackUrl":    "https://example.com/b",
		"callbackSecret": "secret-b",
		"groups":         []string{"123@chatroom"},
		"pluginVersion":  "2026.3.13",
	}, map[string]string{
		"X-Gateway-Key": "test-key",
	})
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("expected second register 409, got %d body=%s", secondRec.Code, secondRec.Body.String())
	}
}

func TestRegisterAcceptsGeWeGatewayKeyHeader(t *testing.T) {
	env := newTestEnv(t)

	rec := postJSON(t, env.handler, "/gateway/v1/instances/register", map[string]any{
		"instanceId":     "instance-a",
		"callbackUrl":    "https://example.com/a",
		"callbackSecret": "secret-a",
		"groups":         []string{"123@chatroom"},
		"pluginVersion":  "2026.3.13",
	}, map[string]string{
		"X-GeWe-Gateway-Key": "test-key",
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("expected register with X-GeWe-Gateway-Key to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHeartbeat(t *testing.T) {
	env := newTestEnv(t)
	registerInstance(t, env.handler, "X-Gateway-Key", "https://example.com/callback")

	rec := postJSON(t, env.handler, "/gateway/v1/instances/heartbeat", map[string]any{
		"instanceId": "instance-a",
	}, map[string]string{
		"X-Gateway-Key": "test-key",
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("expected heartbeat 200, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestUnregisterRemovesInstance(t *testing.T) {
	env := newTestEnv(t)
	registerInstance(t, env.handler, "X-Gateway-Key", "https://example.com/callback")

	unregisterRec := postJSON(t, env.handler, "/gateway/v1/instances/unregister", map[string]any{
		"instanceId": "instance-a",
	}, map[string]string{
		"X-Gateway-Key": "test-key",
	})
	if unregisterRec.Code != http.StatusOK {
		t.Fatalf("expected unregister 200, got %d body=%s", unregisterRec.Code, unregisterRec.Body.String())
	}

	heartbeatRec := postJSON(t, env.handler, "/gateway/v1/instances/heartbeat", map[string]any{
		"instanceId": "instance-a",
	}, map[string]string{
		"X-Gateway-Key": "test-key",
	})
	if heartbeatRec.Code != http.StatusNotFound {
		t.Fatalf("expected heartbeat after unregister 404, got %d body=%s", heartbeatRec.Code, heartbeatRec.Body.String())
	}
}

func TestGeweWebhookForward(t *testing.T) {
	callbackHeader := make(chan string, 1)
	callbackBody := make(chan []byte, 1)
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read callback body: %v", err)
		}
		callbackHeader <- r.Header.Get("X-GEWE-CALLBACK-TOKEN")
		callbackBody <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	defer callback.Close()

	env := newTestEnv(t, func(cfg *config.Config) {
		cfg.GeweWebhookSecret = "webhook-secret"
	})
	registerInstance(t, env.handler, "X-Gateway-Key", callback.URL)

	webhookRec := postJSON(t, env.handler, "/gateway/v1/webhooks/gewe", map[string]any{
		"Appid": "wx-app",
		"Data": map[string]any{
			"MsgId":    "10001",
			"NewMsgId": "10001",
			"FromUserName": map[string]any{
				"string": "123@chatroom",
			},
			"ToUserName": map[string]any{
				"string": "wxid_target",
			},
		},
	}, map[string]string{
		"X-GEWE-CALLBACK-TOKEN": "webhook-secret",
	})

	if webhookRec.Code != http.StatusOK {
		t.Fatalf("expected webhook 200, got %d body=%s", webhookRec.Code, webhookRec.Body.String())
	}

	select {
	case got := <-callbackHeader:
		if got != "callback-secret" {
			t.Fatalf("expected callback secret header, got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected forwarded callback request")
	}

	select {
	case body := <-callbackBody:
		if !strings.Contains(string(body), "\"Appid\":\"wx-app\"") {
			t.Fatalf("expected forwarded callback body to include webhook payload, got %s", string(body))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected forwarded callback body")
	}
}

func TestCompatProxyForwardsPathAndHeaders(t *testing.T) {
	type upstreamRequest struct {
		path        string
		token       string
		contentType string
		body        []byte
	}

	upstreamSeen := make(chan upstreamRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream body: %v", err)
		}
		upstreamSeen <- upstreamRequest{
			path:        r.URL.Path,
			token:       r.Header.Get("X-GEWE-TOKEN"),
			contentType: r.Header.Get("Content-Type"),
			body:        body,
		}
		w.Header().Set("X-Upstream", "ok")
		_, _ = w.Write([]byte(`{"ret":200}`))
	}))
	defer upstream.Close()

	env := newTestEnv(t, func(cfg *config.Config) {
		cfg.GeweAPIBaseURL = upstream.URL
		cfg.GeweToken = "gewe-token"
		cfg.GeweAppID = "wx-app"
	})

	rec := postJSON(t, env.handler, "/gewe/v2/api/message/postText", map[string]any{
		"toWxid": "wxid_target",
		"msg":    "hello",
	}, map[string]string{
		"X-GeWe-Gateway-Key": "test-key",
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("expected compat proxy 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Upstream") != "ok" {
		t.Fatalf("expected upstream header to be copied, got %q", rec.Header().Get("X-Upstream"))
	}

	select {
	case seen := <-upstreamSeen:
		if seen.path != "/gewe/v2/api/message/postText" {
			t.Fatalf("expected upstream path to be preserved, got %q", seen.path)
		}
		if seen.token != "gewe-token" {
			t.Fatalf("expected X-GEWE-TOKEN header, got %q", seen.token)
		}
		if seen.contentType != "application/json" {
			t.Fatalf("expected content type application/json, got %q", seen.contentType)
		}
		if !strings.Contains(string(seen.body), "\"appId\":\"wx-app\"") {
			t.Fatalf("expected request body to inject appId, got %s", string(seen.body))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected compat proxy request to reach upstream")
	}
}

func TestCompatProxySerializesConcurrentRequests(t *testing.T) {
	var current int32
	var maxConcurrent int32
	firstStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	var requestCount int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&current, 1)
		defer atomic.AddInt32(&current, -1)
		for {
			max := atomic.LoadInt32(&maxConcurrent)
			if n <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, n) {
				break
			}
		}

		count := atomic.AddInt32(&requestCount, 1)
		if count == 1 {
			firstStarted <- struct{}{}
			<-releaseFirst
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ret":200}`))
	}))
	defer upstream.Close()

	env := newTestEnv(t, func(cfg *config.Config) {
		cfg.GeweAPIBaseURL = upstream.URL
		cfg.GeweToken = "gewe-token"
		cfg.GeweAppID = "wx-app"
	})
	gateway := httptest.NewServer(env.handler)
	defer gateway.Close()

	sendCompat := func() error {
		body, err := json.Marshal(map[string]any{
			"toWxid": "wxid_target",
			"msg":    "hello",
		})
		if err != nil {
			return err
		}
		req, err := http.NewRequest(http.MethodPost, gateway.URL+"/gewe/v2/api/message/postText", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-GeWe-Gateway-Key", "test-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			payload, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected compat proxy 200, got %d body=%s", resp.StatusCode, string(payload))
		}
		return nil
	}

	var wg sync.WaitGroup
	var firstErr error
	var secondErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		firstErr = sendCompat()
	}()

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("expected first upstream request to start")
	}

	go func() {
		defer wg.Done()
		secondErr = sendCompat()
	}()

	time.Sleep(150 * time.Millisecond)
	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Fatalf("expected second request to wait in gateway queue, got %d upstream requests", got)
	}
	if got := atomic.LoadInt32(&maxConcurrent); got != 1 {
		t.Fatalf("expected max upstream concurrency 1, got %d", got)
	}

	close(releaseFirst)
	wg.Wait()

	if firstErr != nil {
		t.Fatalf("first compat request failed: %v", firstErr)
	}
	if secondErr != nil {
		t.Fatalf("second compat request failed: %v", secondErr)
	}
	if got := atomic.LoadInt32(&requestCount); got != 2 {
		t.Fatalf("expected two upstream requests after release, got %d", got)
	}
}

func TestReadyzRequiresGeweCredentials(t *testing.T) {
	notReadyEnv := newTestEnv(t)

	req := httptest.NewRequest(http.MethodGet, "/gateway/v1/readyz", nil)
	rec := httptest.NewRecorder()
	notReadyEnv.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected readyz 503 when GeWe credentials are missing, got %d body=%s", rec.Code, rec.Body.String())
	}

	readyEnv := newTestEnv(t, func(cfg *config.Config) {
		cfg.GeweToken = "gewe-token"
		cfg.GeweAppID = "wx-app"
	})
	readyReq := httptest.NewRequest(http.MethodGet, "/gateway/v1/readyz", nil)
	readyRec := httptest.NewRecorder()
	readyEnv.handler.ServeHTTP(readyRec, readyReq)
	if readyRec.Code != http.StatusOK {
		t.Fatalf("expected readyz 200 when GeWe credentials are configured, got %d body=%s", readyRec.Code, readyRec.Body.String())
	}
}

func TestCompatProxyRetriesTransientFailuresUntilSuccess(t *testing.T) {
	var requestCount int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count == 1 {
			http.Error(w, "temporary upstream failure", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ret":200,"msg":"ok"}`))
	}))
	defer upstream.Close()

	env := newTestEnv(t, func(cfg *config.Config) {
		cfg.GeweAPIBaseURL = upstream.URL
		cfg.GeweToken = "gewe-token"
		cfg.GeweAppID = "wx-app"
		cfg.OutboundMaxAttempts = 2
		cfg.OutboundBaseBackoff = 5 * time.Millisecond
		cfg.OutboundMaxBackoff = 10 * time.Millisecond
		cfg.WorkerPollInterval = 5 * time.Millisecond
	})

	rec := postJSON(t, env.handler, "/gewe/v2/api/message/postText", map[string]any{
		"toWxid": "wxid_target",
		"msg":    "hello",
	}, map[string]string{
		"X-GeWe-Gateway-Key": "test-key",
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("expected compat proxy to succeed after retry, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := atomic.LoadInt32(&requestCount); got != 2 {
		t.Fatalf("expected 2 upstream attempts, got %d", got)
	}
}

func TestQueueStatsEndpointReportsDeadJobs(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	job, err := env.store.CreateOutboundJob(ctx, "/gewe/v2/api/message/postText", []byte(`{"msg":"hello"}`), 1)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	_, _, err = env.store.ClaimNextOutboundJob(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if err := env.store.MarkOutboundDead(ctx, job.JobID, "forced dead letter"); err != nil {
		t.Fatalf("mark dead: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/gateway/v1/queue/stats", nil)
	req.Header.Set("X-GeWe-Gateway-Key", "test-key")
	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected stats endpoint 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"dead":1`) {
		t.Fatalf("expected queue stats to report dead=1, got %s", rec.Body.String())
	}
}

func TestRequeueDeadEndpoint(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	job, err := env.store.CreateOutboundJob(ctx, "/gewe/v2/api/message/postText", []byte(`{"msg":"hello"}`), 1)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	_, _, err = env.store.ClaimNextOutboundJob(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if err := env.store.MarkOutboundDead(ctx, job.JobID, "forced dead letter"); err != nil {
		t.Fatalf("mark dead: %v", err)
	}

	rec := postJSON(t, env.handler, "/gateway/v1/queue/dead/requeue", map[string]any{
		"limit": 10,
	}, map[string]string{
		"X-GeWe-Gateway-Key": "test-key",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected requeue endpoint 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"requeued":1`) {
		t.Fatalf("expected requeue response to report 1 job, got %s", rec.Body.String())
	}

	stats, err := env.store.GetQueueStats(ctx)
	if err != nil {
		t.Fatalf("queue stats: %v", err)
	}
	if stats.Pending != 1 || stats.Dead != 0 {
		t.Fatalf("expected pending=1 dead=0 after requeue, got pending=%d dead=%d", stats.Pending, stats.Dead)
	}
}

func TestMaintenanceLoopCleansExpiredRecords(t *testing.T) {
	env := newTestEnv(t, func(cfg *config.Config) {
		cfg.GeweToken = "gewe-token"
		cfg.GeweAppID = "wx-app"
		cfg.InboundDedupeRetention = 10 * time.Millisecond
		cfg.OutboundHistoryRetention = 10 * time.Millisecond
		cfg.MaintenanceInterval = 5 * time.Millisecond
	})
	ctx := context.Background()

	if _, err := env.store.MarkInboundSeen(ctx, "dedupe-key"); err != nil {
		t.Fatalf("seed inbound dedupe: %v", err)
	}

	job, err := env.store.CreateOutboundJob(ctx, "/gewe/v2/api/message/postText", []byte(`{"msg":"hello"}`), 1)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if _, _, err := env.store.ClaimNextOutboundJob(ctx, time.Now().UTC()); err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if err := env.store.MarkOutboundSucceeded(ctx, job.JobID); err != nil {
		t.Fatalf("mark job succeeded: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := env.store.GetQueueStats(ctx)
		if err != nil {
			t.Fatalf("queue stats: %v", err)
		}
		alreadySeen, err := env.store.MarkInboundSeen(ctx, "dedupe-key")
		if err != nil {
			t.Fatalf("reinsert dedupe key: %v", err)
		}
		if stats.Succeeded == 0 && !alreadySeen {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	stats, err := env.store.GetQueueStats(ctx)
	if err != nil {
		t.Fatalf("queue stats after waiting: %v", err)
	}
	t.Fatalf("expected maintenance loop to clean dedupe row and succeeded job, got succeeded=%d", stats.Succeeded)
}
