package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gewe-gateway/internal/config"
	"gewe-gateway/internal/store"
)

type API struct {
	cfg          config.Config
	store        *store.Store
	client       *http.Client
	mux          *http.ServeMux
	workerCtx    context.Context
	workerCancel context.CancelFunc
	workerSignal chan struct{}
	closeOnce    sync.Once
	wg           sync.WaitGroup
	waitersMu    sync.Mutex
	waiters      map[int64]chan outboundResult
}

type registerRequest struct {
	InstanceID     string   `json:"instanceId"`
	CallbackURL    string   `json:"callbackUrl"`
	CallbackSecret string   `json:"callbackSecret"`
	Groups         []string `json:"groups"`
	PluginVersion  string   `json:"pluginVersion"`
}

type instanceRequest struct {
	InstanceID string `json:"instanceId"`
}

type requeueDeadRequest struct {
	Limit int `json:"limit"`
}

type geweWebhookPayload struct {
	AppID string `json:"Appid"`
	Data  struct {
		MsgID    any `json:"MsgId"`
		NewMsgID any `json:"NewMsgId"`
		FromUser struct {
			String string `json:"string"`
		} `json:"FromUserName"`
		ToUser struct {
			String string `json:"string"`
		} `json:"ToUserName"`
	} `json:"Data"`
}

type outboundResult struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

type outboundExecution struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
	Retryable  bool
	Err        error
}

var compatPaths = []string{
	"/gewe/v2/api/message/postText",
	"/gewe/v2/api/message/postImage",
	"/gewe/v2/api/message/postVoice",
	"/gewe/v2/api/message/postVideo",
	"/gewe/v2/api/message/postFile",
	"/gewe/v2/api/message/postLink",
	"/gewe/v2/api/message/downloadImage",
	"/gewe/v2/api/message/downloadVoice",
	"/gewe/v2/api/message/downloadVideo",
	"/gewe/v2/api/message/downloadFile",
}

func New(cfg config.Config, sqliteStore *store.Store) (*API, error) {
	if cfg.ForwardTimeout <= 0 {
		cfg.ForwardTimeout = 10 * time.Second
	}
	if cfg.OutboundMaxAttempts <= 0 {
		cfg.OutboundMaxAttempts = 3
	}
	if cfg.OutboundBaseBackoff <= 0 {
		cfg.OutboundBaseBackoff = 2 * time.Second
	}
	if cfg.OutboundMaxBackoff <= 0 {
		cfg.OutboundMaxBackoff = 30 * time.Second
	}
	if cfg.OutboundMaxBackoff < cfg.OutboundBaseBackoff {
		cfg.OutboundMaxBackoff = cfg.OutboundBaseBackoff
	}
	if cfg.WorkerPollInterval <= 0 {
		cfg.WorkerPollInterval = 500 * time.Millisecond
	}
	if cfg.InboundDedupeRetention <= 0 {
		cfg.InboundDedupeRetention = 7 * 24 * time.Hour
	}
	if cfg.OutboundHistoryRetention <= 0 {
		cfg.OutboundHistoryRetention = 14 * 24 * time.Hour
	}
	if cfg.MaintenanceInterval <= 0 {
		cfg.MaintenanceInterval = 1 * time.Hour
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	api := &API{
		cfg:   cfg,
		store: sqliteStore,
		client: &http.Client{
			Timeout: cfg.ForwardTimeout,
		},
		mux:          http.NewServeMux(),
		workerCtx:    workerCtx,
		workerCancel: workerCancel,
		workerSignal: make(chan struct{}, 1),
		waiters:      make(map[int64]chan outboundResult),
	}
	api.routes()

	if _, err := api.store.RequeueRunningJobs(workerCtx); err != nil {
		workerCancel()
		return nil, err
	}
	if err := api.runMaintenanceOnce(workerCtx); err != nil {
		workerCancel()
		return nil, err
	}

	api.wg.Add(2)
	go api.runOutboundWorker()
	go api.runMaintenanceLoop()
	api.signalWorker()
	return api, nil
}

func (a *API) Close() error {
	a.closeOnce.Do(func() {
		a.workerCancel()
		a.wg.Wait()
	})
	return nil
}

func (a *API) Handler() http.Handler {
	return a.mux
}

func (a *API) routes() {
	a.mux.HandleFunc("GET /gateway/v1/healthz", a.handleHealthz)
	a.mux.HandleFunc("GET /gateway/v1/readyz", a.handleReadyz)
	a.mux.HandleFunc("GET /gateway/v1/queue/stats", a.handleQueueStats)
	a.mux.HandleFunc("POST /gateway/v1/queue/dead/requeue", a.handleRequeueDead)
	a.mux.HandleFunc("POST /gateway/v1/instances/register", a.handleRegister)
	a.mux.HandleFunc("POST /gateway/v1/instances/heartbeat", a.handleHeartbeat)
	a.mux.HandleFunc("POST /gateway/v1/instances/unregister", a.handleUnregister)
	a.mux.HandleFunc("POST /gateway/v1/webhooks/gewe", a.handleGeweWebhook)

	for _, path := range compatPaths {
		a.mux.HandleFunc("POST "+path, a.handleCompatProxy)
	}
}

func (a *API) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"service": "gewe-gateway",
	})
}

func (a *API) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := a.store.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":    false,
			"ready": false,
			"error": fmt.Sprintf("database unavailable: %v", err),
		})
		return
	}
	if strings.TrimSpace(a.cfg.GeweToken) == "" || strings.TrimSpace(a.cfg.GeweAppID) == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":    false,
			"ready": false,
			"error": "gewe token/appId not configured",
		})
		return
	}

	stats, err := a.store.GetQueueStats(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":    false,
			"ready": false,
			"error": fmt.Sprintf("queue stats unavailable: %v", err),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":    true,
		"ready": true,
		"queue": stats,
	})
}

func (a *API) handleQueueStats(w http.ResponseWriter, r *http.Request) {
	if !a.authorizeGatewayKey(r) {
		writeError(w, http.StatusUnauthorized, "invalid gateway key")
		return
	}

	stats, err := a.store.GetQueueStats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":    true,
		"queue": stats,
	})
}

func (a *API) handleRequeueDead(w http.ResponseWriter, r *http.Request) {
	if !a.authorizeGatewayKey(r) {
		writeError(w, http.StatusUnauthorized, "invalid gateway key")
		return
	}

	var body requeueDeadRequest
	if err := decodeOptionalJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	requeued, err := a.store.RequeueDeadJobs(r.Context(), body.Limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if requeued > 0 {
		a.signalWorker()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"requeued": requeued,
	})
}

func (a *API) handleRegister(w http.ResponseWriter, r *http.Request) {
	if !a.authorizeGatewayKey(r) {
		writeError(w, http.StatusUnauthorized, "invalid gateway key")
		return
	}

	var body registerRequest
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateRegister(body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	err := a.store.RegisterInstance(r.Context(), store.Registration{
		InstanceID:     strings.TrimSpace(body.InstanceID),
		CallbackURL:    strings.TrimSpace(body.CallbackURL),
		CallbackSecret: strings.TrimSpace(body.CallbackSecret),
		PluginVersion:  strings.TrimSpace(body.PluginVersion),
		Groups:         normalizeGroups(body.Groups),
	}, a.cfg.InstanceLeaseTTL)
	if errors.Is(err, store.ErrGroupConflict) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"instanceId": body.InstanceID,
		"groups":     normalizeGroups(body.Groups),
	})
}

func (a *API) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if !a.authorizeGatewayKey(r) {
		writeError(w, http.StatusUnauthorized, "invalid gateway key")
		return
	}

	var body instanceRequest
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	instanceID := strings.TrimSpace(body.InstanceID)
	if instanceID == "" {
		writeError(w, http.StatusBadRequest, "instanceId is required")
		return
	}

	ok, err := a.store.Heartbeat(r.Context(), instanceID, a.cfg.InstanceLeaseTTL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "instance not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "instanceId": instanceID})
}

func (a *API) handleUnregister(w http.ResponseWriter, r *http.Request) {
	if !a.authorizeGatewayKey(r) {
		writeError(w, http.StatusUnauthorized, "invalid gateway key")
		return
	}

	var body instanceRequest
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	instanceID := strings.TrimSpace(body.InstanceID)
	if instanceID == "" {
		writeError(w, http.StatusBadRequest, "instanceId is required")
		return
	}

	if err := a.store.Unregister(r.Context(), instanceID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "instanceId": instanceID})
}

func (a *API) handleCompatProxy(w http.ResponseWriter, r *http.Request) {
	if !a.authorizeGatewayKey(r) {
		writeError(w, http.StatusUnauthorized, "invalid gateway key")
		return
	}
	if strings.TrimSpace(a.cfg.GeweToken) == "" || strings.TrimSpace(a.cfg.GeweAppID) == "" {
		writeError(w, http.StatusServiceUnavailable, "gewe token/appId not configured")
		return
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read request body failed")
		return
	}
	payload, err = a.injectAppID(payload)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	job, err := a.store.CreateOutboundJob(
		r.Context(),
		r.URL.Path,
		payload,
		a.cfg.OutboundMaxAttempts,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	waiter := a.addWaiter(job.JobID)
	defer a.removeWaiter(job.JobID, waiter)

	a.signalWorker()

	select {
	case result := <-waiter:
		copyHeaders(w.Header(), result.Headers)
		statusCode := result.StatusCode
		if statusCode == 0 {
			statusCode = http.StatusBadGateway
		}
		w.WriteHeader(statusCode)
		_, _ = w.Write(result.Body)
	case <-r.Context().Done():
		writeError(w, http.StatusGatewayTimeout, "gateway request timed out while waiting for upstream delivery")
	}
}

func (a *API) handleGeweWebhook(w http.ResponseWriter, r *http.Request) {
	if !a.authorizeWebhook(r) {
		writeError(w, http.StatusUnauthorized, "invalid webhook token")
		return
	}

	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read request body failed")
		return
	}

	var payload geweWebhookPayload
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid webhook payload")
		return
	}

	groupID := extractGroupID(payload)
	if groupID == "" {
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "status": "ignored_non_group"})
		return
	}

	alreadySeen, err := a.store.MarkInboundSeen(r.Context(), buildDedupeKey(payload))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if alreadySeen {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "duplicate"})
		return
	}

	binding, found, err := a.store.ResolveBinding(r.Context(), groupID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "status": "no_binding"})
		return
	}

	req, err := http.NewRequestWithContext(
		context.WithoutCancel(r.Context()),
		http.MethodPost,
		binding.CallbackURL,
		bytes.NewReader(rawBody),
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if binding.CallbackSecret != "" {
		req.Header.Set("X-GEWE-CALLBACK-TOKEN", binding.CallbackSecret)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("forward webhook failed: %v", err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		writeError(w, http.StatusBadGateway, fmt.Sprintf("callback returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body))))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"status":     "forwarded",
		"instanceId": binding.InstanceID,
		"groupId":    groupID,
	})
}

func (a *API) runOutboundWorker() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.cfg.WorkerPollInterval)
	defer ticker.Stop()

	for {
		processed, err := a.processNextOutboundJob(a.workerCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(a.workerCtx.Err(), context.Canceled) {
				return
			}
			slog.Error("process outbound job failed", "err", err)
		}
		if err == nil && processed {
			continue
		}

		select {
		case <-a.workerCtx.Done():
			return
		case <-a.workerSignal:
		case <-ticker.C:
		}
	}
}

func (a *API) runMaintenanceLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.cfg.MaintenanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.workerCtx.Done():
			return
		case <-ticker.C:
			if err := a.runMaintenanceOnce(a.workerCtx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(a.workerCtx.Err(), context.Canceled) {
					return
				}
				slog.Error("cleanup expired gateway data failed", "err", err)
			}
		}
	}
}

func (a *API) processNextOutboundJob(ctx context.Context) (bool, error) {
	job, ok, err := a.store.ClaimNextOutboundJob(ctx, time.Now().UTC())
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	exec := a.executeOutboundJob(ctx, job)
	if exec.Err == nil {
		if err := a.store.MarkOutboundSucceeded(ctx, job.JobID); err != nil {
			return true, err
		}
		a.notifyWaiter(job.JobID, outboundResult{
			StatusCode: exec.StatusCode,
			Headers:    exec.Headers,
			Body:       exec.Body,
		})
		return true, nil
	}

	lastError := exec.Err.Error()
	if exec.Retryable && job.AttemptCount < job.MaxAttempts {
		nextAttemptAt := time.Now().UTC().Add(a.retryDelay(job.AttemptCount))
		if err := a.store.RescheduleOutboundJob(ctx, job.JobID, nextAttemptAt, lastError); err != nil {
			return true, err
		}
		return true, nil
	}

	if err := a.store.MarkOutboundDead(ctx, job.JobID, lastError); err != nil {
		return true, err
	}
	a.notifyWaiter(job.JobID, a.buildTerminalFailure(exec, lastError))
	return true, nil
}

func (a *API) executeOutboundJob(ctx context.Context, job store.OutboundJob) outboundExecution {
	requestCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.ForwardTimeout)
	defer cancel()

	upstreamURL := strings.TrimRight(a.cfg.GeweAPIBaseURL, "/") + job.RequestPath
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, upstreamURL, bytes.NewReader(job.RequestBody))
	if err != nil {
		return outboundExecution{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GEWE-TOKEN", a.cfg.GeweToken)

	resp, err := a.client.Do(req)
	if err != nil {
		return outboundExecution{
			Retryable: true,
			Err:       fmt.Errorf("upstream request failed: %w", err),
		}
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return outboundExecution{
			Retryable: true,
			Err:       fmt.Errorf("read upstream response failed: %w", readErr),
		}
	}

	result := outboundExecution{
		StatusCode: resp.StatusCode,
		Headers:    cloneHeaders(resp.Header),
		Body:       body,
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return result
	}

	result.Retryable = isRetryableStatus(resp.StatusCode)
	result.Err = fmt.Errorf("upstream returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	return result
}

func (a *API) buildTerminalFailure(exec outboundExecution, lastError string) outboundResult {
	if exec.StatusCode > 0 {
		headers := cloneHeaders(exec.Headers)
		if len(exec.Body) == 0 {
			headers.Set("Content-Type", "application/json")
			return outboundResult{
				StatusCode: exec.StatusCode,
				Headers:    headers,
				Body:       []byte(fmt.Sprintf(`{"ok":false,"error":%q}`, lastError)),
			}
		}
		return outboundResult{
			StatusCode: exec.StatusCode,
			Headers:    headers,
			Body:       exec.Body,
		}
	}

	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	return outboundResult{
		StatusCode: http.StatusBadGateway,
		Headers:    headers,
		Body:       []byte(fmt.Sprintf(`{"ok":false,"error":%q}`, lastError)),
	}
}

func (a *API) retryDelay(attemptCount int) time.Duration {
	if attemptCount <= 0 {
		return a.cfg.OutboundBaseBackoff
	}
	delay := a.cfg.OutboundBaseBackoff
	for i := 1; i < attemptCount; i++ {
		if delay >= a.cfg.OutboundMaxBackoff {
			return a.cfg.OutboundMaxBackoff
		}
		delay *= 2
	}
	if delay > a.cfg.OutboundMaxBackoff {
		return a.cfg.OutboundMaxBackoff
	}
	return delay
}

func isRetryableStatus(statusCode int) bool {
	if statusCode >= 500 {
		return true
	}
	return statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests
}

func (a *API) signalWorker() {
	select {
	case a.workerSignal <- struct{}{}:
	default:
	}
}

func (a *API) addWaiter(jobID int64) chan outboundResult {
	ch := make(chan outboundResult, 1)
	a.waitersMu.Lock()
	a.waiters[jobID] = ch
	a.waitersMu.Unlock()
	return ch
}

func (a *API) removeWaiter(jobID int64, ch chan outboundResult) {
	a.waitersMu.Lock()
	current, ok := a.waiters[jobID]
	if ok && current == ch {
		delete(a.waiters, jobID)
	}
	a.waitersMu.Unlock()
}

func (a *API) notifyWaiter(jobID int64, result outboundResult) {
	a.waitersMu.Lock()
	ch, ok := a.waiters[jobID]
	if ok {
		delete(a.waiters, jobID)
	}
	a.waitersMu.Unlock()
	if !ok {
		return
	}

	select {
	case ch <- result:
	default:
	}
}

func (a *API) authorizeGatewayKey(r *http.Request) bool {
	expected := strings.TrimSpace(a.cfg.GatewayKey)
	if expected == "" {
		return true
	}
	token := strings.TrimSpace(r.Header.Get("X-Gateway-Key"))
	if token == "" {
		token = strings.TrimSpace(r.Header.Get("X-GeWe-Gateway-Key"))
	}
	return token == expected
}

func (a *API) runMaintenanceOnce(ctx context.Context) error {
	if a.cfg.InboundDedupeRetention > 0 {
		deleted, err := a.store.CleanupInboundDedupe(
			ctx,
			time.Now().UTC().Add(-a.cfg.InboundDedupeRetention),
		)
		if err != nil {
			return fmt.Errorf("cleanup inbound dedupe: %w", err)
		}
		if deleted > 0 {
			slog.Info("cleaned expired inbound dedupe rows", "deleted", deleted)
		}
	}

	if a.cfg.OutboundHistoryRetention > 0 {
		deleted, err := a.store.CleanupOutboundHistory(
			ctx,
			time.Now().UTC().Add(-a.cfg.OutboundHistoryRetention),
		)
		if err != nil {
			return fmt.Errorf("cleanup outbound history: %w", err)
		}
		if deleted > 0 {
			slog.Info("cleaned expired outbound jobs", "deleted", deleted)
		}
	}

	return nil
}

func (a *API) authorizeWebhook(r *http.Request) bool {
	expected := strings.TrimSpace(a.cfg.GeweWebhookSecret)
	if expected == "" {
		return true
	}
	token := strings.TrimSpace(r.Header.Get("X-GEWE-CALLBACK-TOKEN"))
	if token == "" {
		token = strings.TrimSpace(r.Header.Get("X-WebHook-Token"))
	}
	if token == expected {
		return true
	}
	queryToken := strings.TrimSpace(r.URL.Query().Get("token"))
	return queryToken == expected
}

func validateRegister(body registerRequest) error {
	if strings.TrimSpace(body.InstanceID) == "" {
		return errors.New("instanceId is required")
	}
	if strings.TrimSpace(body.CallbackURL) == "" {
		return errors.New("callbackUrl is required")
	}
	if _, err := url.ParseRequestURI(strings.TrimSpace(body.CallbackURL)); err != nil {
		return fmt.Errorf("callbackUrl is invalid: %w", err)
	}
	if len(normalizeGroups(body.Groups)) == 0 {
		return errors.New("groups is required")
	}
	return nil
}

func normalizeGroups(groups []string) []string {
	seen := make(map[string]struct{}, len(groups))
	result := make([]string, 0, len(groups))
	for _, item := range groups {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func buildDedupeKey(payload geweWebhookPayload) string {
	msgID := fmt.Sprintf("%v", payload.Data.NewMsgID)
	if strings.TrimSpace(msgID) == "" || msgID == "<nil>" {
		msgID = fmt.Sprintf("%v", payload.Data.MsgID)
	}
	return strings.TrimSpace(payload.AppID) + ":" + strings.TrimSpace(msgID)
}

func extractGroupID(payload geweWebhookPayload) string {
	fromID := strings.TrimSpace(payload.Data.FromUser.String)
	toID := strings.TrimSpace(payload.Data.ToUser.String)
	if strings.HasSuffix(fromID, "@chatroom") {
		return fromID
	}
	if strings.HasSuffix(toID, "@chatroom") {
		return toID
	}
	return ""
}

func (a *API) injectAppID(payload []byte) ([]byte, error) {
	if strings.TrimSpace(a.cfg.GeweAppID) == "" {
		return payload, nil
	}

	var body map[string]any
	if len(bytes.TrimSpace(payload)) == 0 {
		body = map[string]any{}
	} else if err := json.Unmarshal(payload, &body); err != nil {
		return nil, errors.New("request body must be JSON")
	}
	body["appId"] = a.cfg.GeweAppID
	return json.Marshal(body)
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func decodeOptionalJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"ok":    false,
		"error": message,
	})
}

func copyHeaders(dst http.Header, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func cloneHeaders(src http.Header) http.Header {
	if src == nil {
		return make(http.Header)
	}
	dst := make(http.Header, len(src))
	copyHeaders(dst, src)
	return dst
}
