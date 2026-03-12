package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"gewe-gateway/internal/config"
	"gewe-gateway/internal/store"
)

type API struct {
	cfg           config.Config
	store         *store.Store
	client        *http.Client
	mux           *http.ServeMux
	compatProxyMu sync.Mutex
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
	api := &API{
		cfg:   cfg,
		store: sqliteStore,
		client: &http.Client{
			Timeout: cfg.ForwardTimeout,
		},
		mux: http.NewServeMux(),
	}
	api.routes()
	return api, nil
}

func (a *API) Handler() http.Handler {
	return a.mux
}

func (a *API) routes() {
	a.mux.HandleFunc("GET /gateway/v1/healthz", a.handleHealthz)
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
	if strings.TrimSpace(a.cfg.GeweToken) == "" {
		writeError(w, http.StatusServiceUnavailable, "gewe token not configured")
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

	upstreamURL := strings.TrimRight(a.cfg.GeweAPIBaseURL, "/") + r.URL.Path
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL, bytes.NewReader(payload))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GEWE-TOKEN", a.cfg.GeweToken)

	a.compatProxyMu.Lock()
	defer a.compatProxyMu.Unlock()

	resp, err := a.client.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
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
