package langfuse

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMiddlewareRecordsOnlySuccessfulClaudeMessages(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client, err := NewClient(Config{
		Enabled:   true,
		Endpoint:  "http://langfuse.test",
		PublicKey: "pk",
		SecretKey: "sk",
	})
	require.NoError(t, err)

	router := gin.New()
	router.POST("/v1/messages", Middleware(client), func(c *gin.Context) {
		c.Set(common.RequestIdKey, "request-1")
		c.Set("id", 123)
		c.Set("token_id", 456)
		c.Set("token_name", "default")
		c.JSON(http.StatusOK, gin.H{
			"id":    "msg_1",
			"type":  "message",
			"model": "claude-sonnet-4",
			"usage": gin.H{
				"input_tokens":                10,
				"output_tokens":               4,
				"cache_creation_input_tokens": 2,
				"cache_read_input_tokens":     3,
			},
		})
	})
	router.POST("/v1/chat/completions", Middleware(client), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"id": "chat_1"})
	})

	requestBody := `{"model":"claude-sonnet-4","max_tokens":128,"metadata":{"session_id":"session-1"},"messages":[{"role":"user","content":"hello"}]}`
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Request-Id", "53a170f02952ac9f170aeb796c15dac6-7000b458a8dd3b4c")
	request.Header.Set("X-External-User-Id", "aca91010-893b-487d-ae10-faa62f54ae2c")
	request.Header.Set("X-External-Api-Key-Id", "19dd21ec-2ea4-4029-952e-d88c5fb827e9")
	request.Header.Set("X-External-Api-Key-Name", "default")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Len(t, client.eventCh, 2)

	traceEvent := <-client.eventCh
	var trace struct {
		ID        string `json:"id"`
		UserID    string `json:"userId"`
		SessionID string `json:"sessionId"`
		Metadata  map[string]any
	}
	require.NoError(t, common.Unmarshal(traceEvent.Body, &trace))
	assert.Equal(t, "53a170f02952ac9f170aeb796c15dac6-7000b458a8dd3b4c", trace.ID)
	assert.Equal(t, "aca91010-893b-487d-ae10-faa62f54ae2c", trace.UserID)
	assert.Equal(t, "session-1", trace.SessionID)
	assert.Equal(t, map[string]any{
		"user_id":         "aca91010-893b-487d-ae10-faa62f54ae2c",
		"thinking_effort": "max",
		"route":           "POST /v1/messages",
		"request_id":      "53a170f02952ac9f170aeb796c15dac6-7000b458a8dd3b4c",
		"apikey_name":     "default",
		"api_key_id":      "19dd21ec-2ea4-4029-952e-d88c5fb827e9",
	}, trace.Metadata)
	generationEvent := <-client.eventCh
	var generation struct {
		Input           json.RawMessage `json:"input"`
		Output          json.RawMessage `json:"output"`
		Usage           Usage           `json:"usage"`
		ModelParameters map[string]any  `json:"modelParameters"`
		Metadata        map[string]any  `json:"metadata"`
	}
	require.NoError(t, common.Unmarshal(generationEvent.Body, &generation))
	assert.JSONEq(t, requestBody, string(generation.Input))
	assert.JSONEq(t, response.Body.String(), string(generation.Output))
	assert.Equal(t, Usage{Input: 15, Output: 4, Total: 19}, generation.Usage)
	assert.Equal(t, map[string]any{"thinking_effort": "max"}, generation.ModelParameters)
	assert.Equal(t, trace.Metadata, generation.Metadata)

	request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Len(t, client.eventCh, 2)

	traceEvent = <-client.eventCh
	require.NoError(t, common.Unmarshal(traceEvent.Body, &trace))
	assert.Equal(t, "request-1", trace.ID)
	assert.Equal(t, "123", trace.UserID)
	assert.Equal(t, map[string]any{
		"user_id":         "123",
		"thinking_effort": "max",
		"route":           "POST /v1/messages",
		"request_id":      "request-1",
		"apikey_name":     "default",
		"api_key_id":      "456",
	}, trace.Metadata)
	<-client.eventCh

	request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"claude-sonnet-4"}`))
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	assert.Empty(t, client.eventCh)
}

func TestMiddlewareRecordsCompleteClaudeStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client, err := NewClient(Config{
		Enabled:   true,
		Endpoint:  "http://langfuse.test",
		PublicKey: "pk",
		SecretKey: "sk",
	})
	require.NoError(t, err)

	router := gin.New()
	router.POST("/v1/messages", Middleware(client), func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.WriteString("event: message_start\n")
		_, _ = c.Writer.WriteString("data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":7,\"cache_read_input_tokens\":2}}}\n\n")
		_, _ = c.Writer.WriteString("event: message_delta\n")
		_, _ = c.Writer.WriteString("data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":5}}\n\n")
	})

	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4","stream":true,"messages":[{"role":"user","content":"hello"}]}`))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Len(t, client.eventCh, 2)

	<-client.eventCh
	generationEvent := <-client.eventCh
	var generation struct {
		Output string `json:"output"`
		Usage  Usage  `json:"usage"`
	}
	require.NoError(t, common.Unmarshal(generationEvent.Body, &generation))
	assert.Equal(t, response.Body.String(), generation.Output)
	assert.Equal(t, Usage{Input: 9, Output: 5, Total: 14}, generation.Usage)
}

func TestMiddlewareSkipsFailedClaudeMessages(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client, err := NewClient(Config{
		Enabled:   true,
		Endpoint:  "http://langfuse.test",
		PublicKey: "pk",
		SecretKey: "sk",
	})
	require.NoError(t, err)

	router := gin.New()
	router.POST("/v1/messages", Middleware(client), func(c *gin.Context) {
		c.JSON(http.StatusBadGateway, gin.H{"type": "error"})
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4","messages":[{"role":"user","content":"hello"}]}`))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	assert.Equal(t, http.StatusBadGateway, response.Code)
	assert.Empty(t, client.eventCh)
}
