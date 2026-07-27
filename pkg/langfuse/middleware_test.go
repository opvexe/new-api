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
	require.Len(t, client.eventCh, 1)

	traceEvent := <-client.eventCh
	assert.Equal(t, EventTypeTraceCreate, traceEvent.Type)
	var trace struct {
		ID        string          `json:"id"`
		UserID    string          `json:"userId"`
		SessionID string          `json:"sessionId"`
		Input     json.RawMessage `json:"input"`
		Output    json.RawMessage `json:"output"`
		Metadata  map[string]any  `json:"metadata"`
	}
	require.NoError(t, common.Unmarshal(traceEvent.Body, &trace))
	assert.Equal(t, "53a170f02952ac9f170aeb796c15dac6-7000b458a8dd3b4c", trace.ID)
	assert.Equal(t, "aca91010-893b-487d-ae10-faa62f54ae2c", trace.UserID)
	assert.Equal(t, "session-1", trace.SessionID)
	assert.JSONEq(t, requestBody, string(trace.Input))
	assert.JSONEq(t, response.Body.String(), string(trace.Output))
	assert.Equal(t, "aca91010-893b-487d-ae10-faa62f54ae2c", trace.Metadata["user_id"])
	assert.Equal(t, "POST /v1/messages", trace.Metadata["route"])
	assert.Equal(t, "53a170f02952ac9f170aeb796c15dac6-7000b458a8dd3b4c", trace.Metadata["request_id"])
	assert.Equal(t, "default", trace.Metadata["apikey_name"])
	assert.Equal(t, "19dd21ec-2ea4-4029-952e-d88c5fb827e9", trace.Metadata["api_key_id"])
	assert.Equal(t, "claude-sonnet-4", trace.Metadata["model"])
	assert.Equal(t, map[string]any{
		"input":  float64(15),
		"output": float64(4),
		"total":  float64(19),
	}, trace.Metadata["usage"])
	assert.Equal(t, map[string]any{}, trace.Metadata["model_parameters"])
	assert.NotEmpty(t, trace.Metadata["end_time"])
	assert.GreaterOrEqual(t, trace.Metadata["latency_ms"], float64(0))

	request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Len(t, client.eventCh, 1)

	traceEvent = <-client.eventCh
	require.NoError(t, common.Unmarshal(traceEvent.Body, &trace))
	assert.Equal(t, "request-1", trace.ID)
	assert.Equal(t, "123", trace.UserID)
	assert.Equal(t, "123", trace.Metadata["user_id"])
	assert.Equal(t, "POST /v1/messages", trace.Metadata["route"])
	assert.Equal(t, "request-1", trace.Metadata["request_id"])
	assert.Equal(t, "default", trace.Metadata["apikey_name"])
	assert.Equal(t, "456", trace.Metadata["api_key_id"])

	request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"claude-sonnet-4"}`))
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	assert.Empty(t, client.eventCh)
}

func TestMiddlewareRecordsForcedClaudeAdaptiveThinkingInput(t *testing.T) {
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
		c.JSON(http.StatusOK, gin.H{
			"id":    "msg_forced",
			"type":  "message",
			"model": "claude-fable-5",
		})
	})

	requestBody := `{
		"model":"claude-fable-5",
		"thinking":{"type":"enabled","budget_tokens":2048,"display":"summarized"},
		"output_config":{"effort":"low"},
		"temperature":0.2,
		"top_p":0.8,
		"top_k":20,
		"messages":[{"role":"user","content":"hello"}]
	}`
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(requestBody))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Len(t, client.eventCh, 1)

	traceEvent := <-client.eventCh
	assert.Equal(t, EventTypeTraceCreate, traceEvent.Type)
	var trace struct {
		Input    json.RawMessage `json:"input"`
		Output   json.RawMessage `json:"output"`
		Metadata map[string]any  `json:"metadata"`
	}
	require.NoError(t, common.Unmarshal(traceEvent.Body, &trace))

	expectedInput := `{
		"model":"claude-fable-5",
		"thinking":{"type":"adaptive","display":"summarized"},
		"output_config":{"effort":"max"},
		"messages":[{"role":"user","content":"hello"}]
	}`
	assert.JSONEq(t, expectedInput, string(trace.Input))
	assert.JSONEq(t, response.Body.String(), string(trace.Output))
	assert.Equal(t, "max", trace.Metadata["thinking_effort"])
	assert.Equal(t, "claude-fable-5", trace.Metadata["model"])
	assert.Equal(t, map[string]any{"thinking_effort": "max"}, trace.Metadata["model_parameters"])
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
	require.Len(t, client.eventCh, 1)

	traceEvent := <-client.eventCh
	assert.Equal(t, EventTypeTraceCreate, traceEvent.Type)
	var trace struct {
		Output   string         `json:"output"`
		Metadata map[string]any `json:"metadata"`
	}
	require.NoError(t, common.Unmarshal(traceEvent.Body, &trace))
	assert.Equal(t, response.Body.String(), trace.Output)
	assert.Equal(t, map[string]any{
		"input":  float64(9),
		"output": float64(5),
		"total":  float64(14),
	}, trace.Metadata["usage"])
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
