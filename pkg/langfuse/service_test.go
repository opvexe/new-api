package langfuse

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestClientSendsLangfuseTraceCreate(t *testing.T) {
	received := make(chan batchRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		assert.Equal(t, "/api/public/ingestion", request.URL.Path)
		assert.Equal(t, "Basic "+base64.StdEncoding.EncodeToString([]byte("pk:sk")), request.Header.Get("Authorization"))
		var batch batchRequest
		require.NoError(t, common.DecodeJson(request.Body, &batch))
		received <- batch
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Enabled:     true,
		Endpoint:    server.URL,
		PublicKey:   "pk",
		SecretKey:   "sk",
		Environment: "test",
	})
	require.NoError(t, err)
	client.Start()
	t.Cleanup(client.Stop)

	now := time.Now()
	client.TraceGeneration(GenerationRequest{
		TraceID:    "trace-1",
		Name:       "anthropic-messages",
		Model:      "claude-sonnet-4",
		StartTime:  now,
		EndTime:    now.Add(time.Second),
		Input:      map[string]any{"model": "claude-sonnet-4"},
		Output:     map[string]any{"type": "message"},
		Usage:      &Usage{Input: 10, Output: 5, Total: 15},
		UserID:     "user-1",
		UserName:   "alice",
		ApiKeyID:   "key-1",
		ApiKeyName: "default",
		Route:      "POST /v1/messages",
		SessionID:  "session-1",
		Metadata: map[string]any{
			"thinking_effort": "max",
		},
		ModelParameters: map[string]any{
			"thinking_effort": "max",
		},
	})

	select {
	case traceBatch := <-received:
		require.Len(t, traceBatch.Batch, 1)
		assert.Equal(t, EventTypeTraceCreate, traceBatch.Batch[0].Type)
		var trace traceBody
		require.NoError(t, common.Unmarshal(traceBatch.Batch[0].Body, &trace))
		assert.Equal(t, "trace-1", trace.ID)
		assert.Equal(t, "user-1", trace.UserID)
		assert.Equal(t, "session-1", trace.SessionID)
		assert.Equal(t, []string{"user:alice"}, trace.Tags)
		assert.Equal(t, map[string]any{"model": "claude-sonnet-4"}, trace.Input)
		assert.Equal(t, map[string]any{"type": "message"}, trace.Output)
		assert.Equal(t, "claude-sonnet-4", trace.Metadata["model"])
		assert.Equal(t, float64(1000), trace.Metadata["latency_ms"])
		assert.Equal(t, map[string]any{
			"input":  float64(10),
			"output": float64(5),
			"total":  float64(15),
		}, trace.Metadata["usage"])
		assert.Equal(t, map[string]any{"thinking_effort": "max"}, trace.Metadata["model_parameters"])
		assert.Equal(t, "max", trace.Metadata["thinking_effort"])
		assert.Equal(t, map[string]any{
			"user_id":     "user-1",
			"route":       "POST /v1/messages",
			"request_id":  "trace-1",
			"apikey_name": "default",
			"api_key_id":  "key-1",
		}, map[string]any{
			"user_id":     trace.Metadata["user_id"],
			"route":       trace.Metadata["route"],
			"request_id":  trace.Metadata["request_id"],
			"apikey_name": trace.Metadata["apikey_name"],
			"api_key_id":  trace.Metadata["api_key_id"],
		})
		assert.NotEmpty(t, trace.Metadata["end_time"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for langfuse trace ingestion request")
	}
}

func TestNewClientFromEnvDisabled(t *testing.T) {
	t.Setenv("LANGFUSE_ENABLED", "false")

	client, err := NewClientFromEnv()

	require.NoError(t, err)
	assert.Nil(t, client)
}

func TestClientStopCancelsBlockedIngestion(t *testing.T) {
	requestStarted := make(chan struct{})
	var requestStartedOnce sync.Once

	client, err := NewClient(Config{
		Enabled:   true,
		Endpoint:  "http://langfuse.test",
		PublicKey: "pk",
		SecretKey: "sk",
	})
	require.NoError(t, err)
	client.httpClient.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestStartedOnce.Do(func() {
			close(requestStarted)
		})
		<-request.Context().Done()
		return nil, errors.New("request canceled")
	})
	client.Start()
	now := time.Now()
	client.TraceGeneration(GenerationRequest{StartTime: now, EndTime: now})

	select {
	case <-requestStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for blocked ingestion request")
	}
	startedAt := time.Now()
	client.Stop()
	assert.Less(t, time.Since(startedAt), shutdownTimeout+time.Second)
}
