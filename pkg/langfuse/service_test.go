package langfuse

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTraceGenerationIncludesChannelMetadata(t *testing.T) {
	client := &Client{
		environment: "test",
		eventCh:     make(chan event, 1),
		stopCh:      make(chan struct{}),
	}
	startedAt := time.Now()
	client.TraceGeneration(GenerationRequest{
		TraceID:     "trace-channel-metadata",
		Name:        "anthropic-messages",
		Model:       "claude-opus-5",
		StartTime:   startedAt,
		EndTime:     startedAt.Add(time.Second),
		ChannelID:   3,
		ChannelName: "cf-test",
		ChannelType: constant.ChannelCloudflare,
	})

	item := <-client.eventCh
	var trace traceBody
	require.NoError(t, common.Unmarshal(item.Body, &trace))
	assert.Equal(t, "cf-test", trace.Metadata["channel"])
	assert.Equal(t, float64(3), trace.Metadata["channel_id"])
	assert.Equal(t, "Cloudflare", trace.Metadata["channel_type"])
}

func TestTraceGenerationIncludesSiteMetadata(t *testing.T) {
	client := &Client{
		environment: "test",
		eventCh:     make(chan event, 1),
		stopCh:      make(chan struct{}),
	}
	startedAt := time.Now()
	client.TraceGeneration(GenerationRequest{
		TraceID:   "trace-site-tag",
		Name:      "anthropic-messages",
		StartTime: startedAt,
		EndTime:   startedAt.Add(time.Second),
		UserName:  "alice",
		SiteTag:   "baidu",
	})

	item := <-client.eventCh
	var trace traceBody
	require.NoError(t, common.Unmarshal(item.Body, &trace))
	assert.Equal(t, "baidu", trace.Metadata["site"])
	assert.Equal(t, []string{"user:alice"}, trace.Tags)
}

// Trace events carry the full request and response payload. Batching them
// without a byte budget produces ingestion requests large enough to be
// rejected, and a rejected request loses every event it carried.
func TestIngestionBatchesStayWithinByteBudget(t *testing.T) {
	var mu sync.Mutex
	var requestSizes []int
	deliveredEvents := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload batchRequest
		require.NoError(t, common.DecodeJson(r.Body, &payload))
		mu.Lock()
		requestSizes = append(requestSizes, len(payload.Batch))
		deliveredEvents += len(payload.Batch)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Enabled:      true,
		Endpoint:     server.URL,
		PublicKey:    "pk",
		SecretKey:    "sk",
		MaxQueueSize: defaultQueueSize,
		Environment:  "test",
	})
	require.NoError(t, err)
	client.Start()

	const eventCount = 10
	const payloadSize = 300 * 1024
	startedAt := time.Now()
	for i := 0; i < eventCount; i++ {
		client.TraceGeneration(GenerationRequest{
			TraceID:   "trace-byte-budget",
			Name:      "anthropic-messages",
			StartTime: startedAt,
			EndTime:   startedAt.Add(time.Second),
			Input:     strings.Repeat("x", payloadSize),
		})
	}
	client.Stop()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, eventCount, deliveredEvents)
	assert.Zero(t, client.dropped.Load())
	require.Greater(t, len(requestSizes), 1, "oversized events must be split across requests")
	// Each request stays within the budget, allowing the single event that is
	// appended before the budget is rechecked.
	maxEventsPerRequest := maxBatchBytes/payloadSize + 1
	for _, size := range requestSizes {
		assert.LessOrEqual(t, size, maxEventsPerRequest)
	}
}

func TestTraceGenerationDropsInsteadOfBlockingWhenQueueIsFull(t *testing.T) {
	client := &Client{
		environment: "test",
		eventCh:     make(chan event, 1),
		stopCh:      make(chan struct{}),
	}
	startedAt := time.Now()
	request := GenerationRequest{
		TraceID:   "trace-queue-full",
		Name:      "anthropic-messages",
		StartTime: startedAt,
		EndTime:   startedAt.Add(time.Second),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// The second call has no room left in the queue and must not block the
		// relay request goroutine.
		client.TraceGeneration(request)
		client.TraceGeneration(request)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "TraceGeneration blocked when the queue was full")
	}
	assert.Len(t, client.eventCh, 1)
	assert.Equal(t, uint64(1), client.dropped.Load())
}
