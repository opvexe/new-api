package langfuse

import (
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
