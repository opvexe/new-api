package cloudflare

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/types"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cloudflareRelayInfo(mode dto.CloudflareAPIMode) *relaycommon.RelayInfo {
	target := "account-id"
	if mode == dto.CloudflareAPIModeBYOK {
		target = "account-id/gateway-id"
	}
	return &relaycommon.RelayInfo{
		ChannelMeta: &relaycommon.ChannelMeta{
			ApiVersion:     target,
			ChannelBaseUrl: restAPIBaseURL,
			ApiKey:         "channel-key",
			ChannelOtherSettings: dto.ChannelOtherSettings{
				CloudflareAPIMode: mode,
			},
		},
	}
}

func TestGetRequestURLSupportsRESTAndBYOKModes(t *testing.T) {
	tests := []struct {
		name        string
		mode        dto.CloudflareAPIMode
		relayMode   int
		relayFormat types.RelayFormat
		path        string
		want        string
	}{
		{
			name:      "REST chat completions",
			relayMode: relayconstant.RelayModeChatCompletions,
			want:      "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/chat/completions",
		},
		{
			name:      "REST embeddings",
			relayMode: relayconstant.RelayModeEmbeddings,
			want:      "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/embeddings",
		},
		{
			name:      "REST responses",
			relayMode: relayconstant.RelayModeResponses,
			want:      "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/responses",
		},
		{
			name:        "REST Anthropic messages",
			relayFormat: types.RelayFormatClaude,
			want:        "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/messages",
		},
		{
			name:      "BYOK chat completions",
			mode:      dto.CloudflareAPIModeBYOK,
			relayMode: relayconstant.RelayModeChatCompletions,
			want:      "https://gateway.ai.cloudflare.com/v1/account-id/gateway-id/compat/chat/completions",
		},
		{
			name:        "BYOK Anthropic messages",
			mode:        dto.CloudflareAPIModeBYOK,
			relayFormat: types.RelayFormatClaude,
			want:        "https://gateway.ai.cloudflare.com/v1/account-id/gateway-id/anthropic/v1/messages",
		},
		{
			name: "BYOK compatible fallback",
			mode: dto.CloudflareAPIModeBYOK,
			path: "/models",
			want: "https://gateway.ai.cloudflare.com/v1/account-id/gateway-id/compat/models",
		},
	}

	adaptor := &Adaptor{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := cloudflareRelayInfo(test.mode)
			info.RelayMode = test.relayMode
			info.RelayFormat = test.relayFormat
			info.RequestURLPath = test.path

			got, err := adaptor.GetRequestURL(info)

			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestQualifyModelForRESTAndBYOK(t *testing.T) {
	tests := []struct {
		name  string
		model string
		byok  bool
		want  string
	}{
		{name: "REST Anthropic", model: "claude-opus-5", want: "anthropic/claude-opus-5"},
		{name: "REST Google", model: "gemini-3-flash", want: "google/gemini-3-flash"},
		{name: "REST xAI", model: "grok-3", want: "xai/grok-3"},
		{name: "REST Workers AI", model: "@cf/meta/llama-3.1-8b", want: "@cf/meta/llama-3.1-8b"},
		{name: "BYOK Google", model: "gemini-3-flash", byok: true, want: "google-ai-studio/gemini-3-flash"},
		{name: "BYOK Workers AI", model: "@cf/meta/llama-3.1-8b", byok: true, want: "workers-ai/@cf/meta/llama-3.1-8b"},
		{name: "explicit provider", model: "anthropic/claude-opus-5", want: "anthropic/claude-opus-5"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, qualifyModel(test.model, test.byok))
		})
	}
}

func TestConvertClaudeRequestUsesModeSpecificModel(t *testing.T) {
	adaptor := &Adaptor{}
	tests := []struct {
		name  string
		mode  dto.CloudflareAPIMode
		model string
		want  string
	}{
		{
			name:  "REST qualifies bare Anthropic model",
			model: "claude-opus-5",
			want:  "anthropic/claude-opus-5",
		},
		{
			name:  "REST preserves qualified Anthropic model",
			model: "anthropic/claude-opus-5",
			want:  "anthropic/claude-opus-5",
		},
		{
			name:  "BYOK preserves bare provider-native model",
			mode:  dto.CloudflareAPIModeBYOK,
			model: "claude-opus-5",
			want:  "claude-opus-5",
		},
		{
			name:  "BYOK removes REST prefix for provider-native endpoint",
			mode:  dto.CloudflareAPIModeBYOK,
			model: "anthropic/claude-opus-5",
			want:  "claude-opus-5",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := cloudflareRelayInfo(test.mode)
			info.OriginModelName = "client-model-for-billing"
			request := &dto.ClaudeRequest{Model: test.model}

			converted, err := adaptor.ConvertClaudeRequest(nil, info, request)

			require.NoError(t, err)
			assert.Same(t, request, converted)
			assert.Equal(t, test.want, request.Model)
			assert.Equal(t, "client-model-for-billing", info.OriginModelName)
		})
	}
}

func TestConvertOpenAIRequestPreservesProviderPrefixInCompatMode(t *testing.T) {
	adaptor := &Adaptor{}

	for _, mode := range []dto.CloudflareAPIMode{
		dto.CloudflareAPIModeREST,
		dto.CloudflareAPIModeBYOK,
	} {
		t.Run(string(mode), func(t *testing.T) {
			info := cloudflareRelayInfo(mode)
			info.RelayMode = relayconstant.RelayModeChatCompletions
			info.OriginModelName = "client-model-for-billing"
			request := &dto.GeneralOpenAIRequest{Model: "anthropic/claude-opus-5"}

			converted, err := adaptor.ConvertOpenAIRequest(nil, info, request)

			require.NoError(t, err)
			assert.Same(t, request, converted)
			assert.Equal(t, "anthropic/claude-opus-5", request.Model)
			assert.Equal(t, "client-model-for-billing", info.OriginModelName)
		})
	}
}

func TestSetupRequestHeaderUsesModeSpecificAuthentication(t *testing.T) {
	tests := []struct {
		name        string
		mode        dto.CloudflareAPIMode
		relayFormat types.RelayFormat
		check       func(t *testing.T, header http.Header)
	}{
		{
			name:        "REST Anthropic uses Cloudflare token",
			relayFormat: types.RelayFormatClaude,
			check: func(t *testing.T, header http.Header) {
				assert.Equal(t, "Bearer channel-key", header.Get("Authorization"))
				assert.Empty(t, header.Get("x-api-key"))
			},
		},
		{
			name:        "BYOK Anthropic uses provider key",
			mode:        dto.CloudflareAPIModeBYOK,
			relayFormat: types.RelayFormatClaude,
			check: func(t *testing.T, header http.Header) {
				assert.Empty(t, header.Get("Authorization"))
				assert.Equal(t, "channel-key", header.Get("x-api-key"))
				assert.Equal(t, "2024-01-01", header.Get("anthropic-version"))
				assert.Equal(t, "prompt-caching-2024-07-31", header.Get("anthropic-beta"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			c.Request.Header.Set("cf-aig-cache-ttl", "3600")
			c.Request.Header.Set("cf-aig-authorization", "client-secret")
			c.Request.Header.Set("anthropic-version", "2024-01-01")
			c.Request.Header.Set("anthropic-beta", "prompt-caching-2024-07-31")
			header := http.Header{}
			info := cloudflareRelayInfo(test.mode)
			info.RelayFormat = test.relayFormat

			err := (&Adaptor{}).SetupRequestHeader(c, &header, info)

			require.NoError(t, err)
			assert.Equal(t, "3600", header.Get("cf-aig-cache-ttl"))
			assert.Empty(t, header.Get("cf-aig-authorization"))
			test.check(t, header)
		})
	}
}

func TestPassthroughResponseHeadersDoesNotExposeAuthorization(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Add("cf-aig-request-id", "request-1")
	resp.Header.Add("cf-aig-custom", "custom-1")
	resp.Header.Add("cf-aig-custom", "custom-2")
	resp.Header.Set("cf-aig-authorization", "upstream-secret")
	resp.Header.Set("cf-ray", "ray-1")
	resp.Header.Set("x-secret", "must-not-pass")

	(&Adaptor{}).passthroughResponseHeaders(c, resp)

	assert.Equal(t, "request-1", recorder.Header().Get("cf-aig-request-id"))
	assert.Equal(t, []string{"custom-1", "custom-2"}, recorder.Header().Values("cf-aig-custom"))
	assert.Empty(t, recorder.Header().Get("cf-aig-authorization"))
	assert.Equal(t, "ray-1", recorder.Header().Get("cf-ray"))
	assert.Empty(t, recorder.Header().Get("x-secret"))
}

func TestDoResponseChatPreservesOfficialBodyAndUsage(t *testing.T) {
	body := `{"id":"upstream-chat-id","object":"chat.completion","created":1710000000,"model":"anthropic/claude-opus-5","choices":[{"index":0,"message":{"role":"assistant","content":"CHAT_OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18,"prompt_tokens_details":{"cached_tokens":3}},"cloudflare":{"request_id":"cf-request"}}`
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	info := cloudflareRelayInfo(dto.CloudflareAPIModeREST)
	info.RelayMode = relayconstant.RelayModeChatCompletions
	info.RelayFormat = types.RelayFormatOpenAI
	info.UpstreamModelName = "anthropic/claude-opus-5"
	info.SetEstimatePromptTokens(999)

	usageValue, apiErr := (&Adaptor{}).DoResponse(c, resp, info)

	require.Nil(t, apiErr)
	usage, ok := usageValue.(*dto.Usage)
	require.True(t, ok)
	assert.Equal(t, 11, usage.PromptTokens)
	assert.Equal(t, 7, usage.CompletionTokens)
	assert.Equal(t, 18, usage.TotalTokens)
	assert.Equal(t, 3, usage.PromptTokensDetails.CachedTokens)
	assert.Equal(t, body, recorder.Body.String())
	assert.False(t, common.GetContextKeyBool(c, constant.ContextKeyLocalCountTokens))
}

func TestDoResponseChatMissingUsageEstimatesOnlyForBilling(t *testing.T) {
	body := `{"id":"upstream-chat-id","object":"chat.completion","model":"anthropic/claude-opus-5","choices":[{"index":0,"message":{"role":"assistant","content":"CHAT_OK"},"finish_reason":"stop"}],"cloudflare":{"request_id":"cf-request"}}`
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	info := cloudflareRelayInfo(dto.CloudflareAPIModeREST)
	info.RelayMode = relayconstant.RelayModeChatCompletions
	info.RelayFormat = types.RelayFormatOpenAI
	info.UpstreamModelName = "anthropic/claude-opus-5"
	info.SetEstimatePromptTokens(37)

	usageValue, apiErr := (&Adaptor{}).DoResponse(c, resp, info)

	require.Nil(t, apiErr)
	usage, ok := usageValue.(*dto.Usage)
	require.True(t, ok)
	assert.Equal(t, 37, usage.PromptTokens)
	assert.Greater(t, usage.CompletionTokens, 0)
	assert.Equal(t, usage.PromptTokens+usage.CompletionTokens, usage.TotalTokens)
	assert.Equal(t, body, recorder.Body.String())
	assert.NotContains(t, recorder.Body.String(), `"usage"`)
	assert.True(t, common.GetContextKeyBool(c, constant.ContextKeyLocalCountTokens))
}

func TestDoResponseChatStreamUsesTerminalOfficialUsage(t *testing.T) {
	previousStreamingTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() {
		constant.StreamingTimeout = previousStreamingTimeout
	})

	body := strings.Join([]string{
		`data: {"id":"upstream-stream-id","object":"chat.completion.chunk","created":1710000000,"model":"anthropic/claude-opus-5","choices":[{"index":0,"delta":{"role":"assistant","content":"STREAM_OK"},"finish_reason":null}]}`,
		`data: {"id":"upstream-stream-id","object":"chat.completion.chunk","created":1710000000,"model":"anthropic/claude-opus-5","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}`,
		`data: [DONE]`,
		``,
	}, "\n")
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	info := cloudflareRelayInfo(dto.CloudflareAPIModeREST)
	info.RelayMode = relayconstant.RelayModeChatCompletions
	info.RelayFormat = types.RelayFormatOpenAI
	info.UpstreamModelName = "anthropic/claude-opus-5"
	info.IsStream = true
	info.ShouldIncludeUsage = true
	info.DisablePing = true
	info.SetEstimatePromptTokens(999)

	usageValue, apiErr := (&Adaptor{}).DoResponse(c, resp, info)

	require.Nil(t, apiErr)
	usage, ok := usageValue.(*dto.Usage)
	require.True(t, ok)
	assert.Equal(t, 11, usage.PromptTokens)
	assert.Equal(t, 7, usage.CompletionTokens)
	assert.Equal(t, 18, usage.TotalTokens)
	assert.Contains(t, recorder.Body.String(), `"id":"upstream-stream-id"`)
	assert.Contains(t, recorder.Body.String(), `"model":"anthropic/claude-opus-5"`)
	assert.Contains(t, recorder.Body.String(), `"content":"STREAM_OK"`)
	assert.Contains(t, recorder.Body.String(), `"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18`)
	assert.False(t, common.GetContextKeyBool(c, constant.ContextKeyLocalCountTokens))
}

func TestDoResponseEmbeddingPreservesOpaqueDataAndOfficialUsage(t *testing.T) {
	body := `{"object":"list","data":[{"object":"embedding","index":0,"embedding":"AQIDBA=="}],"model":"@cf/baai/bge-base-en-v1.5","usage":{"prompt_tokens":13,"total_tokens":13},"cloudflare":{"request_id":"cf-request"}}`
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/embeddings", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	info := cloudflareRelayInfo(dto.CloudflareAPIModeREST)
	info.RelayMode = relayconstant.RelayModeEmbeddings
	info.RelayFormat = types.RelayFormatOpenAI
	info.UpstreamModelName = "@cf/baai/bge-base-en-v1.5"
	info.SetEstimatePromptTokens(999)

	usageValue, apiErr := (&Adaptor{}).DoResponse(c, resp, info)

	require.Nil(t, apiErr)
	usage, ok := usageValue.(*dto.Usage)
	require.True(t, ok)
	assert.Equal(t, 13, usage.PromptTokens)
	assert.Equal(t, 0, usage.CompletionTokens)
	assert.Equal(t, 13, usage.TotalTokens)
	assert.Equal(t, body, recorder.Body.String())
	assert.False(t, common.GetContextKeyBool(c, constant.ContextKeyLocalCountTokens))
}

func TestDoResponseEmbeddingMissingUsageEstimatesOnlyForBilling(t *testing.T) {
	body := `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"model":"@cf/baai/bge-base-en-v1.5"}`
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/embeddings", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	info := cloudflareRelayInfo(dto.CloudflareAPIModeREST)
	info.RelayMode = relayconstant.RelayModeEmbeddings
	info.RelayFormat = types.RelayFormatOpenAI
	info.UpstreamModelName = "@cf/baai/bge-base-en-v1.5"
	info.SetEstimatePromptTokens(23)

	usageValue, apiErr := (&Adaptor{}).DoResponse(c, resp, info)

	require.Nil(t, apiErr)
	usage, ok := usageValue.(*dto.Usage)
	require.True(t, ok)
	assert.Equal(t, 23, usage.PromptTokens)
	assert.Equal(t, 0, usage.CompletionTokens)
	assert.Equal(t, 23, usage.TotalTokens)
	assert.Equal(t, body, recorder.Body.String())
	assert.NotContains(t, recorder.Body.String(), `"usage"`)
	assert.True(t, common.GetContextKeyBool(c, constant.ContextKeyLocalCountTokens))
}
