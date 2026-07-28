package cloudflare

import (
	"encoding/json"
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
	"github.com/QuantumNous/new-api/service"
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

func TestConvertClaudeRequestNormalizesRESTSystemCompatibility(t *testing.T) {
	adaptor := &Adaptor{}
	request := &dto.ClaudeRequest{
		Model: "claude-opus-5",
		System: []map[string]any{
			{
				"type": "text",
				"text": "global instruction",
				"cache_control": map[string]any{
					"type": "ephemeral",
				},
			},
			{
				"type": "text",
				"text": "second instruction",
			},
		},
		Messages: []dto.ClaudeMessage{
			{Role: "user", Content: "question"},
			{
				Role: "system",
				Content: []map[string]any{
					{"type": "text", "text": "mid-conversation instruction"},
				},
			},
			{Role: "developer", Content: "developer instruction"},
			{Role: "assistant", Content: "answer"},
		},
	}

	converted, err := adaptor.ConvertClaudeRequest(nil, cloudflareRelayInfo(dto.CloudflareAPIModeREST), request)

	require.NoError(t, err)
	assert.Same(t, request, converted)
	assert.Equal(t, "anthropic/claude-opus-5", request.Model)
	assert.Equal(
		t,
		"global instruction\nsecond instruction\nmid-conversation instruction\ndeveloper instruction",
		request.System,
	)
	assert.Equal(t, []dto.ClaudeMessage{
		{Role: "user", Content: "question"},
		{Role: "assistant", Content: "answer"},
	}, request.Messages)
}

func TestConvertClaudeRequestPreservesBYOKClaudeSystemShape(t *testing.T) {
	adaptor := &Adaptor{}
	system := []map[string]any{
		{"type": "text", "text": "global instruction"},
	}
	messages := []dto.ClaudeMessage{
		{Role: "user", Content: "question"},
		{Role: "system", Content: "mid-conversation instruction"},
	}
	request := &dto.ClaudeRequest{
		Model:    "anthropic/claude-opus-5",
		System:   system,
		Messages: messages,
	}

	converted, err := adaptor.ConvertClaudeRequest(nil, cloudflareRelayInfo(dto.CloudflareAPIModeBYOK), request)

	require.NoError(t, err)
	assert.Same(t, request, converted)
	assert.Equal(t, "claude-opus-5", request.Model)
	assert.Equal(t, system, request.System)
	assert.Equal(t, messages, request.Messages)
}

func TestConvertClaudeRequestPreservesBYOKNativeToolsAndMCP(t *testing.T) {
	maxTokens := uint(1024)
	request := &dto.ClaudeRequest{
		Model:     "anthropic/claude-sonnet-4-5",
		MaxTokens: &maxTokens,
		System:    "Use the supplied context and tools.",
		Messages: []dto.ClaudeMessage{
			{Role: "user", Content: "Find the Cloudflare AI Gateway web search documentation."},
		},
		Tools: []map[string]any{
			{
				"name":        "get_context",
				"description": "Read application context",
				"input_schema": map[string]any{
					"type":       "object",
					"properties": map[string]any{},
				},
			},
			{
				"type":     "web_search_20250305",
				"name":     "web_search",
				"max_uses": 1,
			},
			{
				"type":            "mcp_toolset",
				"mcp_server_name": "cloudflare-docs",
			},
		},
		ToolChoice: map[string]any{
			"type": "auto",
		},
		McpServers: json.RawMessage(`[
			{
				"type": "url",
				"url": "https://docs.mcp.cloudflare.com/mcp",
				"name": "cloudflare-docs"
			}
		]`),
		ContextManagement: json.RawMessage(`{"edits":[{"type":"clear_tool_uses_20250919"}]}`),
	}

	converted, err := (&Adaptor{}).ConvertClaudeRequest(
		nil,
		cloudflareRelayInfo(dto.CloudflareAPIModeBYOK),
		request,
	)

	require.NoError(t, err)
	body, err := common.Marshal(converted)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"model": "claude-sonnet-4-5",
		"max_tokens": 1024,
		"system": "Use the supplied context and tools.",
		"messages": [
			{
				"role": "user",
				"content": "Find the Cloudflare AI Gateway web search documentation."
			}
		],
		"tools": [
			{
				"name": "get_context",
				"description": "Read application context",
				"input_schema": {
					"type": "object",
					"properties": {}
				}
			},
			{
				"type": "web_search_20250305",
				"name": "web_search",
				"max_uses": 1
			},
			{
				"type": "mcp_toolset",
				"mcp_server_name": "cloudflare-docs"
			}
		],
		"tool_choice": {
			"type": "auto"
		},
		"mcp_servers": [
			{
				"type": "url",
				"url": "https://docs.mcp.cloudflare.com/mcp",
				"name": "cloudflare-docs"
			}
		],
		"context_management": {
			"edits": [
				{
					"type": "clear_tool_uses_20250919"
				}
			]
		}
	}`, string(body))
}

func TestConvertClaudeRequestOmitsRESTContextManagementAndPreservesBYOK(t *testing.T) {
	contextManagement := json.RawMessage(`{"edits":[{"type":"clear_tool_uses_20250919"}]}`)

	restRequest := &dto.ClaudeRequest{
		Model:             "anthropic/claude-opus-5",
		Messages:          []dto.ClaudeMessage{{Role: "user", Content: "question"}},
		ContextManagement: contextManagement,
	}
	converted, err := (&Adaptor{}).ConvertClaudeRequest(
		nil,
		cloudflareRelayInfo(dto.CloudflareAPIModeREST),
		restRequest,
	)

	require.NoError(t, err)
	assert.Same(t, restRequest, converted)
	assert.Nil(t, restRequest.ContextManagement)
	body, err := common.Marshal(converted)
	require.NoError(t, err)
	assert.NotContains(t, string(body), `"context_management"`)

	byokRequest := &dto.ClaudeRequest{
		Model:             "anthropic/claude-opus-5",
		Messages:          []dto.ClaudeMessage{{Role: "user", Content: "question"}},
		ContextManagement: contextManagement,
	}
	converted, err = (&Adaptor{}).ConvertClaudeRequest(
		nil,
		cloudflareRelayInfo(dto.CloudflareAPIModeBYOK),
		byokRequest,
	)

	require.NoError(t, err)
	assert.Same(t, byokRequest, converted)
	assert.JSONEq(t, string(contextManagement), string(byokRequest.ContextManagement))
	body, err = common.Marshal(converted)
	require.NoError(t, err)
	assert.Contains(t, string(body), `"context_management"`)
}

func TestConvertClaudeRequestRejectsUnrepresentableRESTSystemBlock(t *testing.T) {
	adaptor := &Adaptor{}
	request := &dto.ClaudeRequest{
		Model: "claude-opus-5",
		System: []map[string]any{
			{"type": "image", "source": map[string]any{"type": "url", "url": "https://example.com/image.png"}},
		},
	}

	converted, err := adaptor.ConvertClaudeRequest(nil, cloudflareRelayInfo(dto.CloudflareAPIModeREST), request)

	require.Error(t, err)
	assert.Nil(t, converted)
	var newAPIError *types.NewAPIError
	require.ErrorAs(t, err, &newAPIError)
	assert.Equal(t, http.StatusBadRequest, newAPIError.StatusCode)
	assert.Contains(t, err.Error(), "system")
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
			name:        "BYOK Anthropic uses Cloudflare gateway token",
			mode:        dto.CloudflareAPIModeBYOK,
			relayFormat: types.RelayFormatClaude,
			check: func(t *testing.T, header http.Header) {
				assert.Empty(t, header.Get("Authorization"))
				assert.Empty(t, header.Get("x-api-key"))
				assert.Equal(t, "Bearer channel-key", header.Get("cf-aig-authorization"))
				assert.Equal(t, "2024-01-01", header.Get("anthropic-version"))
				assert.Equal(t, "mcp-client-2025-11-20", header.Get("anthropic-beta"))
			},
		},
		{
			name: "BYOK compatibility endpoint uses Cloudflare gateway token",
			mode: dto.CloudflareAPIModeBYOK,
			check: func(t *testing.T, header http.Header) {
				assert.Empty(t, header.Get("Authorization"))
				assert.Empty(t, header.Get("x-api-key"))
				assert.Equal(t, "Bearer channel-key", header.Get("cf-aig-authorization"))
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
			c.Request.Header.Set("anthropic-beta", "mcp-client-2025-11-20")
			header := http.Header{}
			info := cloudflareRelayInfo(test.mode)
			info.RelayFormat = test.relayFormat

			err := (&Adaptor{}).SetupRequestHeader(c, &header, info)

			require.NoError(t, err)
			assert.Equal(t, "3600", header.Get("cf-aig-cache-ttl"))
			test.check(t, header)
		})
	}
}

func TestBYOKAnthropicRequestMatchesProviderNativeGatewayProtocol(t *testing.T) {
	service.InitHttpClient()

	type capturedRequest struct {
		path   string
		header http.Header
		body   []byte
		err    error
	}
	captured := make(chan capturedRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		captured <- capturedRequest{
			path:   r.URL.Path,
			header: r.Header.Clone(),
			body:   body,
			err:    err,
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	t.Cleanup(server.Close)

	maxTokens := uint(1024)
	request := &dto.ClaudeRequest{
		Model:     "anthropic/claude-opus-5",
		MaxTokens: &maxTokens,
		Messages: []dto.ClaudeMessage{
			{Role: "user", Content: "print 1"},
		},
	}
	info := cloudflareRelayInfo(dto.CloudflareAPIModeBYOK)
	info.ChannelBaseUrl = server.URL
	info.RelayFormat = types.RelayFormatClaude

	converted, err := (&Adaptor{}).ConvertClaudeRequest(nil, info, request)
	require.NoError(t, err)
	body, err := common.Marshal(converted)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(string(body)))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("cf-aig-authorization", "Bearer untrusted-client-token")
	c.Request.Header.Set("cf-aig-metadata", `{"user_id":"test-user"}`)

	response, err := (&Adaptor{}).DoRequest(c, info, strings.NewReader(string(body)))
	require.NoError(t, err)
	httpResponse, ok := response.(*http.Response)
	require.True(t, ok)
	require.NoError(t, httpResponse.Body.Close())

	got := <-captured
	require.NoError(t, got.err)
	assert.Equal(t, "/v1/account-id/gateway-id/anthropic/v1/messages", got.path)
	assert.Equal(t, "Bearer channel-key", got.header.Get("cf-aig-authorization"))
	assert.JSONEq(t, `{"user_id":"test-user"}`, got.header.Get("cf-aig-metadata"))
	assert.Equal(t, "2023-06-01", got.header.Get("anthropic-version"))
	assert.Empty(t, got.header.Get("Authorization"))
	assert.Empty(t, got.header.Get("x-api-key"))
	assert.JSONEq(t, `{
		"model": "claude-opus-5",
		"max_tokens": 1024,
		"messages": [
			{"role": "user", "content": "print 1"}
		]
	}`, string(got.body))
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
