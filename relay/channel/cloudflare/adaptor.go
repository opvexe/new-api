package cloudflare

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/relay/channel"
	"github.com/QuantumNous/new-api/relay/channel/claude"
	"github.com/QuantumNous/new-api/relay/channel/openai"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"

	"github.com/gin-gonic/gin"
)

type Adaptor struct {
}

const (
	restAPIBaseURL = "https://api.cloudflare.com"
	byokBaseURL    = "https://gateway.ai.cloudflare.com"
)

var passthroughResponseHeaders = []string{
	"cf-aig-cache-status",
	"cf-aig-cache-ttl",
	"cf-aig-event-id",
	"cf-aig-log-id",
	"cf-aig-request-id",
	"cf-ray",
}

func (a *Adaptor) ConvertGeminiRequest(*gin.Context, *relaycommon.RelayInfo, *dto.GeminiChatRequest) (any, error) {
	//TODO implement me
	return nil, errors.New("not implemented")
}

func (a *Adaptor) ConvertClaudeRequest(_ *gin.Context, info *relaycommon.RelayInfo, request *dto.ClaudeRequest) (any, error) {
	if request == nil {
		return nil, errors.New("request is nil")
	}
	if isBYOKMode(info) {
		const anthropicPrefix = "anthropic/"
		if strings.HasPrefix(strings.ToLower(request.Model), anthropicPrefix) {
			request.Model = request.Model[len(anthropicPrefix):]
		}
	} else {
		request.Model = qualifyModel(request.Model, false)
		if err := normalizeClaudeRequestForREST(request); err != nil {
			return nil, types.NewErrorWithStatusCode(
				fmt.Errorf("invalid Cloudflare REST Claude request: %w", err),
				types.ErrorCodeBadRequestBody,
				http.StatusBadRequest,
				types.ErrOptionWithSkipRetry(),
			)
		}
	}
	return request, nil
}

func normalizeClaudeRequestForREST(request *dto.ClaudeRequest) error {
	// Cloudflare's REST Messages endpoint rejects this Anthropic beta field,
	// including when the corresponding anthropic-beta header is present.
	request.ContextManagement = nil

	systemParts := make([]string, 0, 1)
	if request.System != nil {
		systemText, err := claudeSystemTextForREST(request.System)
		if err != nil {
			return fmt.Errorf("invalid system: %w", err)
		}
		request.System = systemText
		if systemText != "" {
			systemParts = append(systemParts, systemText)
		}
	}

	messages := make([]dto.ClaudeMessage, 0, len(request.Messages))
	for i, message := range request.Messages {
		if message.Role != "system" && message.Role != "developer" {
			messages = append(messages, message)
			continue
		}

		systemText, err := claudeSystemTextForREST(message.Content)
		if err != nil {
			return fmt.Errorf("invalid messages[%d] %s content: %w", i, message.Role, err)
		}
		if systemText != "" {
			systemParts = append(systemParts, systemText)
		}
	}
	request.Messages = messages

	if len(systemParts) > 0 {
		request.System = strings.Join(systemParts, "\n")
	}
	return nil
}

func claudeSystemTextForREST(content any) (string, error) {
	if content == nil {
		return "", nil
	}
	if text, ok := content.(string); ok {
		return text, nil
	}

	blocks, err := common.Any2Type[[]dto.ClaudeMediaMessage](content)
	if err != nil {
		return "", errors.New("expected a string or an array of text blocks")
	}

	texts := make([]string, 0, len(blocks))
	for i, block := range blocks {
		if block.Type != dto.ContentTypeText || block.Text == nil {
			return "", fmt.Errorf("block %d must be a text block", i)
		}
		texts = append(texts, block.GetText())
	}
	return strings.Join(texts, "\n"), nil
}

func (a *Adaptor) Init(info *relaycommon.RelayInfo) {
}

func (a *Adaptor) GetRequestURL(info *relaycommon.RelayInfo) (string, error) {
	if info == nil {
		return "", errors.New("relay info is nil")
	}
	byok := isBYOKMode(info)
	if err := channel.ValidateCloudflareTarget(info.ApiVersion, byok); err != nil {
		return "", fmt.Errorf("cloudflare Other %w", err)
	}

	baseURL := strings.TrimRight(info.ChannelBaseUrl, "/")
	if byok {
		if baseURL == "" || baseURL == restAPIBaseURL {
			baseURL = byokBaseURL
		}
		if info.RelayFormat == types.RelayFormatClaude {
			return fmt.Sprintf("%s/v1/%s/anthropic/v1/messages", baseURL, info.ApiVersion), nil
		}
		switch info.RelayMode {
		case constant.RelayModeChatCompletions:
			return fmt.Sprintf("%s/v1/%s/compat/chat/completions", baseURL, info.ApiVersion), nil
		case constant.RelayModeEmbeddings:
			return fmt.Sprintf("%s/v1/%s/compat/embeddings", baseURL, info.ApiVersion), nil
		case constant.RelayModeResponses:
			return fmt.Sprintf("%s/v1/%s/compat/responses", baseURL, info.ApiVersion), nil
		default:
			return fmt.Sprintf("%s/v1/%s/compat%s", baseURL, info.ApiVersion, info.RequestURLPath), nil
		}
	}

	if baseURL == "" || baseURL == byokBaseURL {
		baseURL = restAPIBaseURL
	}
	if info.RelayFormat == types.RelayFormatClaude {
		return fmt.Sprintf("%s/client/v4/accounts/%s/ai/v1/messages", baseURL, info.ApiVersion), nil
	}
	switch info.RelayMode {
	case constant.RelayModeChatCompletions:
		return fmt.Sprintf("%s/client/v4/accounts/%s/ai/v1/chat/completions", baseURL, info.ApiVersion), nil
	case constant.RelayModeEmbeddings:
		return fmt.Sprintf("%s/client/v4/accounts/%s/ai/v1/embeddings", baseURL, info.ApiVersion), nil
	case constant.RelayModeResponses:
		return fmt.Sprintf("%s/client/v4/accounts/%s/ai/v1/responses", baseURL, info.ApiVersion), nil
	default:
		return fmt.Sprintf("%s/client/v4/accounts/%s/ai/run/%s", baseURL, info.ApiVersion, info.UpstreamModelName), nil
	}
}

func (a *Adaptor) SetupRequestHeader(c *gin.Context, req *http.Header, info *relaycommon.RelayInfo) error {
	channel.SetupApiRequestHeader(info, c, req)
	req.Del("x-api-key")
	req.Del("cf-aig-authorization")
	req.Del("Authorization")

	for name, values := range c.Request.Header {
		lowerName := strings.ToLower(name)
		if !strings.HasPrefix(lowerName, "cf-aig-") || lowerName == "cf-aig-authorization" {
			continue
		}
		req.Del(name)
		for _, value := range values {
			req.Add(name, value)
		}
	}

	if isBYOKMode(info) {
		req.Set("cf-aig-authorization", fmt.Sprintf("Bearer %s", info.ApiKey))
		if info.RelayFormat == types.RelayFormatClaude {
			anthropicVersion := c.Request.Header.Get("anthropic-version")
			if anthropicVersion == "" {
				anthropicVersion = "2023-06-01"
			}
			req.Set("anthropic-version", anthropicVersion)
			if anthropicBeta := c.Request.Header.Get("anthropic-beta"); anthropicBeta != "" {
				req.Set("anthropic-beta", anthropicBeta)
			}
		}
		return nil
	}

	req.Set("Authorization", fmt.Sprintf("Bearer %s", info.ApiKey))
	return nil
}

func (a *Adaptor) ConvertOpenAIRequest(c *gin.Context, info *relaycommon.RelayInfo, request *dto.GeneralOpenAIRequest) (any, error) {
	if request == nil {
		return nil, errors.New("request is nil")
	}
	switch info.RelayMode {
	case constant.RelayModeCompletions:
		return convertCf2CompletionsRequest(*request), nil
	default:
		request.Model = qualifyModel(request.Model, isBYOKMode(info))
		return request, nil
	}
}

func (a *Adaptor) ConvertOpenAIResponsesRequest(c *gin.Context, info *relaycommon.RelayInfo, request dto.OpenAIResponsesRequest) (any, error) {
	request.Model = qualifyModel(request.Model, isBYOKMode(info))
	return request, nil
}

func (a *Adaptor) DoRequest(c *gin.Context, info *relaycommon.RelayInfo, requestBody io.Reader) (any, error) {
	return channel.DoApiRequest(a, c, info, requestBody)
}

func (a *Adaptor) ConvertRerankRequest(c *gin.Context, relayMode int, request dto.RerankRequest) (any, error) {
	return request, nil
}

func (a *Adaptor) ConvertEmbeddingRequest(c *gin.Context, info *relaycommon.RelayInfo, request dto.EmbeddingRequest) (any, error) {
	request.Model = qualifyModel(request.Model, isBYOKMode(info))
	return request, nil
}

func (a *Adaptor) ConvertAudioRequest(c *gin.Context, info *relaycommon.RelayInfo, request dto.AudioRequest) (io.Reader, error) {
	file, _, err := c.Request.FormFile("file")
	if err != nil {
		return nil, errors.New("file is required")
	}
	defer file.Close()
	requestBody := &bytes.Buffer{}
	if _, err := io.Copy(requestBody, file); err != nil {
		return nil, err
	}
	return requestBody, nil
}

func (a *Adaptor) ConvertImageRequest(c *gin.Context, info *relaycommon.RelayInfo, request dto.ImageRequest) (any, error) {
	//TODO implement me
	return nil, errors.New("not implemented")
}

func (a *Adaptor) DoResponse(c *gin.Context, resp *http.Response, info *relaycommon.RelayInfo) (usage any, err *types.NewAPIError) {
	a.passthroughResponseHeaders(c, resp)
	if info.RelayFormat == types.RelayFormatClaude {
		info.FinalRequestRelayFormat = types.RelayFormatClaude
		if info.IsStream {
			return claude.ClaudeStreamHandler(c, resp, info)
		}
		return claude.ClaudeHandler(c, resp, info)
	}

	switch info.RelayMode {
	case constant.RelayModeEmbeddings:
		err, usage = cfEmbeddingHandler(c, info, resp)
	case constant.RelayModeChatCompletions:
		if info.IsStream {
			return openai.OaiStreamHandler(c, info, resp)
		}
		err, usage = cfChatHandler(c, info, resp)
	case constant.RelayModeResponses:
		if info.IsStream {
			usage, err = openai.OaiResponsesStreamHandler(c, info, resp)
		} else {
			usage, err = openai.OaiResponsesHandler(c, info, resp)
		}
	case constant.RelayModeAudioTranslation:
		fallthrough
	case constant.RelayModeAudioTranscription:
		err, usage = cfSTTHandler(c, info, resp)
	}
	return
}

func (a *Adaptor) GetModelList() []string {
	return ModelList
}

func (a *Adaptor) GetChannelName() string {
	return ChannelName
}

func isBYOKMode(info *relaycommon.RelayInfo) bool {
	return info != nil && info.ChannelMeta != nil &&
		info.ChannelOtherSettings.CloudflareAPIMode == dto.CloudflareAPIModeBYOK
}

func qualifyModel(model string, byok bool) string {
	if model == "" {
		return model
	}
	lowerModel := strings.ToLower(model)
	if strings.HasPrefix(lowerModel, "@cf/") {
		if byok {
			return "workers-ai/" + model
		}
		return model
	}
	if strings.Contains(model, "/") {
		return model
	}
	switch {
	case strings.HasPrefix(lowerModel, "claude"):
		return "anthropic/" + model
	case strings.HasPrefix(lowerModel, "gemini"):
		if byok {
			return "google-ai-studio/" + model
		}
		return "google/" + model
	case strings.HasPrefix(lowerModel, "grok"):
		if byok {
			return "grok/" + model
		}
		return "xai/" + model
	case strings.HasPrefix(lowerModel, "deepseek"):
		return "deepseek/" + model
	case strings.HasPrefix(lowerModel, "command"):
		return "cohere/" + model
	case strings.HasPrefix(lowerModel, "mistral"), strings.HasPrefix(lowerModel, "codestral"):
		return "mistral/" + model
	default:
		return "openai/" + model
	}
}

func (a *Adaptor) passthroughResponseHeaders(c *gin.Context, resp *http.Response) {
	if c == nil || c.Writer == nil || resp == nil {
		return
	}
	knownHeaders := make(map[string]struct{}, len(passthroughResponseHeaders))
	for _, name := range passthroughResponseHeaders {
		knownHeaders[name] = struct{}{}
		for _, value := range resp.Header.Values(name) {
			c.Writer.Header().Add(name, value)
		}
	}
	for name, values := range resp.Header {
		lowerName := strings.ToLower(name)
		if !strings.HasPrefix(lowerName, "cf-aig-") || lowerName == "cf-aig-authorization" {
			continue
		}
		if _, ok := knownHeaders[lowerName]; ok {
			continue
		}
		for _, value := range values {
			c.Writer.Header().Add(name, value)
		}
	}
}
