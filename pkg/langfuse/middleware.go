package langfuse

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

type captureWriter struct {
	gin.ResponseWriter
	body bytes.Buffer
}

func (w *captureWriter) Write(data []byte) (int, error) {
	w.body.Write(data)
	return w.ResponseWriter.Write(data)
}

func (w *captureWriter) WriteString(data string) (int, error) {
	w.body.WriteString(data)
	return w.ResponseWriter.WriteString(data)
}

func Middleware(client *Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		if client == nil || c.Request.Method != http.MethodPost || c.Request.URL.Path != "/v1/messages" {
			c.Next()
			return
		}
		storage, err := common.GetBodyStorage(c)
		if err != nil {
			c.Next()
			return
		}
		requestBody, err := storage.Bytes()
		if err != nil {
			c.Next()
			return
		}
		var request dto.ClaudeRequest
		if err := common.Unmarshal(requestBody, &request); err != nil {
			c.Next()
			return
		}
		input := service.ApplyClaudeAdaptiveThinkingJSON(
			append([]byte(nil), requestBody...),
			request.Model,
		)

		writer := &captureWriter{ResponseWriter: c.Writer}
		c.Writer = writer
		startedAt := time.Now()
		c.Next()

		if writer.Status() < http.StatusOK || writer.Status() >= http.StatusMultipleChoices || writer.body.Len() == 0 {
			return
		}
		outputBytes := append([]byte(nil), writer.body.Bytes()...)
		stream := strings.HasPrefix(writer.Header().Get("Content-Type"), "text/event-stream")
		usage := usageFromResponse(outputBytes, stream)
		var output any = json.RawMessage(outputBytes)
		if stream {
			output = string(outputBytes)
		}
		traceID := strings.TrimSpace(c.GetHeader("X-Request-Id"))
		if traceID == "" {
			traceID = c.GetString(common.RequestIdKey)
		}
		userID := strings.TrimSpace(c.GetHeader("X-External-User-Id"))
		if userID == "" {
			if id := c.GetInt("id"); id != 0 {
				userID = strconv.Itoa(id)
			}
		}
		apiKeyID := strings.TrimSpace(c.GetHeader("X-External-Api-Key-Id"))
		if apiKeyID == "" {
			if id := c.GetInt("token_id"); id != 0 {
				apiKeyID = strconv.Itoa(id)
			}
		}
		apiKeyName := strings.TrimSpace(c.GetHeader("X-External-Api-Key-Name"))
		if apiKeyName == "" {
			apiKeyName = c.GetString("token_name")
		}
		metadata := make(map[string]any)
		modelParameters := make(map[string]any)
		thinkingEffort := request.GetEfforts()
		if service.IsClaudeAdaptiveThinkingModel(request.Model) {
			thinkingEffort = "max"
		}
		if thinkingEffort != "" {
			metadata["thinking_effort"] = thinkingEffort
			modelParameters["thinking_effort"] = thinkingEffort
		}
		client.TraceGeneration(GenerationRequest{
			TraceID:         traceID,
			Name:            "anthropic-messages",
			Model:           request.Model,
			StartTime:       startedAt,
			EndTime:         time.Now(),
			Input:           json.RawMessage(input),
			Output:          output,
			Usage:           usage,
			UserID:          userID,
			UserName:        common.GetContextKeyString(c, constant.ContextKeyUserName),
			ApiKeyID:        apiKeyID,
			ApiKeyName:      apiKeyName,
			Route:           c.Request.Method + " " + c.Request.URL.Path,
			SessionID:       sessionID(request.Metadata),
			ModelParameters: modelParameters,
			Metadata:        metadata,
		})
	}
}

func usageFromResponse(body []byte, stream bool) *Usage {
	var inputTokens int64
	var outputTokens int64
	var cacheCreationTokens int64
	var cacheReadTokens int64
	if stream {
		for _, line := range bytes.Split(body, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			data := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			var response dto.ClaudeResponse
			if common.Unmarshal(data, &response) != nil {
				continue
			}
			if response.Message != nil && response.Message.Usage != nil {
				inputTokens = int64(response.Message.Usage.InputTokens)
				outputTokens = int64(response.Message.Usage.OutputTokens)
				cacheCreationTokens = int64(response.Message.Usage.CacheCreationInputTokens)
				cacheReadTokens = int64(response.Message.Usage.CacheReadInputTokens)
			}
			if response.Usage != nil {
				if response.Usage.InputTokens != 0 {
					inputTokens = int64(response.Usage.InputTokens)
				}
				if response.Usage.OutputTokens != 0 {
					outputTokens = int64(response.Usage.OutputTokens)
				}
				if response.Usage.CacheCreationInputTokens != 0 {
					cacheCreationTokens = int64(response.Usage.CacheCreationInputTokens)
				}
				if response.Usage.CacheReadInputTokens != 0 {
					cacheReadTokens = int64(response.Usage.CacheReadInputTokens)
				}
			}
		}
	} else {
		var response dto.ClaudeResponse
		if common.Unmarshal(body, &response) == nil && response.Usage != nil {
			inputTokens = int64(response.Usage.InputTokens)
			outputTokens = int64(response.Usage.OutputTokens)
			cacheCreationTokens = int64(response.Usage.CacheCreationInputTokens)
			cacheReadTokens = int64(response.Usage.CacheReadInputTokens)
		}
	}
	inputTokens += cacheCreationTokens + cacheReadTokens
	return &Usage{Input: inputTokens, Output: outputTokens, Total: inputTokens + outputTokens}
}

func sessionID(metadata json.RawMessage) string {
	if len(metadata) == 0 {
		return ""
	}
	var values map[string]json.RawMessage
	if common.Unmarshal(metadata, &values) != nil {
		return ""
	}
	var direct string
	if raw, ok := values["session_id"]; ok && common.Unmarshal(raw, &direct) == nil {
		return direct
	}
	var userID string
	if raw, ok := values["user_id"]; !ok || common.Unmarshal(raw, &userID) != nil {
		return ""
	}
	var nested map[string]json.RawMessage
	if common.UnmarshalJsonStr(userID, &nested) != nil {
		return ""
	}
	if raw, ok := nested["session_id"]; ok {
		_ = common.Unmarshal(raw, &direct)
	}
	return direct
}
