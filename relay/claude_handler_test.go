package relay

import (
	"encoding/json"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyClaudeAdaptiveThinkingForcesRequestParameters(t *testing.T) {
	request := &dto.ClaudeRequest{
		Model:        "claude-opus-5",
		Temperature:  common.GetPointer(0.2),
		TopP:         common.GetPointer(0.8),
		TopK:         common.GetPointer(20),
		OutputConfig: json.RawMessage(`{"format":{"type":"json_schema"},"effort":"low"}`),
		Thinking: &dto.Thinking{
			Type:         "enabled",
			BudgetTokens: common.GetPointer(2048),
			Display:      "summarized",
		},
	}

	applied := applyClaudeAdaptiveThinking(request, request.Model)

	assert.True(t, applied)
	require.NotNil(t, request.Thinking)
	assert.Equal(t, "adaptive", request.Thinking.Type)
	assert.Equal(t, "summarized", request.Thinking.Display)
	assert.Nil(t, request.Thinking.BudgetTokens)
	assert.JSONEq(t, `{"format":{"type":"json_schema"},"effort":"max"}`, string(request.OutputConfig))
	assert.Nil(t, request.Temperature)
	assert.Nil(t, request.TopP)
	assert.Nil(t, request.TopK)
}

func TestApplyClaudeAdaptiveThinkingLeavesOtherModelsUnchanged(t *testing.T) {
	request := &dto.ClaudeRequest{
		Model:       "claude-sonnet-4",
		Temperature: common.GetPointer(0.2),
		Thinking:    &dto.Thinking{Type: "enabled", BudgetTokens: common.GetPointer(2048)},
	}
	before, err := common.Marshal(request)
	require.NoError(t, err)

	applied := applyClaudeAdaptiveThinking(request, request.Model)

	after, err := common.Marshal(request)
	require.NoError(t, err)
	assert.False(t, applied)
	assert.JSONEq(t, string(before), string(after))
	assert.False(t, service.IsClaudeAdaptiveThinkingModel(request.Model))
}
