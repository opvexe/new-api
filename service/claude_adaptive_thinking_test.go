package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApplyClaudeAdaptiveThinkingJSONForSupportedModels(t *testing.T) {
	for _, model := range []string{"claude-fable-5", "claude-opus-5"} {
		t.Run(model, func(t *testing.T) {
			rawJSON := []byte(`{
				"model":"` + model + `",
				"thinking":{"type":"enabled","budget_tokens":2048,"display":"summarized"},
				"output_config":{"format":{"type":"json_schema"},"effort":"low"},
				"temperature":0.2,
				"top_p":0.8,
				"top_k":20,
				"messages":[{"role":"user","content":"hello"}]
			}`)

			got := ApplyClaudeAdaptiveThinkingJSON(rawJSON, model)

			assert.JSONEq(t, `{
				"model":"`+model+`",
				"thinking":{"type":"adaptive","display":"summarized"},
				"output_config":{"format":{"type":"json_schema"},"effort":"max"},
				"messages":[{"role":"user","content":"hello"}]
			}`, string(got))
		})
	}
}

func TestApplyClaudeAdaptiveThinkingJSONLeavesOtherModelsUnchanged(t *testing.T) {
	rawJSON := []byte(`{
		"model":"claude-sonnet-4",
		"thinking":{"type":"enabled","budget_tokens":2048},
		"temperature":0.2,
		"messages":[{"role":"user","content":"hello"}]
	}`)

	got := ApplyClaudeAdaptiveThinkingJSON(rawJSON, "claude-sonnet-4")

	assert.Equal(t, string(rawJSON), string(got))
}
