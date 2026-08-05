package service

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func IsClaudeAdaptiveThinkingModel(model string) bool {
	model = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(model)), "anthropic/")
	return model == "claude-fable-5" || model == "claude-opus-5"
}

// ApplyClaudeAdaptiveThinkingJSON is retained for callers that explicitly opt
// into adaptive thinking. Relay paths do not apply it automatically.
func ApplyClaudeAdaptiveThinkingJSON(rawJSON []byte, model string) []byte {
	if !IsClaudeAdaptiveThinkingModel(model) {
		return rawJSON
	}
	thinking := map[string]any{"type": "adaptive"}
	if display := gjson.GetBytes(rawJSON, "thinking.display"); display.Exists() {
		thinking["display"] = display.String()
	}
	out, err := sjson.SetBytes(rawJSON, "thinking", thinking)
	if err != nil {
		return rawJSON
	}
	if out, err = sjson.SetBytes(out, "output_config.effort", "max"); err != nil {
		return rawJSON
	}
	for _, key := range []string{"temperature", "top_p", "top_k"} {
		if out, err = sjson.DeleteBytes(out, key); err != nil {
			return rawJSON
		}
	}
	return out
}
