package controller

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedOfficialRatioPresetIsValid(t *testing.T) {
	parsed, err := parseOfficialRatioPreset(officialRatioPresetFallback)
	require.NoError(t, err)
	assert.NotEmpty(t, parsed["model_ratio"])
}

func TestFetchUpstreamRatiosFallsBackToEmbeddedOfficialPreset(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer remote.Close()

	requestBody, err := common.Marshal(dto.UpstreamRequest{
		Upstreams: []dto.UpstreamDTO{{
			ID:       officialRatioPresetID,
			Name:     officialRatioPresetName,
			BaseURL:  remote.URL,
			Endpoint: "/ratio-config.json",
		}},
		Timeout: 1,
	})
	require.NoError(t, err)

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodPost, "/api/ratio_sync/fetch", bytes.NewReader(requestBody))
	context.Request.Header.Set("Content-Type", "application/json")

	FetchUpstreamRatios(context)
	require.Equal(t, http.StatusOK, recorder.Code)

	var response struct {
		Success bool `json:"success"`
		Data    struct {
			TestResults []dto.TestResult `json:"test_results"`
		} `json:"data"`
	}
	err = common.DecodeJson(recorder.Body, &response)
	require.NoError(t, err)
	assert.True(t, response.Success)
	require.Len(t, response.Data.TestResults, 1)
	assert.Equal(t, "success", response.Data.TestResults[0].Status)
}

func TestParseOfficialRatioPresetRejectsUnsupportedData(t *testing.T) {
	_, err := parseOfficialRatioPreset([]byte(`{"success":true,"data":{"unknown":{}}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no supported pricing fields")
}
