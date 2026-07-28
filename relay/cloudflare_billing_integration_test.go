package relay

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/model_setting"
	"github.com/QuantumNous/new-api/types"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const (
	cloudflareBillingInitialQuota = 10_000
	cloudflareBillingAPIKey       = "cloudflare-integration-key"
	cloudflareBillingAccount      = "integration-account"
)

type cloudflareBillingFixture struct {
	db *gorm.DB
}

type cloudflareUpstreamRequest struct {
	path          string
	authorization string
	model         string
	decodeErr     error
}

func setupCloudflareBillingFixture(t *testing.T) *cloudflareBillingFixture {
	t.Helper()

	previousDB := model.DB
	previousLogDB := model.LOG_DB
	previousMainDBType := common.MainDatabaseType()
	previousLogDBType := common.LogDatabaseType()
	previousRedisEnabled := common.RedisEnabled
	previousRedisClient := common.RDB
	previousBatchUpdateEnabled := common.BatchUpdateEnabled
	previousLogConsumeEnabled := common.LogConsumeEnabled
	previousDataExportEnabled := common.DataExportEnabled
	globalSettings := model_setting.GetGlobalSettings()
	previousPassThroughEnabled := globalSettings.PassThroughRequestEnabled
	previousResponsesPolicy := globalSettings.ChatCompletionsToResponsesPolicy

	db, err := gorm.Open(sqlite.Open("file:cloudflare-billing-integration?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.AutoMigrate(
		&model.User{},
		&model.Token{},
		&model.Channel{},
		&model.Log{},
	))

	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})

	model.DB = db
	model.LOG_DB = db
	common.SetDatabaseTypes(common.DatabaseTypeSQLite, common.DatabaseTypeSQLite)
	common.RedisEnabled = false
	common.RDB = redisClient
	common.BatchUpdateEnabled = false
	common.LogConsumeEnabled = true
	common.DataExportEnabled = false
	globalSettings.PassThroughRequestEnabled = false
	globalSettings.ChatCompletionsToResponsesPolicy.Enabled = false
	service.InitHttpClient()

	t.Cleanup(func() {
		model.DB = previousDB
		model.LOG_DB = previousLogDB
		common.SetDatabaseTypes(previousMainDBType, previousLogDBType)
		common.RedisEnabled = previousRedisEnabled
		common.RDB = previousRedisClient
		common.BatchUpdateEnabled = previousBatchUpdateEnabled
		common.LogConsumeEnabled = previousLogConsumeEnabled
		common.DataExportEnabled = previousDataExportEnabled
		globalSettings.PassThroughRequestEnabled = previousPassThroughEnabled
		globalSettings.ChatCompletionsToResponsesPolicy = previousResponsesPolicy
		_ = redisClient.Close()
		_ = sqlDB.Close()
	})

	return &cloudflareBillingFixture{db: db}
}

func (f *cloudflareBillingFixture) seed(t *testing.T, userID, tokenID, channelID int) {
	t.Helper()

	require.NoError(t, f.db.Create(&model.User{
		Id:       userID,
		Username: fmt.Sprintf("cloudflare-integration-%d", userID),
		Password: "unused-password",
		Status:   common.UserStatusEnabled,
		Group:    "default",
		Quota:    cloudflareBillingInitialQuota,
		AffCode:  fmt.Sprintf("cf-%d", userID),
	}).Error)
	require.NoError(t, f.db.Create(&model.Token{
		Id:          tokenID,
		UserId:      userID,
		Key:         fmt.Sprintf("cloudflare-token-%d", tokenID),
		Name:        "cloudflare-integration-token",
		Status:      common.TokenStatusEnabled,
		RemainQuota: cloudflareBillingInitialQuota,
	}).Error)
	require.NoError(t, f.db.Create(&model.Channel{
		Id:     channelID,
		Type:   constant.ChannelCloudflare,
		Key:    cloudflareBillingAPIKey,
		Name:   "cloudflare-integration-channel",
		Status: common.ChannelStatusEnabled,
		Group:  "default",
	}).Error)
}

func newCloudflareUpstream(t *testing.T, responseBody string) (*httptest.Server, <-chan cloudflareUpstreamRequest) {
	t.Helper()

	requests := make(chan cloudflareUpstreamRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Model string `json:"model"`
		}
		decodeErr := common.DecodeJson(r.Body, &payload)
		requests <- cloudflareUpstreamRequest{
			path:          r.URL.Path,
			authorization: r.Header.Get("Authorization"),
			model:         payload.Model,
			decodeErr:     decodeErr,
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("cf-aig-cache-status", "MISS")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, responseBody)
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func newCloudflareRelayContext(
	t *testing.T,
	path string,
	clientBody string,
	upstreamBaseURL string,
	modelName string,
	requestID string,
	estimatedPromptTokens int,
	userID int,
	tokenID int,
	channelID int,
	tokenKey string,
) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()

	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(clientBody))
	context.Request.Header.Set("Content-Type", "application/json")

	common.SetContextKey(context, constant.ContextKeyRequestStartTime, time.Now())
	common.SetContextKey(context, constant.ContextKeyEstimatedTokens, estimatedPromptTokens)
	common.SetContextKey(context, constant.ContextKeyOriginalModel, modelName)
	common.SetContextKey(context, constant.ContextKeyUserId, userID)
	common.SetContextKey(context, constant.ContextKeyUserName, "cloudflare-integration-user")
	common.SetContextKey(context, constant.ContextKeyUserEmail, "integration@example.com")
	common.SetContextKey(context, constant.ContextKeyUserGroup, "default")
	common.SetContextKey(context, constant.ContextKeyUsingGroup, "default")
	common.SetContextKey(context, constant.ContextKeyUserQuota, cloudflareBillingInitialQuota)
	common.SetContextKey(context, constant.ContextKeyTokenId, tokenID)
	common.SetContextKey(context, constant.ContextKeyTokenKey, tokenKey)
	common.SetContextKey(context, constant.ContextKeyTokenGroup, "default")
	common.SetContextKey(context, constant.ContextKeyTokenUnlimited, false)
	common.SetContextKey(context, constant.ContextKeyChannelId, channelID)
	common.SetContextKey(context, constant.ContextKeyChannelName, "cloudflare-integration-channel")
	common.SetContextKey(context, constant.ContextKeyChannelType, constant.ChannelCloudflare)
	common.SetContextKey(context, constant.ContextKeyChannelBaseUrl, upstreamBaseURL)
	common.SetContextKey(context, constant.ContextKeyChannelKey, cloudflareBillingAPIKey)
	common.SetContextKey(context, constant.ContextKeyChannelSetting, dto.ChannelSettings{})
	common.SetContextKey(context, constant.ContextKeyChannelOtherSetting, dto.ChannelOtherSettings{
		CloudflareAPIMode: dto.CloudflareAPIModeREST,
	})
	context.Set("api_version", cloudflareBillingAccount)
	context.Set("token_name", "cloudflare-integration-token")
	context.Set(common.RequestIdKey, requestID)

	return context, recorder
}

func cloudflareBillingPriceData() types.PriceData {
	return types.PriceData{
		ModelRatio:           1,
		CompletionRatio:      2,
		CacheRatio:           1,
		CacheCreationRatio:   1,
		CacheCreation5mRatio: 1,
		CacheCreation1hRatio: 1,
		ImageRatio:           1,
		AudioRatio:           1,
		AudioCompletionRatio: 1,
		GroupRatioInfo:       types.GroupRatioInfo{GroupRatio: 1},
		QuotaToPreConsume:    0,
	}
}

func (f *cloudflareBillingFixture) assertBilling(
	t *testing.T,
	userID int,
	tokenID int,
	channelID int,
	requestID string,
	expectedPromptTokens int,
	expectedCompletionTokens int,
	expectedQuota int,
) {
	t.Helper()

	var user model.User
	require.NoError(t, f.db.First(&user, userID).Error)
	assert.Equal(t, cloudflareBillingInitialQuota-expectedQuota, user.Quota)
	assert.Equal(t, expectedQuota, user.UsedQuota)
	assert.Equal(t, 1, user.RequestCount)

	var token model.Token
	require.NoError(t, f.db.First(&token, tokenID).Error)
	assert.Equal(t, cloudflareBillingInitialQuota-expectedQuota, token.RemainQuota)
	assert.Equal(t, expectedQuota, token.UsedQuota)

	var channel model.Channel
	require.NoError(t, f.db.First(&channel, channelID).Error)
	assert.Equal(t, int64(expectedQuota), channel.UsedQuota)

	var logs []model.Log
	require.NoError(t, f.db.Where("request_id = ? AND type = ?", requestID, model.LogTypeConsume).Find(&logs).Error)
	require.Len(t, logs, 1)
	assert.Equal(t, expectedPromptTokens, logs[0].PromptTokens)
	assert.Equal(t, expectedCompletionTokens, logs[0].CompletionTokens)
	assert.Equal(t, expectedQuota, logs[0].Quota)
	assert.Equal(t, tokenID, logs[0].TokenId)
	assert.Equal(t, channelID, logs[0].ChannelId)
}

func TestCloudflareChatAndEmbeddingsUseUpstreamUsageForBilling(t *testing.T) {
	fixture := setupCloudflareBillingFixture(t)

	t.Run("chat completions", func(t *testing.T) {
		const (
			userID                 = 1101
			tokenID                = 1201
			channelID              = 1301
			requestID              = "cloudflare-chat-billing-request"
			modelName              = "cf-chat-test"
			estimatedPromptTokens  = 2
			actualPromptTokens     = 13
			actualCompletionTokens = 7
			expectedQuota          = actualPromptTokens + actualCompletionTokens*2
			upstreamResponse       = `{"id":"chatcmpl-cloudflare","object":"chat.completion","created":1777777777,"model":"openai/cf-chat-test","choices":[{"index":0,"message":{"role":"assistant","content":"official chat response"},"finish_reason":"stop"}],"usage":{"prompt_tokens":13,"completion_tokens":7,"total_tokens":20},"system_fingerprint":"cloudflare-official"}`
			clientRequestBody      = `{"model":"cf-chat-test","messages":[{"role":"user","content":"use upstream usage"}]}`
		)

		fixture.seed(t, userID, tokenID, channelID)
		var token model.Token
		require.NoError(t, fixture.db.First(&token, tokenID).Error)
		server, upstreamRequests := newCloudflareUpstream(t, upstreamResponse)
		context, recorder := newCloudflareRelayContext(
			t,
			"/v1/chat/completions",
			clientRequestBody,
			server.URL,
			modelName,
			requestID,
			estimatedPromptTokens,
			userID,
			tokenID,
			channelID,
			token.Key,
		)

		request := &dto.GeneralOpenAIRequest{
			Model: modelName,
			Messages: []dto.Message{{
				Role:    "user",
				Content: "use upstream usage",
			}},
		}
		info, err := relaycommon.GenRelayInfo(context, types.RelayFormatOpenAI, request, nil)
		require.NoError(t, err)
		info.PriceData = cloudflareBillingPriceData()

		require.Nil(t, TextHelper(context, info))

		upstreamRequest := <-upstreamRequests
		require.NoError(t, upstreamRequest.decodeErr)
		assert.Equal(t, "/client/v4/accounts/integration-account/ai/v1/chat/completions", upstreamRequest.path)
		assert.Equal(t, "Bearer "+cloudflareBillingAPIKey, upstreamRequest.authorization)
		assert.Equal(t, "openai/"+modelName, upstreamRequest.model)
		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, upstreamResponse, recorder.Body.String())
		assert.Equal(t, "MISS", recorder.Header().Get("cf-aig-cache-status"))

		fixture.assertBilling(
			t,
			userID,
			tokenID,
			channelID,
			requestID,
			actualPromptTokens,
			actualCompletionTokens,
			expectedQuota,
		)
		assert.NotEqual(t, estimatedPromptTokens, actualPromptTokens)
	})

	t.Run("embeddings", func(t *testing.T) {
		const (
			userID                = 1102
			tokenID               = 1202
			channelID             = 1302
			requestID             = "cloudflare-embedding-billing-request"
			modelName             = "cf-embedding-test"
			estimatedPromptTokens = 3
			actualPromptTokens    = 31
			expectedQuota         = actualPromptTokens
			upstreamResponse      = `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.125,-0.5,1]}],"model":"openai/cf-embedding-test","usage":{"prompt_tokens":31,"completion_tokens":0,"total_tokens":31},"cf_extension":{"preserved":true}}`
			clientRequestBody     = `{"model":"cf-embedding-test","input":"use upstream embedding usage"}`
		)

		fixture.seed(t, userID, tokenID, channelID)
		var token model.Token
		require.NoError(t, fixture.db.First(&token, tokenID).Error)
		server, upstreamRequests := newCloudflareUpstream(t, upstreamResponse)
		context, recorder := newCloudflareRelayContext(
			t,
			"/v1/embeddings",
			clientRequestBody,
			server.URL,
			modelName,
			requestID,
			estimatedPromptTokens,
			userID,
			tokenID,
			channelID,
			token.Key,
		)

		request := &dto.EmbeddingRequest{
			Model: modelName,
			Input: "use upstream embedding usage",
		}
		info, err := relaycommon.GenRelayInfo(context, types.RelayFormatEmbedding, request, nil)
		require.NoError(t, err)
		info.PriceData = cloudflareBillingPriceData()

		require.Nil(t, EmbeddingHelper(context, info))

		upstreamRequest := <-upstreamRequests
		require.NoError(t, upstreamRequest.decodeErr)
		assert.Equal(t, "/client/v4/accounts/integration-account/ai/v1/embeddings", upstreamRequest.path)
		assert.Equal(t, "Bearer "+cloudflareBillingAPIKey, upstreamRequest.authorization)
		assert.Equal(t, "openai/"+modelName, upstreamRequest.model)
		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, upstreamResponse, recorder.Body.String())
		assert.Contains(t, recorder.Body.String(), `"data":[{"object":"embedding"`)
		assert.Contains(t, recorder.Body.String(), `"cf_extension":{"preserved":true}`)
		assert.Equal(t, "MISS", recorder.Header().Get("cf-aig-cache-status"))

		fixture.assertBilling(
			t,
			userID,
			tokenID,
			channelID,
			requestID,
			actualPromptTokens,
			0,
			expectedQuota,
		)
		assert.NotEqual(t, estimatedPromptTokens, actualPromptTokens)
	})
}
