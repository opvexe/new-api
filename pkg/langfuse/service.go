package langfuse

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/google/uuid"
)

const (
	EventTypeTraceCreate = "trace-create"

	defaultQueueSize = 1000
	maxBatchSize     = 50
	// A trace event carries the full request and response payload, so batches
	// are bounded by bytes as well as by count: the ingestion endpoint rejects
	// oversized request bodies, and a rejection loses every event in the batch.
	maxBatchBytes = 1 << 20
	flushInterval = time.Second
	// Batches are handed to a small pool of senders so that one slow ingestion
	// round trip does not stop the worker from draining the event queue.
	senderCount      = 4
	senderQueueDepth = 8
	httpTimeout      = 10 * time.Second
	shutdownTimeout  = 3 * time.Second
	dropReportPeriod = 30 * time.Second
)

type Usage struct {
	Input  int64 `json:"input,omitempty"`
	Output int64 `json:"output,omitempty"`
	Total  int64 `json:"total,omitempty"`
}

type GenerationRequest struct {
	TraceID         string
	Name            string
	Model           string
	StartTime       time.Time
	EndTime         time.Time
	Input           any
	Output          any
	Usage           *Usage
	UserID          string
	UserName        string
	ApiKeyID        string
	ApiKeyName      string
	ChannelID       int
	ChannelName     string
	ChannelType     int
	SiteTag         string
	Route           string
	SessionID       string
	ModelParameters map[string]any
	Metadata        map[string]any
}

type event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp string          `json:"timestamp"`
	Body      json.RawMessage `json:"body"`
}

type traceBody struct {
	ID          string         `json:"id"`
	Timestamp   time.Time      `json:"timestamp"`
	Name        string         `json:"name,omitempty"`
	UserID      string         `json:"userId,omitempty"`
	SessionID   string         `json:"sessionId,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Tags        []string       `json:"tags,omitempty"`
	Environment string         `json:"environment,omitempty"`
	Input       any            `json:"input,omitempty"`
	Output      any            `json:"output,omitempty"`
}

type batchRequest struct {
	Batch    []event `json:"batch"`
	Metadata struct {
		SDKName    string `json:"sdk_name,omitempty"`
		SDKVersion string `json:"sdk_version,omitempty"`
	} `json:"metadata,omitempty"`
}

type Client struct {
	endpoint    string
	authHeader  string
	environment string
	httpClient  *http.Client

	eventCh chan event
	sendCh  chan []event
	senders sync.WaitGroup
	stopCh  chan struct{}
	doneCh  chan struct{}
	force   context.Context
	cancel  context.CancelFunc
	start   sync.Once
	stop    sync.Once
	dropped atomic.Uint64
}

func NewClient(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if !config.Enabled {
		return nil, nil
	}
	queueSize := config.MaxQueueSize
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	force, cancel := context.WithCancel(context.Background())
	return &Client{
		endpoint:    strings.TrimRight(config.Endpoint, "/"),
		authHeader:  "Basic " + base64.StdEncoding.EncodeToString([]byte(config.PublicKey+":"+config.SecretKey)),
		environment: config.Environment,
		httpClient:  &http.Client{Timeout: httpTimeout},
		eventCh:     make(chan event, queueSize),
		sendCh:      make(chan []event, senderQueueDepth),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		force:       force,
		cancel:      cancel,
	}, nil
}

func NewClientFromEnv() (*Client, error) {
	config := Config{
		Enabled:      common.GetEnvOrDefaultBool("LANGFUSE_ENABLED", false),
		Endpoint:     common.GetEnvOrDefaultString("LANGFUSE_ENDPOINT", "https://cloud.langfuse.com"),
		PublicKey:    common.GetEnvOrDefaultString("LANGFUSE_PUBLIC_KEY", ""),
		SecretKey:    common.GetEnvOrDefaultString("LANGFUSE_SECRET_KEY", ""),
		MaxQueueSize: common.GetEnvOrDefault("LANGFUSE_MAX_QUEUE_SIZE", defaultQueueSize),
		Environment:  common.GetEnvOrDefaultString("LANGFUSE_ENVIRONMENT", "production"),
	}
	client, err := NewClient(config)
	if err != nil {
		return nil, err
	}
	if client == nil {
		common.SysLog("langfuse tracing is disabled")
		return nil, nil
	}
	common.SysLog(fmt.Sprintf("langfuse tracing enabled: endpoint=%s environment=%s", client.endpoint, client.environment))
	return client, nil
}

func (c *Client) Start() {
	if c == nil {
		return
	}
	c.start.Do(func() {
		go c.run()
	})
}

func (c *Client) Stop() {
	if c == nil {
		return
	}
	c.Start()
	c.stop.Do(func() {
		close(c.stopCh)
		select {
		case <-c.doneCh:
		case <-time.After(shutdownTimeout):
			c.cancel()
			<-c.doneCh
		}
	})
}

func (c *Client) TraceGeneration(request GenerationRequest) {
	if c == nil {
		return
	}
	traceID := request.TraceID
	if traceID == "" {
		traceID = uuid.NewString()
	}
	latency := request.EndTime.Sub(request.StartTime).Milliseconds()
	metadata := make(map[string]any, len(request.Metadata)+10)
	for key, value := range request.Metadata {
		metadata[key] = value
	}
	metadata["model"] = request.Model
	metadata["end_time"] = request.EndTime
	metadata["latency_ms"] = latency
	metadata["usage"] = request.Usage
	metadata["model_parameters"] = request.ModelParameters
	metadata["user_id"] = request.UserID
	metadata["route"] = request.Route
	metadata["request_id"] = traceID
	metadata["apikey_name"] = request.ApiKeyName
	metadata["api_key_id"] = request.ApiKeyID
	metadata["channel"] = request.ChannelName
	metadata["channel_id"] = request.ChannelID
	metadata["channel_type"] = constant.GetChannelTypeName(request.ChannelType)
	if request.SiteTag != "" {
		metadata["site"] = request.SiteTag
	}
	trace := traceBody{
		ID:          traceID,
		Timestamp:   request.StartTime,
		Name:        request.Name,
		UserID:      request.UserID,
		SessionID:   request.SessionID,
		Metadata:    metadata,
		Environment: c.environment,
		Input:       request.Input,
		Output:      request.Output,
	}
	if request.UserName != "" {
		trace.Tags = []string{"user:" + request.UserName}
	}
	c.enqueue(EventTypeTraceCreate, request.StartTime, trace)
}

func (c *Client) enqueue(eventType string, timestamp time.Time, body any) {
	bodyData, err := common.Marshal(body)
	if err != nil {
		common.SysError("failed to marshal langfuse event: " + err.Error())
		return
	}
	item := event{
		ID:        uuid.NewString(),
		Type:      eventType,
		Timestamp: timestamp.UTC().Format(time.RFC3339Nano),
		Body:      bodyData,
	}
	select {
	case <-c.stopCh:
		c.dropped.Add(1)
		return
	default:
	}
	// Tracing must never slow down the relay request: drop instead of blocking
	// the caller when the ingestion worker cannot keep up.
	select {
	case c.eventCh <- item:
	default:
		c.dropped.Add(1)
	}
}

func (c *Client) run() {
	defer close(c.doneCh)
	for i := 0; i < senderCount; i++ {
		c.senders.Add(1)
		go func() {
			defer c.senders.Done()
			for batch := range c.sendCh {
				c.send(c.force, batch)
			}
		}()
	}
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()
	dropTicker := time.NewTicker(dropReportPeriod)
	defer dropTicker.Stop()
	batch := make([]event, 0, maxBatchSize)
	batchBytes := 0
	for {
		select {
		case <-c.stopCh:
			c.drain(batch, batchBytes)
			close(c.sendCh)
			c.senders.Wait()
			return
		case item := <-c.eventCh:
			// Flush before appending so an event larger than the byte budget
			// forms a batch of its own instead of dragging others into a
			// request that the ingestion endpoint would reject outright.
			if len(batch) > 0 && batchBytes+len(item.Body) > maxBatchBytes {
				batch, batchBytes = c.dispatch(batch), 0
			}
			batch = append(batch, item)
			batchBytes += len(item.Body)
			if len(batch) >= maxBatchSize {
				batch, batchBytes = c.dispatch(batch), 0
			}
		case <-flushTicker.C:
			batch, batchBytes = c.dispatch(batch), 0
		case <-dropTicker.C:
			if dropped := c.dropped.Swap(0); dropped > 0 {
				common.SysError(fmt.Sprintf("langfuse dropped %d events because the queue was full or ingestion failed", dropped))
			}
		}
	}
}

// dispatch hands the batch to a sender and returns a fresh batch buffer. The
// sender owns the slice from here on, so it cannot be truncated and reused.
func (c *Client) dispatch(batch []event) []event {
	if len(batch) == 0 {
		return batch
	}
	select {
	case c.sendCh <- batch:
	default:
		c.dropped.Add(uint64(len(batch)))
	}
	return make([]event, 0, maxBatchSize)
}

func (c *Client) drain(batch []event, batchBytes int) {
	ctx, cancel := context.WithTimeout(c.force, shutdownTimeout)
	defer cancel()
	for {
		select {
		case item := <-c.eventCh:
			if len(batch) > 0 && batchBytes+len(item.Body) > maxBatchBytes {
				c.send(ctx, batch)
				batch, batchBytes = batch[:0], 0
			}
			batch = append(batch, item)
			batchBytes += len(item.Body)
			if len(batch) >= maxBatchSize {
				c.send(ctx, batch)
				batch, batchBytes = batch[:0], 0
			}
		default:
			c.send(ctx, batch)
			return
		}
	}
}

func (c *Client) send(parent context.Context, events []event) {
	if len(events) == 0 {
		return
	}
	requestBody := batchRequest{Batch: events}
	requestBody.Metadata.SDKName = "new-api-go"
	requestBody.Metadata.SDKVersion = "1.0.0"
	body, err := common.Marshal(requestBody)
	if err != nil {
		c.dropped.Add(uint64(len(events)))
		common.SysError("failed to marshal langfuse batch: " + err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(parent, httpTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/api/public/ingestion", bytes.NewReader(body))
	if err != nil {
		c.dropped.Add(uint64(len(events)))
		common.SysError("failed to create langfuse request: " + err.Error())
		return
	}
	request.Header.Set("Authorization", c.authHeader)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		c.dropped.Add(uint64(len(events)))
		common.SysError("failed to send langfuse batch: " + err.Error())
		return
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusMultipleChoices {
		common.SysLog(fmt.Sprintf("langfuse ingestion succeeded: status=%d events=%d", response.StatusCode, len(events)))
		return
	}
	responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 4*1024))
	c.dropped.Add(uint64(len(events)))
	common.SysError(fmt.Sprintf("langfuse ingestion failed: status=%d body=%s", response.StatusCode, responseBody))
}
