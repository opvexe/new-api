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
	httpTimeout      = 60 * time.Second
	shutdownTimeout  = 3 * time.Second
	dropReportPeriod = 30 * time.Second
)

// logEnabled gates every line this package writes. Tracing runs at request
// rate, so the success path alone is thousands of lines an hour, and the
// startup lines carry the ingestion endpoint and the NSQ addresses.
var logEnabled = false

func logInfo(message string) {
	if logEnabled {
		common.SysLog(message)
	}
}

func logError(message string) {
	if logEnabled {
		common.SysError(message)
	}
}

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
	environment string
	sink        eventSink
	// batchSize bounds how many events one deliver call carries. Both sinks want
	// the same bound for different reasons: the ingestion endpoint rejects
	// oversized request bodies, and nsqd rejects an MPUB over its max-body-size.
	batchSize  int
	batchBytes int

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
	sink, batchSize, batchBytes := eventSink(newIngester(config.Endpoint, config.PublicKey, config.SecretKey)), maxBatchSize, maxBatchBytes
	if config.NSQDAddress != "" {
		producer, err := newNSQSink(config.NSQDAddress, config.NSQTopic)
		if err != nil {
			return nil, err
		}
		// Batching stays as it is for the direct sink. It does not change what
		// the consumer reads back — nsqSink still marshals one message per event
		// — it only lets those messages ship in a single MPUB, which is what
		// keeps the producer from being capped by per-message round trips.
		sink = producer
	}
	force, cancel := context.WithCancel(context.Background())
	return &Client{
		environment: config.Environment,
		sink:        sink,
		batchSize:   batchSize,
		batchBytes:  batchBytes,
		eventCh:     make(chan event, queueSize),
		sendCh:      make(chan []event, senderQueueDepth),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		force:       force,
		cancel:      cancel,
	}, nil
}

func NewClientFromEnv() (*Client, error) {
	logEnabled = common.GetEnvOrDefaultBool("LANGFUSE_LOG_ENABLED", false)
	config := Config{
		Enabled:      common.GetEnvOrDefaultBool("LANGFUSE_ENABLED", false),
		Endpoint:     common.GetEnvOrDefaultString("LANGFUSE_ENDPOINT", "https://cloud.langfuse.com"),
		PublicKey:    common.GetEnvOrDefaultString("LANGFUSE_PUBLIC_KEY", ""),
		SecretKey:    common.GetEnvOrDefaultString("LANGFUSE_SECRET_KEY", ""),
		MaxQueueSize: common.GetEnvOrDefault("LANGFUSE_MAX_QUEUE_SIZE", defaultQueueSize),
		Environment:  common.GetEnvOrDefaultString("LANGFUSE_ENVIRONMENT", "production"),
		NSQDAddress:  common.GetEnvOrDefaultString("LANGFUSE_NSQD_ADDRESS", ""),
		NSQTopic:     common.GetEnvOrDefaultString("LANGFUSE_NSQ_TOPIC", defaultNSQTopic),
	}
	client, err := NewClient(config)
	if err != nil {
		logError("langfuse tracing disabled: " + err.Error())
		return nil, err
	}
	if client == nil {
		logInfo("langfuse tracing is disabled")
		return nil, nil
	}
	transport := "direct"
	if config.NSQDAddress != "" {
		transport = fmt.Sprintf("nsq(%s topic=%s)", config.NSQDAddress, config.NSQTopic)
	}
	logInfo(fmt.Sprintf("langfuse tracing enabled: endpoint=%s environment=%s transport=%s", config.Endpoint, config.Environment, transport))
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
		logError("failed to marshal langfuse event: " + err.Error())
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
	batch := make([]event, 0, c.batchSize)
	batchBytes := 0
	for {
		select {
		case <-c.stopCh:
			c.drain(batch, batchBytes)
			close(c.sendCh)
			c.senders.Wait()
			c.sink.Close()
			return
		case item := <-c.eventCh:
			// Flush before appending so an event larger than the byte budget
			// forms a batch of its own instead of dragging others into a
			// request that the ingestion endpoint would reject outright.
			if c.batchBytes > 0 && len(batch) > 0 && batchBytes+len(item.Body) > c.batchBytes {
				batch, batchBytes = c.dispatch(batch), 0
			}
			batch = append(batch, item)
			batchBytes += len(item.Body)
			if len(batch) >= c.batchSize {
				batch, batchBytes = c.dispatch(batch), 0
			}
		case <-flushTicker.C:
			batch, batchBytes = c.dispatch(batch), 0
		case <-dropTicker.C:
			if dropped := c.dropped.Swap(0); dropped > 0 {
				logError(fmt.Sprintf("langfuse dropped %d events because the queue was full or ingestion failed", dropped))
			}
		}
	}
}

// dispatch hands the batch to a sender and returns a fresh batch buffer. The
// sender owns the slice from here on, so it cannot be truncated and reused.
//
// Waiting here is deliberate. dispatch runs on the worker goroutine, never on a
// relay request, so blocking costs nothing user-visible; it simply stops the
// worker from draining eventCh, which is the buffer sized to absorb bursts.
// Dropping instead would cap the effective queue at the handoff depth and throw
// away events the 1000-deep queue exists to protect. enqueue stays the single
// place where events are dropped.
func (c *Client) dispatch(batch []event) []event {
	if len(batch) == 0 {
		return batch
	}
	// A plain blocking handoff cannot deadlock: the worker is the only writer to
	// sendCh and closes it itself, after drain, so the senders are still
	// consuming for as long as this can block. Selecting on stopCh here instead
	// would abandon batches during shutdown, because a closed stopCh makes both
	// cases ready and select picks between them at random.
	c.sendCh <- batch
	return make([]event, 0, c.batchSize)
}

func (c *Client) drain(batch []event, batchBytes int) {
	ctx, cancel := context.WithTimeout(c.force, shutdownTimeout)
	defer cancel()
	for {
		select {
		case item := <-c.eventCh:
			if c.batchBytes > 0 && len(batch) > 0 && batchBytes+len(item.Body) > c.batchBytes {
				c.send(ctx, batch)
				batch, batchBytes = batch[:0], 0
			}
			batch = append(batch, item)
			batchBytes += len(item.Body)
			if len(batch) >= c.batchSize {
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
	if err := c.sink.deliver(parent, events); err != nil {
		c.dropped.Add(uint64(len(events)))
		logError("failed to deliver langfuse events: " + err.Error())
	}
}

// eventSink is the producer's next hop: either Langfuse itself or the NSQ
// buffer in front of it.
type eventSink interface {
	deliver(ctx context.Context, events []event) error
	Close()
}

// ingester posts events to the Langfuse ingestion endpoint. The producer uses
// it when NSQ is not configured, and the NSQ consumer always uses it.
type ingester struct {
	endpoint   string
	authHeader string
	httpClient *http.Client
}

func newIngester(endpoint, publicKey, secretKey string) *ingester {
	return &ingester{
		endpoint:   strings.TrimRight(endpoint, "/"),
		authHeader: "Basic " + base64.StdEncoding.EncodeToString([]byte(publicKey+":"+secretKey)),
		httpClient: &http.Client{Timeout: httpTimeout},
	}
}

func (i *ingester) Close() {}

func (i *ingester) deliver(parent context.Context, events []event) error {
	if len(events) == 0 {
		return nil
	}
	requestBody := batchRequest{Batch: events}
	requestBody.Metadata.SDKName = "new-api-go"
	requestBody.Metadata.SDKVersion = "1.0.0"
	body, err := common.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("marshal langfuse batch: %w", err)
	}
	ctx, cancel := context.WithTimeout(parent, httpTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, i.endpoint+"/api/public/ingestion", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create langfuse request: %w", err)
	}
	request.Header.Set("Authorization", i.authHeader)
	request.Header.Set("Content-Type", "application/json")
	response, err := i.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("send langfuse batch: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusMultipleChoices {
		logInfo(fmt.Sprintf("langfuse ingestion succeeded: status=%d events=%d", response.StatusCode, len(events)))
		return nil
	}
	responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 4*1024))
	return fmt.Errorf("langfuse ingestion failed: status=%d body=%s", response.StatusCode, responseBody)
}
