package langfuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/nsqcc"
	"github.com/QuantumNous/new-api/pkg/nsqcc/filepath/ifs"
	"github.com/QuantumNous/new-api/pkg/nsqcc/in"
	"github.com/nsqio/go-nsq"
)

const (
	defaultNSQTopic       = "langfuse-events"
	defaultNSQChannel     = "ingest"
	defaultNSQMaxInFlight = 1
	// Zero disables NSQ's attempt limit. go-nsq discards a message outright once
	// it exceeds MaxAttempts, and its requeue backoff is linear with a 15 minute
	// ceiling, so any finite limit converts a long Langfuse outage into permanent
	// loss — ten attempts is roughly an hour. Surviving outages is the reason the
	// queue exists, so the default is to keep retrying.
	defaultNSQMaxAttempts    = 0
	defaultNSQLookupAddress  = "127.0.0.1:4161"
	nsqProducerUserAgent     = "new-api-langfuse-producer/1.0"
	nsqConsumerUserAgent     = "new-api-langfuse-consumer/1.0"
	nsqReconnectReportPeriod = time.Minute

	// Matches nsqd's --max-msg-size, and must stay under its --max-body-size
	// (8 MB) or nsqd drops the connection instead of rejecting one event.
	nsqPublishBatchMessages = 50
	nsqPublishBatchBytes    = 4718000

	nsqDialTimeout  = 5 * time.Second
	nsqWriteTimeout = 5 * time.Second

	nsqPublishAttempts  = 5
	nsqPublishRetryBase = 200 * time.Millisecond
	nsqPublishRetryMax  = 1500 * time.Millisecond

	nsqConsumerConnectRetry = 30 * time.Second

	// One worker means strictly serial delivery. Throughput is workers divided
	// by the Langfuse round trip, so MaxInFlight cannot raise it: nsqcc hands
	// messages over one at a time and every delivery blocks on HTTP.
	defaultNSQWorkers = 1
)

// nsqSink publishes one message per trace event. Grouping events into ingestion
// batches is deliberately left to the consumer, which is the side that knows
// how much Langfuse can currently absorb.
//
// The events handed to a single deliver call are still shipped in one MPUB
// command. That only collapses the transport round trip — nsqd stores each
// event as its own message, so the consumer's unit of work is unchanged. It
// matters because a per-event PUB costs a write plus a flush plus a response on
// a single connection, which caps a 4-core nsqd at roughly 30k events/s;
// batching pushes the same host past 80k.
//
// nsqcc's writer only exposes single-message writes, so the producer talks to
// go-nsq directly. The consumer still uses nsqcc, which has no equivalent gap.
type nsqSink struct {
	address string
	topic   string

	mu       sync.RWMutex
	producer nsqPublisher

	connectMu     sync.Mutex
	lastReportErr time.Time
}

// nsqPublisher is the part of *nsq.Producer the sink uses. Keeping it an
// interface lets the batching rules be tested without a live nsqd, which is the
// only way to prove a batch never exceeds what nsqd will accept.
type nsqPublisher interface {
	Publish(topic string, body []byte) error
	MultiPublish(topic string, body [][]byte) error
	Ping() error
	Stop()
}

func newNSQSink(address string, topic string) (*nsqSink, error) {
	if address == "" {
		return nil, fmt.Errorf("langfuse nsqd address is required")
	}
	sink := &nsqSink{address: address, topic: topic}
	// connect pings nsqd and fails when it is unreachable. Tracing must not stop
	// new-api from starting, so a failure here only warns and the sink retries
	// on its next publish.
	if err := sink.connect(); err != nil {
		logError(fmt.Sprintf("langfuse nsqd %s is not reachable yet, will retry: %s", address, err.Error()))
	}
	return sink, nil
}

func (s *nsqSink) connect() error {
	config := nsq.NewConfig()
	config.UserAgent = nsqProducerUserAgent
	// go-nsq defaults both of these to one second, which is tuned for a local
	// nsqd. Publishing a full MPUB batch across a VPC can exceed that whenever
	// TCP backpressure builds, and a reconnect racing a busy nsqd can too; both
	// would surface as a dropped batch rather than a retry.
	config.DialTimeout = nsqDialTimeout
	config.WriteTimeout = nsqWriteTimeout
	producer, err := nsq.NewProducer(s.address, config)
	if err != nil {
		return fmt.Errorf("create nsq producer: %w", err)
	}
	producer.SetLogger(log.New(io.Discard, "", log.Flags()), nsq.LogLevelError)
	if err := producer.Ping(); err != nil {
		producer.Stop()
		return fmt.Errorf("ping nsqd: %w", err)
	}
	s.mu.Lock()
	previous := s.producer
	s.producer = producer
	s.mu.Unlock()
	if previous != nil {
		previous.Stop()
	}
	return nil
}

func (s *nsqSink) deliver(ctx context.Context, events []event) error {
	batch := make([][]byte, 0, len(events))
	batchBytes := 0
	for _, item := range events {
		body, err := common.Marshal(item)
		if err != nil {
			return fmt.Errorf("marshal langfuse event: %w", err)
		}
		if len(batch) > 0 && (len(batch) >= nsqPublishBatchMessages || batchBytes+len(body) > nsqPublishBatchBytes) {
			if err := s.publish(ctx, batch); err != nil {
				return err
			}
			// A fresh slice rather than batch[:0]: publish hands this to an
			// nsqPublisher, and reusing the backing array would rewrite a batch
			// the publisher may still be holding.
			batch = make([][]byte, 0, nsqPublishBatchMessages)
			batchBytes = 0
		}
		batch = append(batch, body)
		batchBytes += len(body)
	}
	if len(batch) == 0 {
		return nil
	}
	return s.publish(ctx, batch)
}

// publish retries because Client.send has no retry of its own: whatever this
// returns an error for is counted as dropped and gone. The budget is bounded at
// roughly the shutdown drain window so a restarting nsqd costs nothing, while a
// genuinely dead one still lets the batch fail instead of stalling the senders.
func (s *nsqSink) publish(ctx context.Context, bodies [][]byte) error {
	var lastErr error
	backoff := nsqPublishRetryBase
	for attempt := 0; attempt < nsqPublishAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		// go-nsq redials by itself between publishes, but a producer whose own
		// dial failed stays broken until it is rebuilt, so every retry rebuilds.
		if attempt > 0 {
			lastErr = s.reconnect()
		}
		if lastErr == nil {
			if lastErr = s.write(bodies); lastErr == nil {
				return nil
			}
			// nsqd answers a malformed or oversized command with a protocol
			// error and closes the connection. A retry replays the same bytes,
			// so it can only burn the budget that a reachable-but-busy nsqd
			// needs, and hold a sender off the queue while it does.
			var protocolErr nsq.ErrProtocol
			if errors.As(lastErr, &protocolErr) {
				return fmt.Errorf("nsqd rejected langfuse events: %w", lastErr)
			}
		}
		if attempt == nsqPublishAttempts-1 {
			break
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff *= 2
		if backoff > nsqPublishRetryMax {
			backoff = nsqPublishRetryMax
		}
	}
	return fmt.Errorf("publish langfuse events to nsq: %w", lastErr)
}

func (s *nsqSink) write(bodies [][]byte) error {
	s.mu.RLock()
	producer := s.producer
	s.mu.RUnlock()
	if producer == nil {
		return fmt.Errorf("langfuse nsq producer is not connected")
	}
	if len(bodies) == 1 {
		return producer.Publish(s.topic, bodies[0])
	}
	return producer.MultiPublish(s.topic, bodies)
}

func (s *nsqSink) reconnect() error {
	s.connectMu.Lock()
	defer s.connectMu.Unlock()
	if err := s.connect(); err != nil {
		// The sender pool retries constantly while nsqd is down; rate-limit the
		// log so an outage cannot flood it.
		if time.Since(s.lastReportErr) > nsqReconnectReportPeriod {
			s.lastReportErr = time.Now()
			logError("failed to reconnect langfuse nsq producer: " + err.Error())
		}
		return fmt.Errorf("reconnect nsq producer: %w", err)
	}
	return nil
}

func (s *nsqSink) Close() {
	s.mu.Lock()
	producer := s.producer
	s.producer = nil
	s.mu.Unlock()
	if producer != nil {
		producer.Stop()
	}
}

// Consumer drains the NSQ topic into Langfuse. nsqcc exposes a pull-based
// reader, so the loop below is serial by construction: one event is delivered
// and acknowledged before the next is read. That, plus MinInterval, is what
// bounds the load Langfuse sees regardless of relay traffic.
type Consumer struct {
	reader   nsqcc.Async
	ingester *ingester

	minInterval time.Duration
	paceMu      sync.Mutex
	nextAt      time.Time
	workers     int

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	stop   sync.Once
}

func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if !config.Enabled {
		return nil, nil
	}
	readerConfig := in.NewConfig()
	readerConfig.Addresses = config.NSQDAddresses
	readerConfig.LookupAddresses = config.LookupAddresses
	readerConfig.Topic = config.Topic
	readerConfig.Channel = config.Channel
	readerConfig.UserAgent = nsqConsumerUserAgent
	readerConfig.MaxInFlight = config.MaxInFlight
	if readerConfig.MaxInFlight <= 0 {
		readerConfig.MaxInFlight = defaultNSQMaxInFlight
	}
	// Assigned unconditionally: nsqcc defaults this to five, so skipping the
	// assignment when the caller asks for no limit would silently install one.
	readerConfig.MaxAttempts = config.MaxAttempts
	if err := readerConfig.Validate(); err != nil {
		return nil, err
	}
	reader, err := in.NewNSQReader(readerConfig, ifs.OS())
	if err != nil {
		return nil, fmt.Errorf("create nsq reader: %w", err)
	}
	workers := config.Workers
	if workers <= 0 {
		workers = defaultNSQWorkers
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		reader:      reader,
		ingester:    newIngester(config.Endpoint, config.PublicKey, config.SecretKey),
		minInterval: config.MinInterval,
		workers:     workers,
		ctx:         ctx,
		cancel:      cancel,
		done:        make(chan struct{}),
	}, nil
}

func NewConsumerFromEnv() (*Consumer, error) {
	logEnabled = common.GetEnvOrDefaultBool("LANGFUSE_LOG_ENABLED", false)
	config := ConsumerConfig{
		Enabled:         common.GetEnvOrDefaultBool("LANGFUSE_NSQ_CONSUMER_ENABLED", false),
		Endpoint:        common.GetEnvOrDefaultString("LANGFUSE_ENDPOINT", "https://cloud.langfuse.com"),
		PublicKey:       common.GetEnvOrDefaultString("LANGFUSE_PUBLIC_KEY", ""),
		SecretKey:       common.GetEnvOrDefaultString("LANGFUSE_SECRET_KEY", ""),
		NSQDAddresses:   splitAddresses(common.GetEnvOrDefaultString("LANGFUSE_NSQD_ADDRESS", "")),
		LookupAddresses: splitAddresses(common.GetEnvOrDefaultString("LANGFUSE_NSQ_LOOKUPD_ADDRESS", defaultNSQLookupAddress)),
		Topic:           common.GetEnvOrDefaultString("LANGFUSE_NSQ_TOPIC", defaultNSQTopic),
		Channel:         common.GetEnvOrDefaultString("LANGFUSE_NSQ_CHANNEL", defaultNSQChannel),
		MaxInFlight:     common.GetEnvOrDefault("LANGFUSE_NSQ_MAX_IN_FLIGHT", defaultNSQMaxInFlight),
		MinInterval:     time.Duration(common.GetEnvOrDefault("LANGFUSE_NSQ_MIN_INTERVAL_MS", 0)) * time.Millisecond,
		MaxAttempts:     uint16(common.GetEnvOrDefault("LANGFUSE_NSQ_MAX_ATTEMPTS", defaultNSQMaxAttempts)),
		Workers:         common.GetEnvOrDefault("LANGFUSE_NSQ_WORKERS", defaultNSQWorkers),
	}
	consumer, err := NewConsumer(config)
	if err != nil {
		logError("langfuse nsq consumer disabled: " + err.Error())
		return nil, err
	}
	if consumer == nil {
		return nil, nil
	}
	logInfo(fmt.Sprintf("langfuse nsq consumer enabled: topic=%s channel=%s nsqd=%v lookupd=%v workers=%d max_in_flight=%d min_interval=%s",
		config.Topic, config.Channel, config.NSQDAddresses, config.LookupAddresses, consumer.workers, config.MaxInFlight, config.MinInterval))
	return consumer, nil
}

func splitAddresses(value string) []string {
	var addresses []string
	for _, part := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			addresses = append(addresses, trimmed)
		}
	}
	return addresses
}

func (c *Consumer) Start() {
	if c == nil {
		return
	}
	go c.run()
}

// run connects before consuming, retrying in the background rather than
// reporting the failure upward. A caller cannot tell a wrong address from a
// host that is briefly unreachable, so surfacing the error would make an nsqd
// restart take the API gateway down with it.
func (c *Consumer) run() {
	defer close(c.done)
	for {
		err := c.reader.Connect(c.ctx)
		if err == nil {
			c.consumeAll()
			return
		}
		if c.ctx.Err() != nil {
			return
		}
		logError("langfuse nsq consumer cannot reach nsqd, will retry: " + err.Error())
		select {
		case <-time.After(nsqConsumerConnectRetry):
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Consumer) Stop() {
	if c == nil {
		return
	}
	c.stop.Do(func() {
		c.cancel()
		if err := c.reader.Close(context.Background()); err != nil {
			logError("failed to close langfuse nsq reader: " + err.Error())
		}
		select {
		case <-c.done:
		case <-time.After(shutdownTimeout):
		}
	})
}

// consumeAll runs the read/deliver/ack loop on several goroutines. Every step
// between nsqd and Langfuse is serial otherwise — go-nsq registers one handler,
// nsqcc hands messages over an unbuffered channel, and each delivery blocks on
// an HTTP round trip — so this is the only place that changes drain rate.
func (c *Consumer) consumeAll() {
	var workers sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			c.consume()
		}()
	}
	workers.Wait()
}

func (c *Consumer) consume() {
	for {
		message, ack, err := c.reader.ReadBatch(c.ctx)
		if err != nil {
			// nsqcc reports both shutdown and a cancelled context as errors, and
			// returns immediately in that state, so bail out instead of spinning.
			if c.ctx.Err() != nil || errors.Is(err, nsqcc.ErrTypeClosed) {
				return
			}
			continue
		}
		// A non-nil ack error requeues the message with NSQ's backoff, which is
		// what keeps events alive across a Langfuse outage.
		if ackErr := ack(c.ctx, c.deliverMessage(message)); ackErr != nil {
			logError("failed to acknowledge langfuse nsq message: " + ackErr.Error())
		}
	}
}

func (c *Consumer) deliverMessage(message *nsq.Message) error {
	if message == nil || len(message.Body) == 0 {
		return nil
	}
	var item event
	if err := common.Unmarshal(message.Body, &item); err != nil {
		// A malformed message will never become valid; requeuing it would block
		// the channel behind it forever, so drop it and keep the pipeline moving.
		logError("langfuse consumer discarding unparseable message: " + err.Error())
		return nil
	}
	c.pace()
	return c.ingester.deliver(c.ctx, []event{item})
}

// pace holds delivery until the configured interval since the previous message
// has elapsed, which is what bounds the load Langfuse sees.
func (c *Consumer) pace() {
	if c.minInterval <= 0 {
		return
	}
	// Each worker reserves its slot under the lock before waiting, so the
	// interval bounds the combined rate rather than each worker's own rate.
	// Reading nextAt and writing it back afterwards would let every worker see
	// the same deadline and fire together.
	c.paceMu.Lock()
	slot := c.nextAt
	if now := time.Now(); slot.Before(now) {
		slot = now
	}
	c.nextAt = slot.Add(c.minInterval)
	c.paceMu.Unlock()

	if wait := time.Until(slot); wait > 0 {
		select {
		case <-time.After(wait):
		case <-c.ctx.Done():
		}
	}
}
