package langfuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/nsqcc"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// queuedReader hands out prepared messages so the consumer can be exercised
// without a live nsqd.
type queuedReader struct {
	messages chan *nsq.Message
}

func (r *queuedReader) Connect(context.Context) error { return nil }
func (r *queuedReader) Close(context.Context) error   { return nil }

func (r *queuedReader) ReadBatch(ctx context.Context) (*nsq.Message, nsqcc.AsyncAckFn, error) {
	select {
	case message := <-r.messages:
		return message, func(context.Context, error) error { return nil }, nil
	case <-ctx.Done():
		return nil, nil, nsqcc.ErrTypeClosed
	}
}

// Every hop between nsqd and Langfuse is serial — go-nsq registers one handler,
// nsqcc hands messages over an unbuffered channel, and each delivery blocks on
// an HTTP round trip — so the worker count is the only thing that sets drain
// rate. MaxInFlight widens NSQ's prefetch window and changes nothing here.
func TestConsumerDeliversWithConfiguredConcurrency(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(map[bool]string{true: "serial", false: "concurrent"}[workers == 1], func(t *testing.T) {
			const messages = 24

			var inFlight, peak, handled atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				current := inFlight.Add(1)
				for {
					seen := peak.Load()
					if current <= seen || peak.CompareAndSwap(seen, current) {
						break
					}
				}
				time.Sleep(40 * time.Millisecond)
				inFlight.Add(-1)
				handled.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			body, err := common.Marshal(event{
				ID:        "probe",
				Type:      EventTypeTraceCreate,
				Timestamp: "2026-08-10T00:00:00Z",
				Body:      json.RawMessage(`{"id":"trace-probe"}`),
			})
			require.NoError(t, err)

			reader := &queuedReader{messages: make(chan *nsq.Message, messages)}
			for i := 0; i < messages; i++ {
				reader.messages <- nsq.NewMessage(nsq.MessageID{byte(i)}, body)
			}

			ctx, cancel := context.WithCancel(context.Background())
			consumer := &Consumer{
				reader:   reader,
				ingester: newIngester(server.URL, "pk", "sk"),
				workers:  workers,
				ctx:      ctx,
				cancel:   cancel,
				done:     make(chan struct{}),
			}
			require.NoError(t, consumer.Start())

			require.Eventually(t, func() bool { return handled.Load() == messages },
				20*time.Second, 20*time.Millisecond, "not every message was delivered")
			consumer.Stop()

			assert.Equal(t, int64(workers), peak.Load(),
				"peak concurrent deliveries must match the configured worker count")
		})
	}
}
