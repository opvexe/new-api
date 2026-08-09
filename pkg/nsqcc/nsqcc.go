package nsqcc

import (
	"context"

	"github.com/nsqio/go-nsq"
)

type Service interface {
	// Connect attempts to establish a connection to the source, if
	// unsuccessful returns an error. If the attempt is successful (or not
	// necessary) returns nil.
	Connect(ctx context.Context) error

	// Close triggers the shut-down of this component and blocks until
	// completion or context cancellation.
	Close(ctx context.Context) error
}

// Async is a type that reads Benthos messages from an external source and
// allows acknowledgements for a message batch to be propagated asynchronously.
type Async interface {
	Service
	// ReadBatch attempts to read a new message from the source. If
	// successful a message is returned along with a function used to
	// acknowledge receipt of the returned message. It's safe to process the
	// returned message and read the next message asynchronously.
	ReadBatch(ctx context.Context) (*nsq.Message, AsyncAckFn, error)
}

// AsyncAckFn is a function used to acknowledge receipt of a message batch. The
// provided response indicates whether the message batch was successfully
// delivered. Returns an error if the acknowledgment was not propagated.
type AsyncAckFn func(context.Context, error) error

// noopAsyncAckFn is a no-op acknowledgment function.
var noopAsyncAckFn AsyncAckFn = func(context.Context, error) error {
	return nil
}

type AsyncSink interface {
	Service
	// WriteWithContext should block until either the message is sent (and
	// acknowledged) to a sink, or a transport specific error has occurred, or
	// the Type is closed.
	WriteWithContext(ctx context.Context, topic string, msg []byte) error
}
