package in

import (
	"context"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noopDelegate struct{}

func (noopDelegate) OnFinish(*nsq.Message)                       {}
func (noopDelegate) OnRequeue(*nsq.Message, time.Duration, bool) {}
func (noopDelegate) OnTouch(*nsq.Message)                        {}

// Acknowledging a message must release it. The reader tracks unacknowledged
// messages so shutdown can requeue them, and every tracked message pins its
// whole body: if acknowledgement does not remove it, a long-running consumer
// retains every message it has ever handled and grows without bound.
func TestReadBatchReleasesAcknowledgedMessages(t *testing.T) {
	reader := &nsqReader{
		unAckMsgs:        make(map[nsq.MessageID]*nsq.Message),
		internalMessages: make(chan *nsq.Message, 1),
		interruptChan:    make(chan struct{}),
	}

	for i := 0; i < 100; i++ {
		var id nsq.MessageID
		id[0] = byte(i)
		id[1] = byte(i >> 8)
		message := nsq.NewMessage(id, []byte("body"))
		message.Delegate = noopDelegate{}
		reader.internalMessages <- message

		got, ack, err := reader.ReadBatch(context.Background())
		require.NoError(t, err)
		require.Equal(t, message, got)
		require.Len(t, reader.unAckMsgs, 1, "the in-flight message must be tracked")
		require.NoError(t, ack(context.Background(), nil))
		assert.Empty(t, reader.unAckMsgs, "acknowledged message %d was not released", i)
	}
}

// A failed delivery is requeued rather than finished, but it is no longer this
// reader's responsibility either, so it must be released the same way.
func TestReadBatchReleasesRequeuedMessages(t *testing.T) {
	reader := &nsqReader{
		unAckMsgs:        make(map[nsq.MessageID]*nsq.Message),
		internalMessages: make(chan *nsq.Message, 1),
		interruptChan:    make(chan struct{}),
	}

	message := nsq.NewMessage(nsq.MessageID{9}, []byte("body"))
	message.Delegate = noopDelegate{}
	reader.internalMessages <- message

	_, ack, err := reader.ReadBatch(context.Background())
	require.NoError(t, err)
	require.NoError(t, ack(context.Background(), assert.AnError))
	assert.Empty(t, reader.unAckMsgs)
}
