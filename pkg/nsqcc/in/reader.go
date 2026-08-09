package in

import (
	"context"
	"crypto/tls"
	"io"
	"log"
	"sync"

	"github.com/QuantumNous/new-api/pkg/nsqcc"

	"github.com/QuantumNous/new-api/pkg/nsqcc/filepath/ifs"
	"github.com/nsqio/go-nsq"
)

type nsqReader struct {
	consumer         *nsq.Consumer
	cMut             sync.Mutex
	unAckMsgs        []*nsq.Message
	internalMessages chan *nsq.Message
	interruptChan    chan struct{}
	interruptOnce    sync.Once
	tlsConf          *tls.Config
	conf             Config
}

func NewNSQReader(conf Config, mgr ifs.FS) (nsqcc.Async, error) {
	n := &nsqReader{
		conf:             conf,
		internalMessages: make(chan *nsq.Message),
		interruptChan:    make(chan struct{}),
	}

	if conf.TLS.Enabled {
		var err error
		if n.tlsConf, err = conf.TLS.Get(mgr); err != nil {
			return nil, err
		}
	}
	return n, nil
}

func (n *nsqReader) Connect(ctx context.Context) error {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	cfg := nsq.NewConfig()
	cfg.UserAgent = n.conf.UserAgent
	cfg.MaxInFlight = n.conf.MaxInFlight
	cfg.MaxAttempts = n.conf.MaxAttempts
	cfg.ReadTimeout = n.conf.ReadTimeout
	cfg.WriteTimeout = n.conf.WriteTimeout
	cfg.Deflate = n.conf.Compression
	cfg.Snappy = n.conf.Compression
	if n.tlsConf != nil {
		cfg.TlsV1 = true
		cfg.TlsConfig = n.tlsConf
	}

	consumer, err := nsq.NewConsumer(n.conf.Topic, n.conf.Channel, cfg)
	if err != nil {
		return err
	}

	consumer.SetLogger(log.New(io.Discard, "", log.Flags()), nsq.LogLevelError)
	consumer.AddHandler(n)

	if err = consumer.ConnectToNSQDs(n.conf.Addresses); err != nil {
		consumer.Stop()
		return err
	}

	if err := consumer.ConnectToNSQLookupds(n.conf.LookupAddresses); err != nil {
		consumer.Stop()
		return err
	}

	n.consumer = consumer
	return nil
}

func (n *nsqReader) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	select {
	case n.internalMessages <- message:
	case <-n.interruptChan:
		message.Requeue(-1)
		message.Finish()
	}
	return nil
}

func (n *nsqReader) ReadBatch(ctx context.Context) (*nsq.Message, nsqcc.AsyncAckFn, error) {
	msg, err := n.read(ctx)
	if err != nil {
		return nil, nil, err
	}
	n.unAckMsgs = append(n.unAckMsgs, msg)

	return msg, func(rctx context.Context, res error) error {
		if res != nil {
			msg.Requeue(-1)
		}
		msg.Finish()
		return nil
	}, nil
}

func (n *nsqReader) read(ctx context.Context) (*nsq.Message, error) {
	var msg *nsq.Message
	select {
	case msg = <-n.internalMessages:
		return msg, nil
	case <-ctx.Done():
	case <-n.interruptChan:
		for _, m := range n.unAckMsgs {
			m.Requeue(-1)
			m.Finish()
		}
		n.unAckMsgs = nil
		_ = n.disconnect()
		return nil, nsqcc.ErrTypeClosed
	}
	return nil, nsqcc.ErrTimeout
}

func (n *nsqReader) disconnect() error {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.consumer != nil {
		n.consumer.Stop()
		n.consumer = nil
	}
	return nil
}

func (n *nsqReader) Close(ctx context.Context) (err error) {
	n.interruptOnce.Do(func() {
		close(n.interruptChan)
	})
	err = n.disconnect()
	return
}
