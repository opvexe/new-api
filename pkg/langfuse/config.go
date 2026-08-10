package langfuse

import (
	"fmt"
	"time"
)

type Config struct {
	Enabled      bool
	Endpoint     string
	PublicKey    string
	SecretKey    string
	MaxQueueSize int
	Environment  string

	// NSQDAddress switches the producer from posting to Langfuse directly to
	// publishing one message per trace event into NSQ. The consumer then paces
	// delivery into Langfuse, so an ingestion outage backs up on NSQ's disk
	// instead of being dropped from the in-memory queue.
	NSQDAddress string
	NSQTopic    string
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Endpoint == "" {
		return fmt.Errorf("langfuse endpoint is required")
	}
	if c.PublicKey == "" || c.SecretKey == "" {
		return fmt.Errorf("langfuse public key and secret key are required")
	}
	if c.NSQDAddress != "" && c.NSQTopic == "" {
		return fmt.Errorf("langfuse nsq topic is required when an nsqd address is configured")
	}
	return nil
}

// ConsumerConfig drives the NSQ side of the pipeline: it reads trace events
// back off the queue and posts them to Langfuse at a controlled rate.
type ConsumerConfig struct {
	Enabled   bool
	Endpoint  string
	PublicKey string
	SecretKey string

	NSQDAddresses   []string
	LookupAddresses []string
	Topic           string
	Channel         string

	// MaxInFlight caps how many messages NSQ hands to the handler at once. The
	// default of 1 makes delivery strictly serial: one event finishes before
	// the next starts.
	MaxInFlight int
	// Workers is how many deliveries run at once. MaxInFlight only widens NSQ's
	// prefetch window; it cannot raise throughput, because nsqcc hands messages
	// over one at a time and each delivery blocks on an HTTP round trip.
	Workers int
	// MinInterval paces delivery further by holding each message until the
	// interval since the previous one has elapsed. Zero disables pacing.
	MinInterval time.Duration
	// MaxAttempts bounds requeues so a permanently rejected event cannot loop
	// forever. Zero removes the bound, which is the safe default here: NSQ
	// discards a message that exhausts its attempts, so a limit trades an outage
	// for permanent loss.
	MaxAttempts uint16
}

func (c ConsumerConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Endpoint == "" {
		return fmt.Errorf("langfuse endpoint is required")
	}
	if c.PublicKey == "" || c.SecretKey == "" {
		return fmt.Errorf("langfuse public key and secret key are required")
	}
	if c.Topic == "" || c.Channel == "" {
		return fmt.Errorf("langfuse nsq topic and channel are required")
	}
	// nsqcc's reader connects to both, and rejects either list being empty.
	if len(c.NSQDAddresses) == 0 || len(c.LookupAddresses) == 0 {
		return fmt.Errorf("langfuse consumer requires both nsqd and nsqlookupd addresses")
	}
	return nil
}
