package langfuse

import "fmt"

type Config struct {
	Enabled      bool
	Endpoint     string
	PublicKey    string
	SecretKey    string
	MaxQueueSize int
	Environment  string
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
	return nil
}
