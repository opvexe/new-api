package langfuse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSiteTagFromHost(t *testing.T) {
	tests := []struct {
		name string
		host string
		want string
	}{
		{name: "www domain", host: "www.baidu.com", want: "baidu"},
		{name: "subdomain and port", host: "api.baidu.com:8443", want: "baidu"},
		{name: "multi-part public suffix", host: "www.example.co.uk", want: "example"},
		{name: "localhost", host: "localhost:3000", want: ""},
		{name: "IP address", host: "127.0.0.1:3000", want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, siteTagFromHost(test.host))
		})
	}
}

func TestSiteTagPrefersConfiguredServerAddress(t *testing.T) {
	tests := []struct {
		name          string
		serverAddress string
		requestHost   string
		want          string
	}{
		{
			name:          "configured public domain",
			serverAddress: "https://www.baidu.com",
			requestHost:   "127.0.0.1:3000",
			want:          "baidu",
		},
		{
			name:          "request host fallback",
			serverAddress: "http://localhost:3000",
			requestHost:   "www.baidu.com",
			want:          "baidu",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, siteTag(test.serverAddress, test.requestHost))
		})
	}
}
