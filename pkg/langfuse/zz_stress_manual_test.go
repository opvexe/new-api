package langfuse

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
)

// 临时压测文件，跑完即删。

type upstream struct {
	server   *httptest.Server
	batches  atomic.Int64
	events   atomic.Int64
	inflight atomic.Int64
}

func newUpstream(t *testing.T, handle func()) *upstream {
	t.Helper()
	up := &upstream{}
	up.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload batchRequest
		_ = common.DecodeJson(r.Body, &payload)
		up.batches.Add(1)
		up.events.Add(int64(len(payload.Batch)))
		up.inflight.Add(1)
		defer up.inflight.Add(-1)
		handle()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(up.server.Close)
	return up
}

func newStressClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	client, err := NewClient(Config{
		Enabled: true, Endpoint: endpoint, PublicKey: "pk", SecretKey: "sk",
		MaxQueueSize: defaultQueueSize, Environment: "stress",
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Start()
	t.Cleanup(client.Stop)
	return client
}

type stats struct {
	p50, p99, p999, max time.Duration
	dropped             uint64
}

// 以固定 QPS 持续压 duration 时长，模拟真实 relay 流量。
func driveAtRate(t *testing.T, client *Client, qps int, duration time.Duration) stats {
	t.Helper()
	const workers = 64
	interval := time.Duration(int64(time.Second) / int64(qps) * int64(workers))
	perWorker := int(duration / (interval))
	latencies := make([]time.Duration, 0, workers*perWorker)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := make([]time.Duration, 0, perWorker)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for i := 0; i < perWorker; i++ {
				<-ticker.C
				now := time.Now()
				start := now
				client.TraceGeneration(GenerationRequest{
					TraceID:   fmt.Sprintf("trace-%d-%d", w, i),
					Name:      "anthropic-messages",
					Model:     "claude-opus-5",
					StartTime: now,
					EndTime:   now.Add(time.Second),
					Usage:     &Usage{Input: 1000, Output: 2000, Total: 3000},
					Input:     map[string]any{"messages": "hello world, a reasonably sized prompt"},
					Output:    map[string]any{"text": "a reasonably sized completion body"},
				})
				local = append(local, time.Since(start))
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })
	q := func(f float64) time.Duration { return latencies[int(float64(len(latencies)-1)*f)] }
	return stats{p50: q(0.5), p99: q(0.99), p999: q(0.999), max: latencies[len(latencies)-1], dropped: client.dropped.Load()}
}

// 核心不变量：上游挂死时的请求路径延迟，必须和上游健康时基本一致。
// 只要两者同量级，就说明 Langfuse 的慢/挂完全没有传导到 relay 请求。
func TestStressUpstreamStateDoesNotAffectRequestPath(t *testing.T) {
	const qps = 500
	const duration = 6 * time.Second

	release := make(chan struct{})
	defer close(release)

	cases := []struct {
		name   string
		handle func()
	}{
		{"healthy", func() {}},
		{"slow-2s", func() { time.Sleep(2 * time.Second) }},
		{"hung", func() { <-release }},
	}

	results := make(map[string]stats)
	for _, tc := range cases {
		up := newUpstream(t, tc.handle)
		client := newStressClient(t, up.server.URL)
		got := driveAtRate(t, client, qps, duration)
		results[tc.name] = got
		time.Sleep(1500 * time.Millisecond)
		t.Logf("%-8s | p50=%-10v p99=%-10v p999=%-10v max=%-10v | dropped=%-6d batches=%-4d events=%-6d avg_batch=%.1f",
			tc.name, got.p50, got.p99, got.p999, got.max, got.dropped,
			up.batches.Load(), up.events.Load(),
			float64(up.events.Load())/float64(maxInt64(up.batches.Load(), 1)))
	}

	// 挂死 vs 健康：p999 不得劣化超过 5 倍，且绝对值必须在毫秒级。
	healthy, hung := results["healthy"], results["hung"]
	if hung.p999 > 20*time.Millisecond {
		t.Errorf("上游挂死时 p999 过高: %v", hung.p999)
	}
	if healthy.p999 > 0 && hung.p999 > 5*healthy.p999 {
		t.Errorf("上游挂死显著拖慢请求路径: healthy p999=%v hung p999=%v", healthy.p999, hung.p999)
	}
	if healthy.dropped != 0 {
		t.Errorf("健康上游下不应丢事件: dropped=%d", healthy.dropped)
	}
}

// 上游挂死时 Stop 必须在 shutdownTimeout 附近返回。
func TestStressShutdownWhileUpstreamHung(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	up := newUpstream(t, func() { <-release })

	client, err := NewClient(Config{
		Enabled: true, Endpoint: up.server.URL, PublicKey: "pk", SecretKey: "sk",
		MaxQueueSize: defaultQueueSize, Environment: "stress",
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Start()
	driveAtRate(t, client, 200, 2*time.Second)

	start := time.Now()
	done := make(chan struct{})
	go func() { client.Stop(); close(done) }()
	select {
	case <-done:
		t.Logf("Stop 耗时 %v", time.Since(start))
	case <-time.After(30 * time.Second):
		t.Fatal("Stop 永久阻塞")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("Stop 耗时过长: %v", elapsed)
	}
}

// Stop 之后继续调用不得阻塞或 panic。
func TestStressTraceAfterStop(t *testing.T) {
	up := newUpstream(t, func() {})
	client, err := NewClient(Config{
		Enabled: true, Endpoint: up.server.URL, PublicKey: "pk", SecretKey: "sk",
		MaxQueueSize: defaultQueueSize, Environment: "stress",
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Start()
	client.Stop()

	got := driveAtRate(t, client, 500, 2*time.Second)
	t.Logf("after-stop | p50=%v p99=%v p999=%v max=%v dropped=%d", got.p50, got.p99, got.p999, got.max, got.dropped)
	if got.p999 > 20*time.Millisecond {
		t.Errorf("Stop 后调用被阻塞: p999=%v", got.p999)
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
