// Package metrics exposes a Prometheus-format /metrics endpoint. It is
// dependency-free (0cgo, no external libs): counters are plain atomics and the
// exposition is hand-rendered in the Prometheus text format.
//
// Katrix-specific metrics:
//   - katrix_events_total{kind}          (sent, received, redacted)
//   - katrix_sync_requests_total         (all /sync invocations)
//   - katrix_sync_active                 (in-flight long-polls)
//   - katrix_federation_inbound_pdus_total
//   - katrix_federation_outbound_requests_total
//   - katrix_media_uploads_total
//   - katrix_media_remote_fetch_total
//   - katrix_db_pool_acquired_conns
//   - katrix_db_pool_max_conns
//
// Standard Go runtime metrics (goroutines, gc, memstats) are also exposed.
package metrics

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// Counters is the shared metric set. Increment via AddXxx/IncXxx; read via
// Render.
var Counters = &counters{}

type counters struct {
	EventsSent             atomic.Int64
	EventsReceived         atomic.Int64
	EventsRedacted         atomic.Int64
	SyncRequests           atomic.Int64
	SyncActive             atomic.Int64
	FedInboundPDUs         atomic.Int64
	FedOutboundRequests    atomic.Int64
	MediaUploads           atomic.Int64
	MediaRemoteFetch       atomic.Int64
	MediaRemoteFetchErrors atomic.Int64
}

// Handler returns the http.HandlerFunc that serves /metrics.
func Handler() http.HandlerFunc {
	startTime := time.Now()
	return func(w http.ResponseWriter, r *http.Request) {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		gc := debug.GCStats{PauseEnd: make([]time.Time, 0)}
		debug.ReadGCStats(&gc)

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		uptime := time.Since(startTime).Seconds()
		// Build info.
		fmt.Fprintf(w, "# HELP katrix_build_info Build metadata.\n")
		fmt.Fprintf(w, "# TYPE katrix_build_info gauge\n")
		fmt.Fprintf(w, "katrix_build_info{version=\"%s\"} 1\n", escape(os.Getenv("KATRIX_VERSION")))
		// Uptime.
		fmt.Fprintf(w, "# HELP katrix_uptime_seconds Server uptime in seconds.\n")
		fmt.Fprintf(w, "# TYPE katrix_uptime_seconds gauge\n")
		fmt.Fprintf(w, "katrix_uptime_seconds %f\n", uptime)
		// Go runtime.
		fmt.Fprintf(w, "# HELP go_goroutines Number of goroutines.\n")
		fmt.Fprintf(w, "# TYPE go_goroutines gauge\n")
		fmt.Fprintf(w, "go_goroutines %d\n", runtime.NumGoroutine())
		fmt.Fprintf(w, "# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.\n")
		fmt.Fprintf(w, "# TYPE go_memstats_alloc_bytes gauge\n")
		fmt.Fprintf(w, "go_memstats_alloc_bytes %d\n", mem.Alloc)
		fmt.Fprintf(w, "# HELP go_memstats_sys_bytes Number of bytes obtained from system.\n")
		fmt.Fprintf(w, "# TYPE go_memstats_sys_bytes gauge\n")
		fmt.Fprintf(w, "go_memstats_sys_bytes %d\n", mem.Sys)
		fmt.Fprintf(w, "# HELP go_memstats_heap_inuse_bytes Bytes in use heap spans.\n")
		fmt.Fprintf(w, "# TYPE go_memstats_heap_inuse_bytes gauge\n")
		fmt.Fprintf(w, "go_memstats_heap_inuse_bytes %d\n", mem.HeapInuse)
		fmt.Fprintf(w, "# HELP go_gc_duration_seconds Summary of the GC pause duration.\n")
		fmt.Fprintf(w, "# TYPE go_gc_duration_seconds summary\n")
		fmt.Fprintf(w, "go_gc_duration_seconds{quantile=\"0\"} %f\n", 0.0)
		fmt.Fprintf(w, "go_gc_duration_seconds{quantile=\"1\"} %f\n", gc.PauseTotal.Seconds())
		fmt.Fprintf(w, "go_gc_duration_seconds_sum %f\n", gc.PauseTotal.Seconds())
		fmt.Fprintf(w, "go_gc_duration_seconds_count %d\n", gc.NumGC)

		// Katrix events.
		fmt.Fprintf(w, "# HELP katrix_events_total Total events processed by kind.\n")
		fmt.Fprintf(w, "# TYPE katrix_events_total counter\n")
		fmt.Fprintf(w, "katrix_events_total{kind=\"sent\"} %d\n", Counters.EventsSent.Load())
		fmt.Fprintf(w, "katrix_events_total{kind=\"received\"} %d\n", Counters.EventsReceived.Load())
		fmt.Fprintf(w, "katrix_events_total{kind=\"redacted\"} %d\n", Counters.EventsRedacted.Load())

		// /sync.
		fmt.Fprintf(w, "# HELP katrix_sync_requests_total Total /sync requests.\n")
		fmt.Fprintf(w, "# TYPE katrix_sync_requests_total counter\n")
		fmt.Fprintf(w, "katrix_sync_requests_total %d\n", Counters.SyncRequests.Load())
		fmt.Fprintf(w, "# HELP katrix_sync_active In-flight /sync long-polls.\n")
		fmt.Fprintf(w, "# TYPE katrix_sync_active gauge\n")
		fmt.Fprintf(w, "katrix_sync_active %d\n", Counters.SyncActive.Load())

		// Federation.
		fmt.Fprintf(w, "# HELP katrix_federation_inbound_pdus_total Total inbound federation PDUs.\n")
		fmt.Fprintf(w, "# TYPE katrix_federation_inbound_pdus_total counter\n")
		fmt.Fprintf(w, "katrix_federation_inbound_pdus_total %d\n", Counters.FedInboundPDUs.Load())
		fmt.Fprintf(w, "# HELP katrix_federation_outbound_requests_total Total outbound federation requests.\n")
		fmt.Fprintf(w, "# TYPE katrix_federation_outbound_requests_total counter\n")
		fmt.Fprintf(w, "katrix_federation_outbound_requests_total %d\n", Counters.FedOutboundRequests.Load())

		// Media.
		fmt.Fprintf(w, "# HELP katrix_media_uploads_total Total media uploads.\n")
		fmt.Fprintf(w, "# TYPE katrix_media_uploads_total counter\n")
		fmt.Fprintf(w, "katrix_media_uploads_total %d\n", Counters.MediaUploads.Load())
		fmt.Fprintf(w, "# HELP katrix_media_remote_fetch_total Total remote media fetches.\n")
		fmt.Fprintf(w, "# TYPE katrix_media_remote_fetch_total counter\n")
		fmt.Fprintf(w, "katrix_media_remote_fetch_total %d\n", Counters.MediaRemoteFetch.Load())
		fmt.Fprintf(w, "katrix_media_remote_fetch_errors_total %d\n", Counters.MediaRemoteFetchErrors.Load())
	}
}

// escape backslash-escapes a label value.
func escape(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\\' || c == '"' || c == '\n' {
			out = append(out, '\\')
		}
		out = append(out, c)
	}
	return string(out)
}
