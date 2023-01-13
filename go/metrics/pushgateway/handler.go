package pushgateway

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/openark/golib/log"
	"github.com/prometheus/client_golang/prometheus"
	prompush "github.com/prometheus/client_golang/prometheus/push"

	"github.com/github/gh-ost/go/base"
)

var (
	promNamespace  = "gh_ost"
	rowsCopiedOpts = prometheus.CounterOpts{
		Namespace: promNamespace,
		Name:      "rows_copied_total",
		Help:      "The total number of rows copied by the migration",
	}
)

type Handler struct {
	counters         map[string]prometheus.Counter
	migrationContext *base.MigrationContext
	pusher           *prompush.Pusher
	stop             chan bool
	wg               sync.WaitGroup
}

func NewHandler(migrationContext *base.MigrationContext) (*Handler, error) {
	if migrationContext.PushgatewayAddress == "" {
		return nil, fmt.Errorf("--metrics-pushgateway-address must be defined")
	}

	h := &Handler{
		migrationContext: migrationContext,
		counters: map[string]prometheus.Counter{
			rowsCopiedOpts.Name: prometheus.NewCounter(rowsCopiedOpts),
		},
		pusher: prompush.New(
			migrationContext.PushgatewayAddress,
			migrationContext.PushgatewayJobName,
		),
		stop: make(chan bool, 1),
	}
	go h.startMetricsPusher()

	return h, nil
}

func (h *Handler) push(collector prometheus.Collector) error {
	timeout := time.Duration(h.migrationContext.PushgatewayTimeoutSec) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return h.pusher.Collector(collector).
		Grouping("instance", h.migrationContext.Hostname).
		Grouping("uuid", h.migrationContext.Uuid).
		Grouping("database", h.migrationContext.DatabaseName).
		Grouping("table", h.migrationContext.OriginalTableName).
		PushContext(ctx)
}

func (h *Handler) pushCounters() {
	for _, counter := range h.counters {
		if err := h.push(counter); err != nil {
			log.Errorf("Failed to push to Prometheus Pushgateway, skipping push interval: %+v", err)
			return
		}
	}
}

func (h *Handler) startMetricsPusher() {
	if h.migrationContext.PushgatewayIntervalSec == 0 {
		return
	}

	h.wg.Add(1)
	log.Info("Started Prometheus Pushgateway metrics pusher")

	interval := time.Duration(h.migrationContext.PushgatewayIntervalSec) * time.Second
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		h.wg.Done()
	}()

	for {
		select {
		case <-h.stop:
			log.Info("Stopping Prometheus Pushgateway metrics pusher")
			return
		case <-ticker.C:
			h.pushCounters()
		}
	}
}

func (h *Handler) Close() {
	h.stop <- true
	h.wg.Wait()
}

func (h *Handler) Name() string {
	return "pushgateway"
}

func (h *Handler) AddBinlogsApplied(delta int64) {
	// TODO: send metrics
}

func (h *Handler) AddBinlogsRead(delta int64) {
	// TODO: send metrics
}

func (h *Handler) AddRowsCopied(delta int64) {
	h.counters[rowsCopiedOpts.Name].Add(float64(delta))
}

func (h *Handler) IncrChunkIteration() {
	// TODO: send metrics
}

func (h *Handler) SetETAMilliseconds(millis int64) {
	// TODO: send metrics
}

func (h *Handler) SetTotalRows(rows int64) {
	// TODO: send metrics
}
