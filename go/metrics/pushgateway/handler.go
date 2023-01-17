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

const (
	promNamespace = "gh_ost"
)

type Handler struct {
	migrationContext *base.MigrationContext
	pusher           *prompush.Pusher
	stop             chan bool
	wg               sync.WaitGroup
}

func NewHandler(migrationContext *base.MigrationContext) (*Handler, error) {
	if migrationContext.pushgatewayAddress == "" {
		return nil, fmt.Errorf("--metrics-pushgateway-address must be defined")
	}

	h := &Handler{
		migrationContext: migrationContext,
		pusher: prompush.New(
			migrationContext.pushgatewayAddress,
			migrationContext.pushgatewayJobName,
		),
		stop: make(chan bool, 1),
	}
	go h.startMetricsPusher()

	return h, nil
}

func (h *Handler) registerMetrics() error {
	// rows_copied_total
	h.counterFuncs["rows_copied_total"] = prometheus.CounterFunc(
		prometheus.CounterOpts{
			Namespace: promNamespace,
			Name:      "rows_copied_total",
			Help:      "The total number of rows copied by the migration",
		},
		func() float64 { float64(h.migrationContext.GetTotalRowsCopied()) },
	)

	// rows_total
	h.gaugeFuncs["rows_total"] = prometheus.GaugeFunc(
		prometheus.GaugeOpts{
			Namespace: promNamespace,
			Name:      "rows_total",
			Help:      "The total estimated number of rows in the table",
		},
		func() float64 { float64(h.migrationContext.RowsEstimate) },
	)
}

func (h *Handler) push(collector prometheus.Collector) error {
	timeout := time.Duration(h.migrationContext.pushgatewayTimeoutSec) * time.Second
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
			log.Errorf("Failed to push to Prometheus pushgateway, skipping push interval: %+v", err)
			return
		}
	}
}

func (h *Handler) startMetricsPusher() {
	if h.migrationContext.pushgatewayIntervalSec == 0 {
		return
	}

	h.wg.Add(1)
	log.Info("Started Prometheus pushgateway metrics pusher")

	interval := time.Duration(h.migrationContext.pushgatewayIntervalSec) * time.Second
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		h.wg.Done()
	}()

	for {
		select {
		case <-h.stop:
			log.Info("Stopping Prometheus pushgateway metrics pusher")
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
