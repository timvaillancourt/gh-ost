package pushgateway

import (
	"context"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	prompush "github.com/prometheus/client_golang/prometheus/push"

	"github.com/github/gh-ost/go/base"
)

var (
	promNamespace  = "gh-ost"
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
}

func NewHandler(migrationContext *base.MigrationContext) *Handler {
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
	go h.startPusher()
	return h
}

func (h *Handler) push(collector prometheus.Collector) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second) // TODO: add flag
	defer cancel()
	return h.pusher.Collector(collector).
		Grouping("uuid", h.migrationContext.Uuid).
		Grouping("database", h.migrationContext.DatabaseName).
		Grouping("table", h.migrationContext.OriginalTableName).
		PushContext(ctx)
}

func (h *Handler) startPusher() {
	log.Printf("Started Prometheus Pushgateway metrics pusher")
	ticker := time.NewTicker(time.Second * 5) // TODO: add flag
	for {
		select {
		case <-h.stop:
			log.Printf("Stopping Prometheus Pushgateway metrics pusher")
			return
		case <-ticker.C:
			for _, counter := range h.counters {
				if err := h.push(counter); err != nil {
					log.Printf("failed to push to Prometheus Pushgateway, skipping push interval: %+v", err)
					return
				}
			}
		}
	}
}

func (h *Handler) Close() {
	h.stop <- true
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

func (h *Handler) SetETAMilliseconds(millis int64) {
	// TODO: send metrics
}

func (h *Handler) SetTotalRows(rows int64) {
	// TODO: send metrics
}
