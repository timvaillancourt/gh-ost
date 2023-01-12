package metrics

import (
	"context"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	prompush "github.com/prometheus/client_golang/prometheus/push"

	"github.com/github/gh-ost/go/base"
)

var (
	pushgatewayPromNamespace  = "gh-ost"
	pushgatewayRowsCopiedOpts = prometheus.CounterOpts{
		Namespace: pushgatewayPromNamespace,
		Name:      "rows_copied_total",
		Help:      "The total number of rows copied by the migration",
	}
)

type PushgatewayHandler struct {
	counters         map[string]prometheus.Counter
	migrationContext *base.MigrationContext
	pusher           *prompush.Pusher
}

func NewPushgatewayHandler(migrationContext *base.MigrationContext) *PushgatewayHandler {
	h := &PushgatewayHandler{
		migrationContext: migrationContext,
		counters: map[string]prometheus.Counter{
			pushgatewayRowsCopiedOpts.Name: prometheus.NewCounter(pushgatewayRowsCopiedOpts),
		},
		pusher: prompush.New(
			migrationContext.PushgatewayAddress,
			migrationContext.PushgatewayJobName,
		),
	}
	go h.startPusher()
	return h
}

func (h *PushgatewayHandler) push(collector prometheus.Collector) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second) // TODO: add flag
	defer cancel()
	return h.pusher.Collector(collector).
		Grouping("uuid", h.migrationContext.Uuid).
		Grouping("database", h.migrationContext.DatabaseName).
		Grouping("table", h.migrationContext.OriginalTableName).
		PushContext(ctx)
}

func (h *PushgatewayHandler) startPusher() {
	ticker := time.NewTicker(time.Second * 5) // TODO: add flag
	for {
		select {
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

func (h *PushgatewayHandler) Name() string {
	return "pushgateway"
}

func (h *PushgatewayHandler) IncrBinlogsApplied() {
	// TODO: send metrics
}

func (h *PushgatewayHandler) IncrBinlogsRead() {
	// TODO: send metrics
}

func (h *PushgatewayHandler) IncrRowsCopied() {
	h.counters[pushgatewayRowsCopiedOpts.Name].Inc()
}

func (h *PushgatewayHandler) SetETAMilliseconds(millis int64) {
	// TODO: send metrics
}

func (h *PushgatewayHandler) SetTotalRows(rows int64) {
	// TODO: send metrics
}
