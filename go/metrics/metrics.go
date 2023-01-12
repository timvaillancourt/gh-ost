package metrics

import (
	"fmt"
	"strings"

	"github.com/github/gh-ost/go/base"
)

var handlers []MetricsHandler

func init() {
	handlers = make([]MetricsHandler, 0)
}

type MetricsHandler interface {
	IncrBinlogsApplied()
	IncrBinlogsRead()
	IncrRowsCopied()
	SetETAMilliseconds(millis int64)
	SetTotalRows(rows int64)
}

func RegisterHandlers(migrationContext *base.MigrationContext) error {
	metricsHandlers := strings.TrimSpace(migrationContext.MetricsHandlers)
	for _, handlerType := range strings.Split(metricsHandlers, ",") {
		switch handlerType {
		case "pushgateway":
			handlers = append(handlers, NewPushgatewayHandler(migrationContext))
		default:
			return fmt.Errorf("unsupported metrics handler: %+v", handlerType)
		}
	}
	return nil
}

func doAllHandlers(f func(h MetricsHandler)) {
	for _, handler := range handlers {
		f(handler)
	}
}

func IncrBinlogsApplied() {
	doAllHandlers(func(h MetricsHandler) { h.IncrBinlogsApplied() })
}

func IncrBinlogsRead() {
	doAllHandlers(func(h MetricsHandler) { h.IncrBinlogsRead() })
}

func IncrRowsCopied() {
	doAllHandlers(func(h MetricsHandler) { h.IncrRowsCopied() })
}

func SetETAMilliseconds(millis int64) {
	doAllHandlers(func(h MetricsHandler) { h.SetETAMilliseconds(millis) })
}

func SetTotalRows(rows int64) {
	doAllHandlers(func(h MetricsHandler) { h.SetTotalRows(rows) })
}
