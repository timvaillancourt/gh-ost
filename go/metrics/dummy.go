package metrics

var (
	dummyBinlogsAppliedKey = "binlogs-applied"
	dummyBinlogsReadKey    = "binlogs-read"
	dummyRowsCopiedKey     = "rows-copied"
	dummyETAMilliseconds   = "eta-milliseconds"
	dummyTotalRowsKey      = "total-rows"
)

type DummyHandler struct {
	data map[string]int64
}

func NewDummyHandler() *DummyHandler {
	return &DummyHandler{
		data: make(map[string]int64, 0),
	}
}

func (h *DummyHandler) Close() {}

func (h *DummyHandler) Name() string {
	return "dummy"
}

func (h *DummyHandler) AddBinlogsApplied(delta int64) {
	h.data[dummyBinlogsAppliedKey] += delta
}

func (h *DummyHandler) AddBinlogsRead(delta int64) {
	h.data[dummyBinlogsReadKey] += delta
}

func (h *DummyHandler) AddRowsCopied(delta int64) {
	h.data[dummyRowsCopiedKey] += delta
}

func (h *DummyHandler) SetETAMilliseconds(millis int64) {
	h.data[dummyETAMilliseconds] = millis
}

func (h *DummyHandler) SetTotalRows(rows int64) {
	h.data[dummyTotalRowsKey] = rows
}
