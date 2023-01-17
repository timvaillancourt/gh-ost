package metrics

const (
	dummyBinlogsAppliedKey = iota
	dummyBinlogsReadKey
	dummyChunkIteration
	dummyRowsCopiedKey
	dummyETAMilliseconds
	dummyTotalRowsKey
)

type DummyHandler struct {
	data map[int]int64
}

func NewDummyHandler() *DummyHandler {
	return &DummyHandler{
		data: make(map[int]int64, 0),
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

func (h *DummyHandler) IncrChunkIteration() {
	h.data[dummyChunkIteration]++
}

func (h *DummyHandler) SetETAMilliseconds(millis int64) {
	h.data[dummyETAMilliseconds] = millis
}

func (h *DummyHandler) SetTotalRows(rows int64) {
	h.data[dummyTotalRowsKey] = rows
}
