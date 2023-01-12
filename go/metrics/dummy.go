package metrics

var (
	dummyBinlogsAppliedKey = "binlogs-applied"
	dummyBinlogsReadKey    = "binlogs-read"
	dummyRowsCopiedKey     = "rows-copied"
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

func (h *DummyHandler) Name() string {
	return "dummy"
}

func (h *DummyHandler) IncrBinlogsApplied() {
	h.data[dummyBinlogsAppliedKey] += 1
}

func (h *DummyHandler) IncrBinlogsRead() {
	h.data[dummyBinlogsReadKey] += 1
}

func (h *DummyHandler) IncrRowsCopied() {
	h.data[dummyRowsCopiedKey] += 1
}

func (h *DummyHandler) SetTotalRows(rows int64) {
	h.data[dummyTotalRowsKey] = rows
}
