package metrics

type Handler interface {
	Close()
	AddBinlogsApplied(delta int64)
	AddBinlogsRead(delta int64)
	AddRowsCopied(delta int64)
	SetETAMilliseconds(millis int64)
	SetTotalRows(rows int64)
}

type Handlers []Handler

// doAll runs a func on all handlers
func (hs Handlers) doAll(f func(h Handler)) {
	for _, handler := range hs {
		f(handler)
	}
}

// Close closes all handlers
func (hs Handlers) Close() {
	hs.doAll(func(h Handler) { h.Close() })
}

// AddBinlogsApplied runs on all handlers
func (hs Handlers) AddBinlogsApplied(delta int64) {
	hs.doAll(func(h Handler) { h.AddBinlogsApplied(delta) })
}

// AddBinlogsRead runs on all handlers
func (hs Handlers) AddBinlogsRead(delta int64) {
	hs.doAll(func(h Handler) { h.AddBinlogsRead(delta) })
}

// AddRowsCopied runs on all handlers
func (hs Handlers) AddRowsCopied(delta int64) {
	hs.doAll(func(h Handler) { h.AddRowsCopied(delta) })
}

// SetETAMilliseconds runs on all handlers
func (hs Handlers) SetETAMilliseconds(millis int64) {
	hs.doAll(func(h Handler) { h.SetETAMilliseconds(millis) })
}

// SetTotalRows runs on all handlers
func (hs Handlers) SetTotalRows(rows int64) {
	hs.doAll(func(h Handler) { h.SetTotalRows(rows) })
}
