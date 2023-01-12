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

func (hs Handlers) doAll(f func(h Handler)) {
	for _, handler := range hs {
		f(handler)
	}
}

func (hs Handlers) Close() {
	hs.doAll(func(h Handler) { h.Close() })
}

func (hs Handlers) AddBinlogsApplied(delta int64) {
	hs.doAll(func(h Handler) { h.AddBinlogsApplied(delta) })
}

func (hs Handlers) AddBinlogsRead(delta int64) {
	hs.doAll(func(h Handler) { h.AddBinlogsRead(delta) })
}

func (hs Handlers) AddRowsCopied(delta int64) {
	hs.doAll(func(h Handler) { h.AddRowsCopied(delta) })
}

func (hs Handlers) SetETAMilliseconds(millis int64) {
	hs.doAll(func(h Handler) { h.SetETAMilliseconds(millis) })
}

func (hs Handlers) SetTotalRows(rows int64) {
	hs.doAll(func(h Handler) { h.SetTotalRows(rows) })
}
