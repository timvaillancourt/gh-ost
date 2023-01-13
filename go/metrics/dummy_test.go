package metrics

import (
	"testing"

	test "github.com/openark/golib/tests"
)

func TestMetricsDummyHandler(t *testing.T) {
	dummy := NewDummyHandler()

	// test match for Handler + Handlers interfaces
	var metrics Handlers
	metrics = append(metrics, dummy)

	// test dummy handler gets metric updates
	metrics.AddRowsCopied(12344)
	test.S(t).ExpectEquals(dummy.data[dummyRowsCopiedKey], int64(12344))
	metrics.AddRowsCopied(1)
	test.S(t).ExpectEquals(dummy.data[dummyRowsCopiedKey], int64(12345))
}
