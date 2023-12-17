package sql

import (
	"testing"

	test "github.com/openark/golib/tests"
)

func TestOptimizerHintsString(t *testing.T) {
	hints := OptimizerHints{
		ResourceGroup:    "gh-ost",
		MaxExecutionTime: 1000,
	}
	test.S(t).ExpectEquals(hints.String(), `/*+ RESOURCE_GROUP(gh-ost) MAX_EXECUTION_TIME(1000) */`)
}
