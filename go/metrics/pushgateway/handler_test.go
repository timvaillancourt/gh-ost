package pushgateway

import (
	"os"
	"testing"

	"github.com/github/gh-ost/go/base"
)

func TestMetricsPushgatewayHandler(t *testing.T) {
	if os.Getenv("TEST_METRICS_PUSHGATEWAY") != "true" {
		t.Logf("Skipping pushgateway tests, TEST_METRICS_PUSHGATEWAY is not 'true'")
		return
	}

	migrationContext := base.NewMigrationContext()
	migrationContext.DatabaseName = "test"
	migrationContext.OriginalTableName = t.Name()
	migrationContext.PushgatewayAddress = "127.0.0.1:9091"
	migrationContext.PushgatewayJobName = promNamespace
	migrationContext.PushgatewayTimeoutSec = 1

	pg, _ := NewHandler(migrationContext)
	pg.AddRowsCopied(12345)
	pg.pushCounters()
	//time.Sleep(time.Second * 2)
	// test
}
