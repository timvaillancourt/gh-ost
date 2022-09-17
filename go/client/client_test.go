package client

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	test "github.com/openark/golib/tests"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/logic"
	"github.com/github/gh-ost/go/mysql"
)

func TestBuildClientCommand(t *testing.T) {
	{
		_, err := buildClientCommand("doesnt-exist", nil)
		test.S(t).ExpectNotNil(err)
	}
	{
		cmd, err := buildClientCommand("help", nil)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(cmd, "help")
	}
	{
		cmd, err := buildClientCommand("max-load", nil)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(cmd, "max-load=?")
	}
	{
		cmd, err := buildClientCommand("max-load", "threads_running=20")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(cmd, "max-load=threads_running=20")
	}
}

func TestServerWithClient(t *testing.T) {
	testVersion := "1.2.3"
	migrationContext := base.NewMigrationContext()

	tmpDir, _ := ioutil.TempDir("", t.Name())
	defer os.RemoveAll(tmpDir)

	migrationContext.ServeSocketFile = filepath.Join(tmpDir, "gh-ost.sock")
	var f logic.PrintStatusFunc = func(rule logic.PrintStatusRule, writer io.Writer) {
		return
	}
	server := logic.NewServer(migrationContext, logic.NewHooksExecutor(migrationContext), f, testVersion)

	test.S(t).ExpectNil(server.BindSocketFile())
	defer server.RemoveSocketFile()
	go server.Serve()

	client := New("unix", migrationContext.ServeSocketFile)

	// test 'applier' command
	t.Run("applier", func(t *testing.T) {
		migrationContext.ApplierConnectionConfig = &mysql.ConnectionConfig{
			ImpliedKey: &mysql.InstanceKey{
				Hostname: "test-host",
				Port:     3306,
			},
		}
		migrationContext.ApplierMySQLVersion = "1.2.3"
		applier, err := client.GetApplier()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(applier, "Host: test-host:3306, Version: 1.2.3")
	})

	// test 'dml-batch-size' command
	t.Run("dml-batch-size", func(t *testing.T) {
		batchSize, err := client.GetDMLBatchSize()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(batchSize, base.DefaultDMLBatchSize)

		err = client.SetDMLBatchSize(123)
		test.S(t).ExpectNil(err)

		batchSize, err = client.GetDMLBatchSize()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(batchSize, int64(123))
	})

	// test 'help' command
	t.Run("help", func(t *testing.T) {
		help, err := client.GetHelp()
		test.S(t).ExpectNil(err)

		expected := `applier                                # Print the hostname of the applier
chunk-size=<newsize>                   # Set a new chunk-size (int)
coordinates                            # Print the currently inspected coordinates
critical-load=<load>                   # Set a new set of max-load thresholds (string)
dml-batch-size=<newsize>               # Set a new dml-batch-size (int)
help                                   # Print this message
inspector                              # Print the hostname of the inspector
max-lag-millis=<max-lag>               # Set a new replication lag threshold (int)
max-load=<load>                        # Set a new set of max-load thresholds (string)
nice-ratio=<ratio>                     # Set a new nice-ratio, immediate sleep after each row-copy operation, float (examples: 0 is aggressive, 0.7 adds 70% runtime, 1.0 doubles runtime, 2.0 triples runtime, ...) (float)
no-throttle,continue,resume,unthrottle # End forced throttling (other throttling may still apply)
panic                                  # Panic and quit without cleanup
replication-lag-query=<query>          # (Deprecated) set a new query that determines replication lag without quotes (string)
status,info                            # Print a detailed status message
sup                                    # Print a short status message
throttle,pause,suspend                 # Force throttle
throttle-control-replicas=<replicas>   # Set a new comma delimited list of throttle control replicas (string)
throttle-http=<url>                    # Set a new throttle URL (string)
throttle-query=<query>                 # Set a new throttle-query without quotes (string)
unpostpone,cut-over,no-postpone        # Bail out a cut-over postpone; proceed to cut-over
version                                # Print the gh-ost version
- use '?' (question mark) as argument to get info rather than set. e.g. "max-load=?" will just print out current max-load.`

		t.Logf("help:\n%+v", help)
		test.S(t).ExpectEquals(help, expected)
	})

	// test 'max-lag-millis' command
	t.Run("max-lag-millis", func(t *testing.T) {
		test.S(t).ExpectEquals(
			migrationContext.MaxLagMillisecondsThrottleThreshold,
			base.DefaultMaxLagMillisecondsThrottleThreshold,
		)

		err := client.SetMaxLagMillis(12345)
		test.S(t).ExpectNil(err)

		res, err := client.GetMaxLagMillis()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(res, int64(12345))
	})

	// test 'max-load' command
	t.Run("max-load", func(t *testing.T) {
		maxLoad := migrationContext.GetMaxLoad()
		test.S(t).ExpectTrue(len(maxLoad) == 0)

		err := client.SetMaxLoad("threads_connected=10")
		test.S(t).ExpectNil(err)

		res, err := client.GetMaxLoad()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(res.String(), "threads_connected=10")
	})

	// test 'nice-ratio' command
	t.Run("nice-ratio", func(t *testing.T) {
		test.S(t).ExpectEquals(migrationContext.GetNiceRatio(), 0.00)

		err := client.SetNiceRatio(0.123)
		test.S(t).ExpectNil(err)

		niceRatio, err := client.GetNiceRatio()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(niceRatio, 0.123)
	})

	// test 'panic' command
	t.Run("panic", func(t *testing.T) {
		go func() {
			test.S(t).ExpectNotNil(<-migrationContext.PanicAbort)
		}()
		err := client.SetPanic(t.Name())
		test.S(t).ExpectNil(err)
	})

	// test 'throttle' and 'no-throttle' commands
	t.Run("throttle+no-throttle", func(t *testing.T) {
		err := client.Throttle()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(migrationContext.ThrottleCommandedByUser, int64(1))

		err = client.NoThrottle()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(migrationContext.ThrottleCommandedByUser, int64(0))
	})

	// test 'throttle-control-replicas' command
	t.Run("throttle-control-replicas", func(t *testing.T) {
		replicas, err := client.GetThrottleControlReplicas()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(len(replicas) == 0)

		replicasMap := mysql.NewInstanceKeyMap()
		err = replicasMap.ReadCommaDelimitedList("mysql2:3307,mysql1:3306,mysql2:3306")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectNil(client.SetThrottleControlReplicas(replicasMap))

		replicas, err = client.GetThrottleControlReplicas()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(replicas.ToCommaDelimitedList(), "mysql1:3306,mysql2:3306,mysql2:3307") // expect sorted list
	})

	// test 'throttle-query' command
	t.Run("throttle-query", func(t *testing.T) {
		test.S(t).ExpectEquals(migrationContext.GetThrottleQuery(), "")

		err := client.SetThrottleQuery(t.Name())
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(migrationContext.GetThrottleQuery(), t.Name())

		query, err := client.GetThrottleQuery()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(query, t.Name())
	})

	// test 'version' command
	t.Run("version", func(t *testing.T) {
		version, err := client.GetVersion()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(version, fmt.Sprintf("gh-ost version: %s", testVersion))
	})
}
