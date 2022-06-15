package main

import (
	"fmt"
	"net/url"
	"path/filepath"
	"syscall"

	"github.com/openark/golib/log"
	"github.com/urfave/cli/v2"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/logic"
	"github.com/github/gh-ost/go/sql"
)

func buildGhostMigrateFlags(migrationContext *base.MigrationContext) []cli.Flag {
	return []cli.Flag{
		// required flags
		&cli.StringFlag{
			Name:        "database",
			Usage:       "database name (mandatory)",
			Required:    true,
			Destination: &migrationContext.DatabaseName,
		},
		&cli.StringFlag{
			Name:        "table",
			Usage:       "table name (mandatory)",
			Required:    true,
			Destination: &migrationContext.OriginalTableName,
		},
		&cli.StringFlag{
			Name:        "alter",
			Usage:       "table sql statement (mandatory)",
			Required:    true,
			Destination: &migrationContext.AlterStatement,
		},

		// main flags
		&cli.StringFlag{
			Name:  "cut-over",
			Value: "atomic",
			Usage: "choose cut-over type (default|atomic, two-step)",
		},
		&cli.BoolFlag{
			Name:  "execute",
			Usage: "actually execute the alter & migrate the table. Default is noop: do some tests and exit",
		},

		// non-required
		&cli.StringFlag{
			Name:        "host",
			Value:       "127.0.0.1",
			Usage:       "MySQL hostname (preferably a replica, not the master)",
			Destination: &migrationContext.InspectorConnectionConfig.Key.Hostname,
		},
		&cli.StringFlag{
			Name:        "assume-primary-host",
			Aliases:     []string{"assume-master-host"}, // TODO: deprecate
			Usage:       "(optional) explicitly tell gh-ost the identity of the primary. Format: some.host.com[:port] This is useful in primary-primary setups where you wish to pick an explicit primary, or in a tungsten-replicator where gh-ost is unable to determine the primary",
			Destination: &migrationContext.AssumeMasterHostname,
		},
		&cli.IntFlag{
			Name:        "port",
			Value:       3306,
			Usage:       "MySQL port (preferably a replica, not the master)",
			Destination: &migrationContext.InspectorConnectionConfig.Key.Port,
		},
		&cli.Float64Flag{
			Name:        "mysql-timeout",
			Value:       0.0,
			Usage:       "Connect, read and write timeout for MySQL",
			Destination: &migrationContext.InspectorConnectionConfig.Timeout,
		},
		&cli.StringFlag{
			Name:        "user",
			Usage:       "MySQL user",
			Destination: &migrationContext.CliUser,
		},
		&cli.StringFlag{
			Name:        "password",
			Usage:       "MySQL password",
			Destination: &migrationContext.CliPassword,
		},
		&cli.BoolFlag{
			Name:    "ask-password",
			Aliases: []string{"ask-pass"},
			Usage:   "prompt for MySQL password",
		},
		&cli.StringFlag{
			Name:        "primary-user",
			Aliases:     []string{"master-user"}, // TODO: deprecate
			Usage:       "MySQL user on primary, if different from that on replica. Requires --assume-primary-host",
			Destination: &migrationContext.CliMasterUser,
		},
		&cli.StringFlag{
			Name:        "primary-password",
			Aliases:     []string{"master-password"}, // TODO: deprecate
			Usage:       "MySQL password on primary, if different from that on replica. Requires --assume-primary-host",
			Destination: &migrationContext.CliMasterPassword,
		},
		&cli.BoolFlag{
			Name:        "ssl",
			Aliases:     []string{"tls"},
			Usage:       "Enable SSL encrypted connections to MySQL hosts",
			Destination: &migrationContext.UseTLS,
		},
		&cli.StringFlag{
			Name:        "ssl-ca",
			Aliases:     []string{"tls-ca"},
			Usage:       "CA certificate in PEM format for TLS connections to MySQL hosts. Requires --ssl",
			Destination: &migrationContext.TLSCACertificate,
		},
		&cli.StringFlag{
			Name:        "ssl-cert",
			Aliases:     []string{"tls-cert"},
			Usage:       "Certificate in PEM format for TLS connections to MySQL hosts. Requires --ssl",
			Destination: &migrationContext.TLSCertificate,
		},
		&cli.StringFlag{
			Name:        "ssl-key",
			Aliases:     []string{"tls-key"},
			Usage:       "Key in PEM format for TLS connections to MySQL hosts. Requires --ssl",
			Destination: &migrationContext.TLSKey,
		},
		&cli.BoolFlag{
			Name:        "ssl-allow-insecure",
			Aliases:     []string{"tls-allow-insecure"},
			Usage:       "Skips verification of MySQL hosts' certificate chain and host name. Requires --ssl",
			Destination: &migrationContext.TLSAllowInsecure,
		},
		&cli.BoolFlag{
			Name:        "exact-rowcount",
			Usage:       "actually count table rows as opposed to estimate them (results in more accurate progress estimation)",
			Destination: &migrationContext.CountTableRows,
		},
		&cli.BoolFlag{
			Name:        "concurrent-rowcount",
			Value:       true,
			Usage:       "(with --exact-rowcount), when true (default): count rows after row-copy begins, concurrently, and adjust row estimate later on; when false: first count rows, then start row copy",
			Destination: &migrationContext.CountTableRows,
		},
		&cli.BoolFlag{
			Name:        "allow-on-primary",
			Aliases:     []string{"allow-on-master"}, // TODO: deprecate
			Usage:       "allow this migration to run directly on a primary. Preferably it would run on a replica",
			Destination: &migrationContext.AllowedRunningOnMaster,
		},
		&cli.BoolFlag{
			Name:        "allow-primary-primary",
			Aliases:     []string{"allow-master-master"}, // TODO: deprecate
			Usage:       "explicitly allow running in a primary-primary replication setup",
			Destination: &migrationContext.AllowedMasterMaster,
		},
		&cli.BoolFlag{
			Name:        "allow-nullable-unique-key",
			Usage:       "allow gh-ost to migrate based on a unique key with nullable columns. As long as no NULL values exist, this should be OK. If NULL values exist in chosen key, data may be corrupted. Use at your own risk!",
			Destination: &migrationContext.NullableUniqueKeyAllowed,
		},
		&cli.BoolFlag{
			Name:        "approve-renamed-columns",
			Usage:       "in case your `ALTER` statement renames columns, gh-ost will note that and offer its interpretation of the rename. By default gh-ost does not proceed to execute. This flag approves that gh-ost's interpretation is correct",
			Destination: &migrationContext.ApproveRenamedColumns,
		},
		&cli.BoolFlag{
			Name:        "skip-renamed-columns",
			Usage:       "in case your `ALTER` statement renames columns, gh-ost will note that and offer its interpretation of the rename. By default gh-ost does not proceed to execute. This flag tells gh-ost to skip the renamed columns, i.e. to treat what gh-ost thinks are renamed columns as unrelated columns. NOTE: you may lose column data",
			Destination: &migrationContext.SkipRenamedColumns,
		},
		&cli.BoolFlag{
			Name:        "tungsten",
			Usage:       "explicitly let gh-ost know that you are running on a tungsten-replication based topology (you are likely to also provide --assume-primary-host)",
			Destination: &migrationContext.IsTungsten,
		},
		&cli.BoolFlag{
			Name:        "discard-foreign-keys",
			Usage:       "DANGER! This flag will migrate a table that has foreign keys and will NOT create foreign keys on the ghost table, thus your altered table will have NO foreign keys. This is useful for intentional dropping of foreign keys",
			Destination: &migrationContext.DiscardForeignKeys,
		},
		&cli.BoolFlag{
			Name:        "skip-foreign-key-checks",
			Usage:       "set to 'true' when you know for certain there are no foreign keys on your table, and wish to skip the time it takes for gh-ost to verify that",
			Destination: &migrationContext.SkipForeignKeyChecks,
		},
		&cli.BoolFlag{
			Name:        "skip-strict-mode",
			Usage:       "explicitly tell gh-ost binlog applier not to enforce strict sql mode",
			Destination: &migrationContext.SkipStrictMode,
		},
		&cli.BoolFlag{
			Name:        "aliyun-rds",
			Usage:       "set to 'true' when you execute on Aliyun RDS.",
			Destination: &migrationContext.AliyunRDS,
		},
		&cli.StringFlag{
			Name:        "serve-socket-dir",
			Value:       "/tmp",
			Usage:       "TBD",
			Destination: &migrationContext.ServeSocketDir,
		},
		&cli.IntFlag{
			Name:  "exponential-backoff-max-interval",
			Value: 64,
			Usage: "Maximum number of seconds to wait between attempts when performing various operations with exponential backoff.",
		},
		&cli.IntFlag{
			Name:  "chunk-size",
			Value: 1000,
			Usage: "Number of rows to handle in each iteration (allowed range: 10-100,000)",
		},
		&cli.IntFlag{
			Name:  "dml-batch-size",
			Value: 10,
			Usage: "Number of DML events to apply in a single transaction (range 1-100)",
		},
		&cli.IntFlag{
			Name:  "default-retries",
			Value: 60,
			Usage: "Default number of retries for various operations before panicking",
		},
		&cli.IntFlag{
			Name:  "cut-over-lock-timeout-seconds",
			Value: 3,
			Usage: "Max number of seconds to hold locks on tables while attempting to cut-over (retry attempted when lock exceeds timeout)",
		},
		&cli.Float64Flag{
			Name:  "nice-ratio",
			Value: 0,
			Usage: "force being 'nice', imply sleep time per chunk time; range: [0.0..100.0]. Example values: 0 is aggressive. 1: for every 1ms spent copying rows, sleep additional 1ms (effectively doubling runtime); 0.7: for every 10ms spend in a rowcopy chunk, spend 7ms sleeping immediately after",
		},
		&cli.StringFlag{
			Name:  "throttle-control-replicas",
			Usage: "List of replicas on which to check for lag; comma delimited. Example: myhost1.com:3306,myhost2.com,myhost3.com:3307",
		},
		&cli.StringFlag{
			Name:  "throttle-query",
			Usage: "when given, issued (every second) to check if operation should throttle. Expecting to return zero for no-throttle, >0 for throttle. Query is issued on the migrated server. Make sure this query is lightweight",
		},
		&cli.StringFlag{
			Name:  "throttle-http",
			Usage: "when given, gh-ost checks given URL via HEAD request; any response code other than 200 (OK) causes throttling; make sure it has low latency response",
		},
		&cli.BoolFlag{
			Name:  "ignore-http-errors",
			Usage: "ignore HTTP connection errors during throttle check",
		},
		&cli.IntFlag{
			Name:  "heartbeat-interval-millis",
			Value: 100,
			Usage: "how frequently would gh-ost inject a heartbeat value",
		},
		&cli.IntFlag{
			Name:  "max-lag-millis",
			Value: 1500,
			Usage: "replication lag at which to throttle operation",
		},
		&cli.StringFlag{
			Name:  "max-load",
			Usage: "Comma delimited status-name=threshold. e.g: 'Threads_running=100,Threads_connected=500'. When status exceeds threshold, app throttles writes",
		},
		&cli.StringFlag{
			Name:  "critical-load",
			Usage: "Comma delimited status-name=threshold, same format as --max-load. When status exceeds threshold, app panics and quits",
		},
	}
}

func runGhostMigrate(c *cli.Context, migrationContext *base.MigrationContext) error {
	migrationContext.Log.SetLevel(log.ERROR)
	if c.IsSet("verbose") {
		migrationContext.Log.SetLevel(log.INFO)
	}
	if c.IsSet("debug") {
		migrationContext.Log.SetLevel(log.DEBUG)
	}
	if c.IsSet("stack") {
		migrationContext.Log.SetPrintStackTrace(true)
	}
	if c.IsSet("quiet") {
		// Override!!
		migrationContext.Log.SetLevel(log.ERROR)
	}

	if migrationContext.AlterStatement == "" {
		log.Fatalf("--alter must be provided and statement must not be empty")
	}
	parser := sql.NewParserFromAlterStatement(migrationContext.AlterStatement)
	migrationContext.AlterStatementOptions = parser.GetAlterStatementOptions()

	if migrationContext.DatabaseName == "" {
		if parser.HasExplicitSchema() {
			migrationContext.DatabaseName = parser.GetExplicitSchema()
		} else {
			log.Fatalf("--database must be provided and database name must not be empty, or --alter must specify database name")
		}
	}
	migrationContext.DatabaseName = url.QueryEscape(migrationContext.DatabaseName)

	if migrationContext.OriginalTableName == "" {
		if parser.HasExplicitTable() {
			migrationContext.OriginalTableName = parser.GetExplicitTable()
		} else {
			log.Fatalf("--table must be provided and table name must not be empty, or --alter must specify table name")
		}
	}

	migrationContext.Noop = !c.Bool("execute")
	if migrationContext.AllowedRunningOnMaster && migrationContext.TestOnReplica {
		migrationContext.Log.Fatalf("--allow-on-primary and --test-on-replica are mutually exclusive")
	}
	if migrationContext.AllowedRunningOnMaster && migrationContext.MigrateOnReplica {
		migrationContext.Log.Fatalf("--allow-on-primary and --migrate-on-replica are mutually exclusive")
	}
	if migrationContext.MigrateOnReplica && migrationContext.TestOnReplica {
		migrationContext.Log.Fatalf("--migrate-on-replica and --test-on-replica are mutually exclusive")
	}
	if migrationContext.SwitchToRowBinlogFormat && migrationContext.AssumeRBR {
		migrationContext.Log.Fatalf("--switch-to-rbr and --assume-rbr are mutually exclusive")
	}
	if migrationContext.TestOnReplicaSkipReplicaStop {
		if !migrationContext.TestOnReplica {
			migrationContext.Log.Fatalf("--test-on-replica-skip-replica-stop requires --test-on-replica to be enabled")
		}
		migrationContext.Log.Warning("--test-on-replica-skip-replica-stop enabled. We will not stop replication before cut-over. Ensure you have a plugin that does this.")
	}
	if migrationContext.CliMasterUser != "" && migrationContext.AssumeMasterHostname == "" {
		migrationContext.Log.Fatalf("--master-user requires --assume-primary-host")
	}
	if migrationContext.CliMasterPassword != "" && migrationContext.AssumeMasterHostname == "" {
		migrationContext.Log.Fatalf("--master-password requires --assume-primary-host")
	}
	if migrationContext.TLSCACertificate != "" && !migrationContext.UseTLS {
		migrationContext.Log.Fatalf("--ssl-ca requires --ssl")
	}
	if migrationContext.TLSCertificate != "" && !migrationContext.UseTLS {
		migrationContext.Log.Fatalf("--ssl-cert requires --ssl")
	}
	if migrationContext.TLSKey != "" && !migrationContext.UseTLS {
		migrationContext.Log.Fatalf("--ssl-key requires --ssl")
	}
	if migrationContext.TLSAllowInsecure && !migrationContext.UseTLS {
		migrationContext.Log.Fatalf("--ssl-allow-insecure requires --ssl")
	}

	switch c.String("cut-over") {
	case "atomic", "default", "":
		migrationContext.CutOverType = base.CutOverAtomic
	case "two-step":
		migrationContext.CutOverType = base.CutOverTwoStep
	default:
		migrationContext.Log.Fatalf("Unknown cut-over: %s", c.String("cut-over"))
	}

	if err := migrationContext.ReadConfigFile(); err != nil {
		migrationContext.Log.Fatale(err)
	}
	if err := migrationContext.ReadThrottleControlReplicaKeys(c.String("throttle-control-replicas")); err != nil {
		migrationContext.Log.Fatale(err)
	}
	if err := migrationContext.ReadMaxLoad(c.String("max-load")); err != nil {
		migrationContext.Log.Fatale(err)
	}
	if err := migrationContext.ReadCriticalLoad(c.String("critical-load")); err != nil {
		migrationContext.Log.Fatale(err)
	}
	if migrationContext.ServeSocketFile == "" {
		migrationContext.ServeSocketFile = filepath.Join(
			migrationContext.ServeSocketDir,
			fmt.Sprintf("gh-ost.%s.%s.sock", migrationContext.DatabaseName, migrationContext.OriginalTableName),
		)
	}

	if c.Bool("ask-password") {
		fmt.Println("Password:")
		bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			migrationContext.Log.Fatale(err)
		}
		migrationContext.CliPassword = string(bytePassword)
	}

	migrationContext.SetHeartbeatIntervalMilliseconds(c.Int64("heartbeat-interval-millis"))
	migrationContext.SetNiceRatio(c.Float64("nice-ratio"))
	migrationContext.SetChunkSize(c.Int64("chunk-size"))
	migrationContext.SetDMLBatchSize(c.Int64("dml-batch-size"))
	migrationContext.SetMaxLagMillisecondsThrottleThreshold(c.Int64("max-lag-millis"))
	migrationContext.SetThrottleQuery(c.String("throttle-query"))
	migrationContext.SetThrottleHTTP(c.String("throttle-http"))
	migrationContext.SetIgnoreHTTPErrors(c.Bool("ignore-http-errors"))
	migrationContext.SetDefaultNumRetries(c.Int64("default-retries"))
	migrationContext.ApplyCredentials()
	if err := migrationContext.SetupTLS(); err != nil {
		migrationContext.Log.Fatale(err)
	}
	if err := migrationContext.SetCutOverLockTimeoutSeconds(c.Int64("cut-over-lock-timeout-seconds")); err != nil {
		migrationContext.Log.Errore(err)
	}
	if err := migrationContext.SetExponentialBackoffMaxInterval(c.Int64("exponential-backoff-max-interval")); err != nil {
		migrationContext.Log.Errore(err)
	}

	log.Infof("starting gh-ost %+v", AppVersion)
	acceptSignals(migrationContext)

	migrator := logic.NewMigrator(migrationContext)
	if err := migrator.Migrate(); err != nil {
		migrator.ExecOnFailureHook()
		migrationContext.Log.Fatale(err)
	}

	log.Info("Done")
	return nil
}

func buildGhostMigrateCommand() *cli.Command {
	migrationContext := base.NewMigrationContext()
	return &cli.Command{
		Name:  "migrate",
		Usage: "Run a migration",
		Flags: buildGhostMigrateFlags(migrationContext),
		Action: func(c *cli.Context) error {
			return runGhostMigrate(c, migrationContext)
		},
	}
}
