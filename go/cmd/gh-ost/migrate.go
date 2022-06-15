package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"syscall"

	"github.com/openark/golib/log"
	"github.com/urfave/cli/v2"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/logic"
	"github.com/github/gh-ost/go/sql"
)

func setupGhostMigrateCommand() *cli.Command {
	var askPass bool
	migrationContext := base.NewMigrationContext()

	return &cli.Command{
		Name:    "migrate",
		Aliases: []string{"run"},
		Usage:   "TBD",
		Flags: []cli.Flag{
			// required
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
				Name:        "ask-password",
				Aliases:     []string{"ask-pass"},
				Usage:       "prompt for MySQL password",
				Destination: &askPass,
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
		},
		Action: func(c *cli.Context) error {
			return handleGhostMigrateCommand(c, migrationContext)
		},
	}
}

func handleGhostMigrateCommand(c *cli.Context, migrationContext *base.MigrationContext) error {
	migrationContext.Log.SetLevel(log.ERROR)
	if c.IsSet("verbose") {
		migrationContext.Log.SetLevel(log.INFO)
	}
	if c.IsSet("debug") {
		migrationContext.Log.SetLevel(log.DEBUG)
	}
	if c.IsSet("stack") {
		migrationContext.Log.SetPrintStackTrace(c.Bool("stack"))
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

	if err := flag.Set("database", url.QueryEscape(migrationContext.DatabaseName)); err != nil {
		migrationContext.Log.Fatale(err)
	}

	if migrationContext.OriginalTableName == "" {
		if parser.HasExplicitTable() {
			migrationContext.OriginalTableName = parser.GetExplicitTable()
		} else {
			log.Fatalf("--table must be provided and table name must not be empty, or --alter must specify table name")
		}
	}
	migrationContext.Noop = !c.Bool("execute")
	if migrationContext.AllowedRunningOnMaster && migrationContext.TestOnReplica {
		migrationContext.Log.Fatalf("--allow-on-master and --test-on-replica are mutually exclusive")
	}
	if migrationContext.AllowedRunningOnMaster && migrationContext.MigrateOnReplica {
		migrationContext.Log.Fatalf("--allow-on-master and --migrate-on-replica are mutually exclusive")
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
		migrationContext.Log.Fatalf("--master-user requires --assume-master-host")
	}
	if migrationContext.CliMasterPassword != "" && migrationContext.AssumeMasterHostname == "" {
		migrationContext.Log.Fatalf("--master-password requires --assume-master-host")
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

	switch *cutOver {
	case "atomic", "default", "":
		migrationContext.CutOverType = base.CutOverAtomic
	case "two-step":
		migrationContext.CutOverType = base.CutOverTwoStep
	default:
		migrationContext.Log.Fatalf("Unknown cut-over: %s", *cutOver)
	}
	if err := migrationContext.ReadConfigFile(); err != nil {
		migrationContext.Log.Fatale(err)
	}
	if err := migrationContext.ReadThrottleControlReplicaKeys(*throttleControlReplicas); err != nil {
		migrationContext.Log.Fatale(err)
	}
	if err := migrationContext.ReadMaxLoad(*maxLoad); err != nil {
		migrationContext.Log.Fatale(err)
	}
	if err := migrationContext.ReadCriticalLoad(*criticalLoad); err != nil {
		migrationContext.Log.Fatale(err)
	}
	if migrationContext.ServeSocketFile == "" {
		migrationContext.ServeSocketFile = fmt.Sprintf("/tmp/gh-ost.%s.%s.sock", migrationContext.DatabaseName, migrationContext.OriginalTableName)
	}
	if askPass {
		fmt.Println("Password:")
		bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			migrationContext.Log.Fatale(err)
		}
		migrationContext.CliPassword = string(bytePassword)
	}
	migrationContext.SetHeartbeatIntervalMilliseconds(*heartbeatIntervalMillis)
	migrationContext.SetNiceRatio(*niceRatio)
	migrationContext.SetChunkSize(*chunkSize)
	migrationContext.SetDMLBatchSize(*dmlBatchSize)
	migrationContext.SetMaxLagMillisecondsThrottleThreshold(*maxLagMillis)
	migrationContext.SetThrottleQuery(*throttleQuery)
	migrationContext.SetThrottleHTTP(*throttleHTTP)
	migrationContext.SetIgnoreHTTPErrors(*ignoreHTTPErrors)
	migrationContext.SetDefaultNumRetries(*defaultRetries)
	migrationContext.ApplyCredentials()
	if err := migrationContext.SetupTLS(); err != nil {
		migrationContext.Log.Fatale(err)
	}
	if err := migrationContext.SetCutOverLockTimeoutSeconds(*cutOverLockTimeoutSeconds); err != nil {
		migrationContext.Log.Errore(err)
	}
	if err := migrationContext.SetExponentialBackoffMaxInterval(*exponentialBackoffMaxInterval); err != nil {
		migrationContext.Log.Errore(err)
	}

	log.Infof("starting gh-ost %+v", AppVersion)
	acceptSignals(migrationContext)

	migrator := logic.NewMigrator(migrationContext)
	if err = migrator.Migrate(); err != nil {
		migrator.ExecOnFailureHook()
		migrationContext.Log.Fatale(err)
	}
	fmt.Fprintf(os.Stdout, "# Done\n")
	return nil
}
