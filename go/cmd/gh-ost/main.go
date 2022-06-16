/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/github/gh-ost/go/base"
	_ "github.com/go-sql-driver/mysql"
	"github.com/openark/golib/log"

	"github.com/urfave/cli/v2"
)

var AppVersion string

// acceptSignals registers for OS signals
func acceptSignals(migrationContext *base.MigrationContext) {
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGHUP:
				migrationContext.Log.Infof("Received SIGHUP. Reloading configuration")
				if err := migrationContext.ReadConfigFile(); err != nil {
					log.Errore(err)
				} else {
					migrationContext.MarkPointOfInterest()
				}
			}
		}
	}()
}

// main is the application's entry point. It will either spawn a CLI or HTTP interfaces.
func main() {
	cli.VersionFlag = &cli.BoolFlag{Name: "version", Aliases: []string{"V"}}
	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Fprintf(c.App.Writer, "gh-ost version %s", c.App.Version)
	}

	ghost := &cli.App{
		Name:    "gh-ost",
		Version: AppVersion,
		Usage: `GitHub's Online Schema Migrations for MySQL
				https://github.com/github/gh-ost`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c", "conf"},
				Usage:   "Config file",
			},
			&cli.BoolFlag{Name: "quiet", Usage: "quiet"},
			&cli.BoolFlag{Name: "verbose", Usage: "verbose"},
			&cli.BoolFlag{Name: "debug", Usage: "debug mode (very verbose)"},
			&cli.BoolFlag{Name: "stack", Usage: "add stack trace upon error"},
		},
		Commands: []*cli.Command{
			buildMigrateCommand(),
			//buildListCommand(),
			//buildPurgeCommand(),
		},
	}

	if err := ghost.Run(os.Args); err != nil {
		log.Fatale(err)
	}
}
