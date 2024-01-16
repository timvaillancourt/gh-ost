package logic

import (
	gosql "database/sql"
	"errors"
	"time"

	"github.com/openark/golib/log"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
)

var (
	ErrWatchdogCheckFailed      = errors.New("watchdog check failed")
	ErrWatchdogUnexpectedChange = errors.New("watchdog detected unexpected change")
	watchdogInterval            = time.Second * 10
)

type dbProvider interface {
	Name() string
	DB() *gosql.DB
	ServerInfo() *mysql.ServerInfo
}

func getDBProviderServerInfo(provider dbProvider) (*mysql.ServerInfo, error) {
	return mysql.GetServerInfo(provider.DB())
}

type Watchdog struct {
	dbProviders        []dbProvider
	migrationContext   *base.MigrationContext
	serverInfoProvider func(dbProvider) (*mysql.ServerInfo, error)
	done               chan bool
}

func NewWatchdog(migrator *Migrator) *Watchdog {
	return &Watchdog{
		dbProviders: []dbProvider{
			migrator.inspector,
			migrator.applier,
		},
		migrationContext:   migrator.migrationContext,
		serverInfoProvider: getDBProviderServerInfo,
	}
}

func (this *Watchdog) Teardown() {
	close(this.done)
}

func (this *Watchdog) checkDBProvider(provider dbProvider) error {
	origServerInfo := provider.ServerInfo()
	serverInfo, err := this.serverInfoProvider(provider)
	if err != nil {
		log.Errorf("watchdog %s check failed: %+v", provider.Name(), err)
		return ErrWatchdogCheckFailed
	}
	if !origServerInfo.Equals(serverInfo) {
		log.Errorf("watchdog detected unexpected %s change from %+v to %+v", provider.Name(), origServerInfo, serverInfo)
		return ErrWatchdogUnexpectedChange
	}
	return nil
}

func (this *Watchdog) InitiateChecker() {
	this.done = make(chan bool)
	ticker := time.NewTicker(watchdogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-this.done:
			return
		case <-ticker.C:
			for _, provider := range this.dbProviders {
				if err := this.checkDBProvider(provider); err != nil {
					this.migrationContext.PanicAbort <- err
				}
			}
		}
	}
}
