package logic

import (
	gosql "database/sql"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/openark/golib/log"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
)

var (
	ErrWatchdogCheckFailed             = errors.New("watchdog check failed")
	ErrWatchdogTempDNSFailuresExceeded = errors.New("watchdog reached max temporary DNS failures")
	ErrWatchdogUnexpectedChange        = errors.New("watchdog detected unexpected change")
	//
	maxTempDNSFailures int64 = 10
	watchdogInterval         = time.Second * 10
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
	tempDNSFailures    map[string]*int64
}

func NewWatchdog(migrator *Migrator) *Watchdog {
	return &Watchdog{
		dbProviders: []dbProvider{
			migrator.inspector,
			migrator.applier,
		},
		migrationContext:   migrator.migrationContext,
		serverInfoProvider: getDBProviderServerInfo,
		tempDNSFailures:    make(map[string]*int64),
	}
}

func (this *Watchdog) Teardown() {
	close(this.done)
	for k := range this.tempDNSFailures {
		delete(this.tempDNSFailures, k)
	}
}

func (this *Watchdog) checkDBProvider(provider dbProvider) error {
	origServerInfo := provider.ServerInfo()
	providerTempDNSFailures := this.tempDNSFailures[provider.Name()]
	serverInfo, err := this.serverInfoProvider(provider)
	if err != nil {
		switch e := err.(type) {
		case *net.DNSError:
			if atomic.LoadInt64(providerTempDNSFailures) > maxTempDNSFailures {
				log.Errorf("%s watchdog reached max temporary DNS failures (%d)", provider.Name(), maxTempDNSFailures)
				return ErrWatchdogTempDNSFailuresExceeded
			} else if e.IsTemporary {
				log.Warningf("%s watchdog ignoring temporary DNS failure: %+v", provider.Name(), e.Err)
				atomic.AddInt64(providerTempDNSFailures, 1)
				return nil
			}
		case *net.OpError:
			log.Warningf("%s watchdog ignoring possibly-transient network error: %+v", provider.Name(), err)
			return nil
		}
		log.Errorf("%s watchdog check failed: %+v", provider.Name(), err)
		return ErrWatchdogCheckFailed
	}
	atomic.StoreInt64(providerTempDNSFailures, 0)
	if !origServerInfo.Equals(serverInfo) {
		log.Errorf("%s watchdog detected unexpected runtime change from %+v to %+v", provider.Name(), origServerInfo, serverInfo)
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
					return
				}
			}
		}
	}
}
