/*
   Copyright 2024 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

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
	maxTempDNSFailures int64 = 25
	watchdogInterval         = time.Second * 15
)

type dbProvider interface {
	Name() string
	DB() *gosql.DB
	ServerInfo() *mysql.ServerInfo
	Teardown()
}

func getDBProviderServerInfo(provider dbProvider) (*mysql.ServerInfo, error) {
	return mysql.GetServerInfo(provider.DB())
}

type Watchdog struct {
	dbProviders        []dbProvider
	migrationContext   *base.MigrationContext
	serverInfoProvider func(dbProvider) (*mysql.ServerInfo, error)
	done               chan bool
	tempDNSFailures    map[string]int64
}

func NewWatchdog(migrator *Migrator) *Watchdog {
	return &Watchdog{
		dbProviders: []dbProvider{
			migrator.inspector,
			migrator.applier,
		},
		migrationContext:   migrator.migrationContext,
		serverInfoProvider: getDBProviderServerInfo,
		tempDNSFailures:    make(map[string]int64),
	}
}

func (this *Watchdog) Teardown() {
	close(this.done)
	for k := range this.tempDNSFailures {
		delete(this.tempDNSFailures, k)
	}
}

func (this *Watchdog) checkDBProvider(provider dbProvider) error {
	providerTempDNSFailures := this.tempDNSFailures[provider.Name()]
	origServerInfo := provider.ServerInfo()
	runtimeServerInfo, err := this.serverInfoProvider(provider)
	if err != nil {
		switch e := err.(type) {
		case *net.DNSError:
			// ignore transient *net.DNSError, unless a limit is reached consequtively
			// fail on "no such host" errors
			if atomic.LoadInt64(&providerTempDNSFailures) > maxTempDNSFailures {
				log.Errorf("%s watchdog reached max temporary DNS failures (%d)", provider.Name(), maxTempDNSFailures)
				return ErrWatchdogTempDNSFailuresExceeded
			} else if e.IsTemporary {
				// return nil with the assumption another check will occur
				log.Warningf("%s watchdog ignoring temporary DNS failure: %+v", provider.Name(), e.Err)
				atomic.AddInt64(&providerTempDNSFailures, 1)
				return nil
			} else if e.IsNotFound {
				log.Errorf("%s watchdog got DNS error %q for %q, assuming host is gone", provider.Name(), e.Err, e.Name)
				return ErrWatchdogUnexpectedChange
			}
		case *net.OpError:
			// assume *net.OpError errors are handled by something else, don't panic
			log.Warningf("%s watchdog ignoring possibly-transient network error: %+v", provider.Name(), err)
			return nil
		default:
			log.Errorf("%s watchdog check failed: %+v", provider.Name(), err)
			return ErrWatchdogCheckFailed
		}
	}

	// check runtime config matches initial state
	if !origServerInfo.Equals(runtimeServerInfo) {
		log.Errorf("%s watchdog found unexpected runtime change from:\n    %s\nto\n    %s", provider.Name(), origServerInfo, runtimeServerInfo)
		return ErrWatchdogUnexpectedChange
	}
	atomic.StoreInt64(&providerTempDNSFailures, 0)
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
