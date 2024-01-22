package logic

import (
	"net"
	"testing"

	"github.com/openark/golib/tests"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
)

func TestWatchdogCheckDBProvider(t *testing.T) {
	migrationContext := base.NewMigrationContext()
	migrationContext.InspectorServerInfo = &mysql.ServerInfo{
		Hostname:        "inspector",
		Port:            mysql.NewServerPort(3306),
		Version:         "8.9.10",
		VersionComment:  "test",
		LogBin:          true,
		LogSlaveUpdates: true,
	}
	migrationContext.ApplierServerInfo = &mysql.ServerInfo{
		Hostname:        "applier",
		Port:            mysql.NewServerPort(3306),
		Version:         "8.9.10",
		VersionComment:  "test",
		LogBin:          true,
		LogSlaveUpdates: true,
	}

	testCases := []struct {
		name                           string
		returnServerInfoErr            error
		returnServerInfoVersion        string
		expectApplierCheckDBProvider   error
		expectInspectorCheckDBProvider error
		expectApplierTempDNSFailures   int64
		expectInspectorTempDNSFailures int64
		maxTempDNSFailures             int64
	}{
		{
			name:                           "success",
			expectApplierCheckDBProvider:   nil,
			expectInspectorCheckDBProvider: nil,
		},
		/*
			{
				name: "success w temporary DNS failures under limit",
			},
			{
				name: "failed w temporary DNS failures above limit",
			},
		*/
		{
			name:                           "failed runtime config change",
			returnServerInfoVersion:        "1.2.3",
			expectApplierCheckDBProvider:   ErrWatchdogUnexpectedChange,
			expectInspectorCheckDBProvider: ErrWatchdogUnexpectedChange,
		},
		{
			name: "failed no such host",
			returnServerInfoErr: &net.DNSError{
				IsTemporary: false,
				IsNotFound:  true,
				Name:        t.Name(),
				Server:      "1.2.3.4:53",
				Err:         "no such host",
			},
			expectApplierCheckDBProvider:   ErrWatchdogUnexpectedChange,
			expectInspectorCheckDBProvider: ErrWatchdogUnexpectedChange,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			migrator := NewMigrator(migrationContext, "1.2.3")
			migrator.applier = NewApplier(migrationContext)
			migrator.inspector = NewInspector(migrationContext)
			watchdog := NewWatchdog(migrator)

			if testCase.returnServerInfoVersion == "" {
				testCase.returnServerInfoVersion = "8.9.10"
			}

			watchdog.serverInfoProvider = func(provider dbProvider) (*mysql.ServerInfo, error) {
				return &mysql.ServerInfo{
					Hostname:        provider.Name(),
					Port:            mysql.NewServerPort(3306),
					Version:         testCase.returnServerInfoVersion,
					VersionComment:  "test",
					LogBin:          true,
					LogSlaveUpdates: true,
				}, testCase.returnServerInfoErr
			}
			tests.S(t).ExpectEquals(watchdog.checkDBProvider(migrator.inspector), testCase.expectInspectorCheckDBProvider)
			tests.S(t).ExpectEquals(watchdog.checkDBProvider(migrator.applier), testCase.expectApplierCheckDBProvider)
			tests.S(t).ExpectEquals(watchdog.tempDNSFailures["inspector"], testCase.expectInspectorTempDNSFailures)
			tests.S(t).ExpectEquals(watchdog.tempDNSFailures["applier"], testCase.expectApplierTempDNSFailures)
		})
	}

	/*
		// success: temporary dns error with < maxTempDNSFailures (ignored)
		{
			watchdog.serverInfoProvider = func(provider dbProvider) (*mysql.ServerInfo, error) {
				return &mysql.ServerInfo{Hostname: provider.Name()}, &net.DNSError{
					IsTemporary: true,
					Err:         "test",
				}
			}
			tests.S(t).ExpectEquals(watchdog.checkDBProvider(migrator.inspector), nil)
			tests.S(t).ExpectEquals(watchdog.checkDBProvider(migrator.applier), nil)
			tests.S(t).ExpectEquals(*watchdog.tempDNSFailures["inspector"], int64(1))
			tests.S(t).ExpectEquals(*watchdog.tempDNSFailures["applier"], int64(1))
		}
		// failure: temporary dns error with > maxTempDNSFailures (ignored)
		{
			maxTempDNSFailures = 1
			var one int64 = 1
			watchdog.tempDNSFailures["inspector"] = &one
			watchdog.tempDNSFailures["applier"] = &one
			watchdog.serverInfoProvider = func(provider dbProvider) (*mysql.ServerInfo, error) {
				return &mysql.ServerInfo{Hostname: provider.Name()}, &net.DNSError{
					IsTemporary: true,
					Err:         "test",
				}
			}
			tests.S(t).ExpectEquals(watchdog.checkDBProvider(migrator.inspector), nil)
			tests.S(t).ExpectEquals(watchdog.checkDBProvider(migrator.applier), nil)
			tests.S(t).ExpectEquals(*watchdog.tempDNSFailures["inspector"], int64(2))
			tests.S(t).ExpectEquals(*watchdog.tempDNSFailures["applier"], int64(2))
		}
	*/
}
