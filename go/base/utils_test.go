/*
   Copyright 2023 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package base

import (
	"testing"

	"github.com/github/gh-ost/go/mysql"
	"github.com/openark/golib/log"
	test "github.com/openark/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestStringContainsAll(t *testing.T) {
	s := `insert,delete,update`

	test.S(t).ExpectFalse(StringContainsAll(s))
	test.S(t).ExpectFalse(StringContainsAll(s, ""))
	test.S(t).ExpectFalse(StringContainsAll(s, "drop"))
	test.S(t).ExpectTrue(StringContainsAll(s, "insert"))
	test.S(t).ExpectFalse(StringContainsAll(s, "insert", "drop"))
	test.S(t).ExpectTrue(StringContainsAll(s, "insert", ""))
	test.S(t).ExpectTrue(StringContainsAll(s, "insert", "update", "delete"))
}

func TestValidateConnection(t *testing.T) {
	connectionConfig := &mysql.ConnectionConfig{
		Key: mysql.InstanceKey{
			Hostname: t.Name(),
			Port:     mysql.DefaultInstancePort,
		},
	}

	// check valid port matching connectionConfig validates
	{
		migrationContext := &MigrationContext{Log: NewDefaultLogger()}
		serverInfo := &mysql.ServerInfo{
			Port:      mysql.NewServerPort(mysql.DefaultInstancePort),
			ExtraPort: mysql.NewServerPort(mysql.DefaultInstancePort + 1),
		}
		test.S(t).ExpectNil(ValidateConnection(serverInfo, connectionConfig, migrationContext, "test"))
	}
	// check NULL port validates when AliyunRDS=true
	{
		migrationContext := &MigrationContext{
			Log:       NewDefaultLogger(),
			AliyunRDS: true,
		}
		serverInfo := &mysql.ServerInfo{}
		test.S(t).ExpectNil(ValidateConnection(serverInfo, connectionConfig, migrationContext, "test"))
	}
	// check NULL port validates when AzureMySQL=true
	{
		migrationContext := &MigrationContext{
			Log:        NewDefaultLogger(),
			AzureMySQL: true,
		}
		serverInfo := &mysql.ServerInfo{}
		test.S(t).ExpectNil(ValidateConnection(serverInfo, connectionConfig, migrationContext, "test"))
	}
	// check NULL port validates when GoogleCloudPlatform=true
	{
		migrationContext := &MigrationContext{
			Log:                 NewDefaultLogger(),
			GoogleCloudPlatform: true,
		}
		serverInfo := &mysql.ServerInfo{}
		test.S(t).ExpectNil(ValidateConnection(serverInfo, connectionConfig, migrationContext, "test"))
	}
	// check extra_port validates when port=NULL
	{
		migrationContext := &MigrationContext{Log: NewDefaultLogger()}
		serverInfo := &mysql.ServerInfo{
			ExtraPort: mysql.NewServerPort(mysql.DefaultInstancePort),
		}
		test.S(t).ExpectNil(ValidateConnection(serverInfo, connectionConfig, migrationContext, "test"))
	}
	// check extra_port validates when port does not match but extra_port does
	{
		migrationContext := &MigrationContext{Log: NewDefaultLogger()}
		serverInfo := &mysql.ServerInfo{
			Port:      mysql.NewServerPort(12345),
			ExtraPort: mysql.NewServerPort(mysql.DefaultInstancePort),
		}
		test.S(t).ExpectNil(ValidateConnection(serverInfo, connectionConfig, migrationContext, "test"))
	}
	// check validation fails when valid port does not match connectionConfig
	{
		migrationContext := &MigrationContext{Log: NewDefaultLogger()}
		serverInfo := &mysql.ServerInfo{
			Port: mysql.NewServerPort(9999),
		}
		test.S(t).ExpectNotNil(ValidateConnection(serverInfo, connectionConfig, migrationContext, "test"))
	}
	// check validation fails when port and extra_port are invalid
	{
		migrationContext := &MigrationContext{Log: NewDefaultLogger()}
		serverInfo := &mysql.ServerInfo{}
		test.S(t).ExpectNotNil(ValidateConnection(serverInfo, connectionConfig, migrationContext, "test"))
	}
}
