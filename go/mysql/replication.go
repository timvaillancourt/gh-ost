package mysql

import "time"

// SemiSyncConfig represents the semi-sync
// replication config of a mysql server.
type SemiSyncConfig struct {
	Enabled     bool
	Timeout     time.Duration
	WaitNoSlave bool
}
