/*
   Copyright 2024 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	gosql "database/sql"
	"encoding/json"
	"reflect"
)

// ServerPort wraps gosql.NullInt64.
type ServerPort gosql.NullInt64

// NewServerPort returns a new ServerPort.
func NewServerPort(port int64) ServerPort {
	return ServerPort{Int64: port, Valid: port > 0 && port <= 65535}
}

// MarshalJSON causes the underlying gosql.NullInt64 struct
// to marshal more-cleanly as an int64 vs a struct.
func (sp *ServerPort) MarshalJSON() ([]byte, error) {
	return json.Marshal(sp.Int64)
}

// ServerInfo represents the online config of a MySQL server.
type ServerInfo struct {
	Version         string     `json:",omitempty"`
	VersionComment  string     `json:",omitempty"`
	Hostname        string     `json:",omitempty"`
	Port            ServerPort `json:",omitempty"`
	BinlogFormat    string     `json:",omitempty"`
	BinlogRowImage  string     `json:",omitempty"`
	LogBin          bool       `json:",omitempty"`
	LogSlaveUpdates bool       `json:",omitempty"`
	SQLMode         string     `json:",omitempty"`
	TimeZone        string     `json:",omitempty"`

	// @@global.extra_port is Percona/MariaDB-only
	ExtraPort ServerPort
}

// GetServerInfo returns a ServerInfo struct representing
// the online config of a MySQL server.
func GetServerInfo(db *gosql.DB) (*ServerInfo, error) {
	var info ServerInfo
	query := `select /* gh-ost */ @@global.version, @@global.version_comment, @@global.hostname,
		@@global.port, @@global.binlog_format, @@global.binlog_row_image, @@global.log_bin,
		@@global.log_slave_updates, @@global.sql_mode, @@global.time_zone`
	if err := db.QueryRow(query).Scan(&info.Version, &info.VersionComment, &info.Hostname,
		&info.Port, &info.BinlogFormat, &info.BinlogRowImage, &info.LogBin,
		&info.LogSlaveUpdates, &info.SQLMode, &info.TimeZone,
	); err != nil {
		return nil, err
	}

	extraPortQuery := `select @@global.extra_port`
	// swallow possible error. not all servers support extra_port
	_ = db.QueryRow(extraPortQuery).Scan(&info.ExtraPort)

	return &info, nil
}

// String returns a JSON representation of *ServerInfo.
func (info *ServerInfo) String() string {
	data, err := json.Marshal(info)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// Equals returns true if the provided *ServerInfo is
// equal to *ServerInfo.
func (info *ServerInfo) Equals(cmp *ServerInfo) bool {
	return reflect.DeepEqual(info, cmp)
}
