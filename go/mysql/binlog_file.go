/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"fmt"
	"strconv"
	"strings"
)

// BinlogCoordinates described binary log coordinates in the form of log file & log position.
type FileBinlogCoordinates struct {
	LogFile string
	LogPos  int64
}

// ParseFileBinlogCoordinates parses a binlog file position into a FileBinlogCoordinates struct
func ParseFileBinlogCoordinates(logFileLogPos string) (*FileBinlogCoordinates, error) {
	tokens := strings.SplitN(logFileLogPos, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("ParseBinlogCoordinates: Cannot parse BinlogCoordinates from %s. Expected format is file:pos", logFileLogPos)
	}

	if logPos, err := strconv.ParseInt(tokens[1], 10, 0); err != nil {
		return nil, fmt.Errorf("ParseBinlogCoordinates: invalid pos: %s", tokens[1])
	} else {
		return &FileBinlogCoordinates{LogFile: tokens[0], LogPos: logPos}, nil
	}
}

// DisplayString returns a user-friendly string representation of these coordinates
func (this *FileBinlogCoordinates) DisplayString() string {
	return fmt.Sprintf("%s:%d", this.LogFile, this.LogPos)
}

// String returns a user-friendly string representation of these coordinates
func (this FileBinlogCoordinates) String() string {
	return this.DisplayString()
}

// Equals tests equality of this coordinate and another one.
func (this *FileBinlogCoordinates) Equals(other BinlogCoordinates) bool {
	if other == nil {
		return false
	}
	otherCoords, ok := other.(*FileBinlogCoordinates)
	if !ok {
		return false
	}
	return this.LogFile == otherCoords.LogFile && this.LogPos == otherCoords.LogPos
}

// IsEmpty returns true if the log file is empty, unnamed
func (this *FileBinlogCoordinates) IsEmpty() bool {
	return this.LogFile == ""
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (this *FileBinlogCoordinates) SmallerThan(other BinlogCoordinates) bool {
	otherCoords, ok := other.(*FileBinlogCoordinates)
	if !ok {
		return false
	}

	if this.LogFile < otherCoords.LogFile {
		return true
	}
	if this.LogFile == otherCoords.LogFile && this.LogPos < otherCoords.LogPos {
		return true
	}

	return false
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use this.Equals()
func (this *FileBinlogCoordinates) SmallerThanOrEquals(other BinlogCoordinates) bool {
	if this.SmallerThan(other) {
		return true
	}

	otherCoords, ok := other.(*FileBinlogCoordinates)
	if !ok {
		return false
	}
	return this.LogFile == otherCoords.LogFile && this.LogPos == otherCoords.LogPos
}
