/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import "errors"

var ErrUnsupportedBinlogCoordinates = errors.New("unsupported BinlogCoordinates")

type BinlogCoordinates interface {
	String() string
	DisplayString() string
	IsEmpty() bool
	Equals(other BinlogCoordinates) bool
	SmallerThan(other BinlogCoordinates) bool
	SmallerThanOrEquals(other BinlogCoordinates) bool
}
