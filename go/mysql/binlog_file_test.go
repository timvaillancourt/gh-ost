/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"testing"

	"github.com/openark/golib/log"
	test "github.com/openark/golib/tests"
)

func init() {
	log.SetLevel(log.ERROR)
}

func TestFileBinlogCoordinates(t *testing.T) {
	c1 := FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c3 := FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 5000}
	c4 := FileBinlogCoordinates{LogFile: "mysql-bin.00112", LogPos: 104}

	test.S(t).ExpectTrue(c1.Equals(&c2))
	test.S(t).ExpectFalse(c1.Equals(&c3))
	test.S(t).ExpectFalse(c1.Equals(&c4))
	test.S(t).ExpectFalse(c1.SmallerThan(&c2))
	test.S(t).ExpectTrue(c1.SmallerThan(&c3))
	test.S(t).ExpectTrue(c1.SmallerThan(&c4))
	test.S(t).ExpectTrue(c3.SmallerThan(&c4))
	test.S(t).ExpectFalse(c3.SmallerThan(&c2))
	test.S(t).ExpectFalse(c4.SmallerThan(&c2))
	test.S(t).ExpectFalse(c4.SmallerThan(&c3))

	test.S(t).ExpectTrue(c1.SmallerThanOrEquals(&c2))
	test.S(t).ExpectTrue(c1.SmallerThanOrEquals(&c3))
}

func TestFileBinlogCoordinatesAsKey(t *testing.T) {
	m := make(map[FileBinlogCoordinates]bool)

	c1 := FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := FileBinlogCoordinates{LogFile: "mysql-bin.00022", LogPos: 104}
	c3 := FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c4 := FileBinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 222}

	m[c1] = true
	m[c2] = true
	m[c3] = true
	m[c4] = true

	test.S(t).ExpectEquals(len(m), 3)
}
