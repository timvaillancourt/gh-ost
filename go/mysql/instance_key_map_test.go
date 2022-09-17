package mysql

import (
	"fmt"
	"testing"

	test "github.com/openark/golib/tests"
)

func TestToCommaDelimitedList(t *testing.T) {
	m := InstanceKeyMap{
		InstanceKey{
			Hostname: t.Name() + "9",
			Port:     3306,
		}: true,
		InstanceKey{
			Hostname: t.Name() + "3",
			Port:     3306,
		}: true,
		InstanceKey{
			Hostname: t.Name() + "1",
			Port:     3306,
		}: true,
	}

	// check result is sorted
	test.S(t).ExpectEquals(
		m.ToCommaDelimitedList(),
		fmt.Sprintf("%s1:3306,%s3:3306,%s9:3306", t.Name(), t.Name(), t.Name()),
	)
}
