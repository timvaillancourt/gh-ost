package sql

import (
	"fmt"
	"reflect"
	"strings"
)

// OptimizerHints stores MySQL Optimizer Hints. See:
// https://dev.mysql.com/doc/refman/8.0/en/optimizer-hints.html.
type OptimizerHints struct {
	ResourceGroup    string `hint:"RESOURCE_GROUP"`
	MaxExecutionTime int    `hint:"MAX_EXECUTION_TIME"`
}

// String returns a optimizer hint string containing all
// defined hints.
func (hints OptimizerHints) String() (hintsSQL string) {
	ts := reflect.TypeOf(hints)
	if ts.NumField() == 0 {
		return hintsSQL
	}

	vs := reflect.ValueOf(hints)
	hintSlice := make([]string, 0, ts.NumField())
	for i := 0; i < ts.NumField(); i++ {
		fieldTag := ts.Field(i).Tag
		value := vs.Field(i)
		if name, ok := fieldTag.Lookup("hint"); ok && !value.IsZero() {
			switch value.Kind() {
			case reflect.String:
				hintSlice = append(hintSlice, fmt.Sprintf(`%s(%s)`, name, value.String()))
			case reflect.Int, reflect.Int32:
				hintSlice = append(hintSlice, fmt.Sprintf(`%s(%d)`, name, value.Int()))
			}
		}
	}

	if len(hintSlice) > 0 {
		hintsSQL = fmt.Sprintf(`/*+ %s */`, strings.Join(hintSlice, " "))
	}
	return hintsSQL
}
