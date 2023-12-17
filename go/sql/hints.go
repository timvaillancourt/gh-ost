package sql

import (
	"fmt"
	"reflect"
	"strings"
)

type OptimizerHints struct {
	ResourceGroup    string `hint:"RESOURCE_GROUP"`
	MaxExecutionTime int    `hint:"MAX_EXECUTION_TIME"`
}

func (hints OptimizerHints) String() (comment string) {
	ts := reflect.TypeOf(hints)
	if ts.NumField() == 0 {
		return comment
	}

	vs := reflect.ValueOf(hints)
	hintSlice := make([]string, 0, ts.NumField())
	for i := 0; i < ts.NumField(); i++ {
		fieldTag := ts.Field(i).Tag
		value := vs.Field(i)
		if hint, ok := fieldTag.Lookup("hint"); ok && !value.IsZero() {
			switch value.Kind() {
			case reflect.String:
				hintSlice = append(hintSlice, fmt.Sprintf(`%s(%s)`, hint, value.String()))
			default:
				hintSlice = append(hintSlice, fmt.Sprintf(`%s(%d)`, hint, value.Int()))
			}
		}
	}

	if len(hintSlice) > 0 {
		comment = fmt.Sprintf(`/*+ %s */`, strings.Join(hintSlice, " "))
	}
	return comment
}
