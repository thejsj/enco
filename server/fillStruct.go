package main

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func SetField(obj interface{}, name string, value interface{}) error {
	// Convert to Capitalized
	name = strings.Title(name)
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return errors.New("Provided value type didn't match obj field type")
	}

	structFieldValue.Set(val)
	return nil
}

func FillStruct(data map[string]interface{}, result interface{}) error {
	for key, value := range data {
		err := SetField(result, key, value)
		if err != nil {
			return err
		}
	}
	return nil
}
