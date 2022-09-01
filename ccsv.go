package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/cast"
)

// CustomMarshallingFunc is a function that can be used to customize the marshalling of a field.
type CustomMarshallingFunc func(v *reflect.Value, fieldValue string) error

// Options defines general configuration of CSV processing.
type Options struct {
	Separator        rune // Separator character (defaults to ',')
	LazyQuotes       bool // LazyQuotes is a flag that determines whether quotes should be escaped if they are part of the data (defaults to false)
	FieldsPerRecord  int  // FieldsPerRecord is the number of expected fields per record (defaults to -1, meaning any number of fields)
	TrimLeadingSpace bool // TrimLeadingSpace is a flag that determines whether leading white space in a field is trimmed (defaults to false)
	Comment          rune // Comment character (defaults to '#')

	IgnoreUnknownFields      bool // IgnoreUnknownFields is a flag that determines whether to ignore fields that are not defined in the struct (defaults to false)
	IgnoreFieldTypeErrors    bool // IgnoreFieldTypeErrors is a flag that determines whether to ignore field type errors (defaults to false)
	UseFieldNames            bool // UseFieldNames is a flag that indicates to use struct field names
	UseStructTags            bool // UseStructTags is a flag that indicates to use struct field tags
	CustomMarshallingFuncMap map[string]CustomMarshallingFunc
}

// ProcessCSV processes CSV input and returns a slice of structs.
func ProcessCSV[T any](options *Options, content string) ([]*T, error) {
	r := csv.NewReader(strings.NewReader(content))

	if options != nil {
		if options.Separator != 0 {
			r.Comma = options.Separator
		}
		if options.LazyQuotes {
			r.LazyQuotes = true
		}
		if options.FieldsPerRecord != 0 {
			r.FieldsPerRecord = options.FieldsPerRecord
		}
		if options.TrimLeadingSpace {
			r.TrimLeadingSpace = true
		}
		if options.Comment != 0 {
			r.Comment = options.Comment
		}

		if !options.UseFieldNames && !options.UseStructTags {
			options.UseFieldNames = true
		}

		if options.UseFieldNames && options.UseStructTags {
			options.UseStructTags = false
		}
	}

	headers, err := r.Read()
	if err == io.EOF {
		return nil, nil
	}

	ts := []*T{}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading csv: %s", err)
		}

		t := new(T)

		ts = append(ts, t)
		err = UnmarshalRecord(options, headers, record, ts[len(ts)-1])
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling record: %s", err)
		}
	}

	return ts, nil
}

// UnmarshalRecord unmarshals a single record into a struct.
func UnmarshalRecord[T any](options *Options, headers []string, record []string, v *T) error {
	s := reflect.ValueOf(v).Elem()
	for i := 0; i < len(record); i++ {
		var fieldName string
		if options.UseFieldNames {
			fieldName = headers[i]
		}
		if options.UseStructTags {
			var err error
			fieldName, err = getFieldNameFromStructTag(headers[i], "csv", v)
			if err != nil {
				return fmt.Errorf("error getting field name from struct tag: %s", err)
			}
		}

		if fieldName == "" {
			return fmt.Errorf("unknown field: %s", headers[i])
		}

		f := s.FieldByName(fieldName)
		if !options.IgnoreUnknownFields && !f.IsValid() {
			return fmt.Errorf("unknown field: %s", headers[i])
		}
		if options.IgnoreUnknownFields && !f.IsValid() {
			continue
		}

		switch f.Type().String() {
		case "int":
			k, err := cast.ToInt64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetInt(k)
		case "int8":
			k, err := cast.ToInt64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetInt(k)
		case "int16":
			k, err := cast.ToInt64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetInt(k)
		case "int32":
			k, err := cast.ToInt64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetInt(k)
		case "int64":
			k, err := cast.ToInt64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetInt(k)
		case "uint":
			k, err := cast.ToUint64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetUint(k)
		case "uint8":
			k, err := cast.ToUint64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetUint(k)
		case "uint16":
			k, err := cast.ToUint64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetUint(k)
		case "uint32":
			k, err := cast.ToUint64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetUint(k)
		case "uint64":
			k, err := cast.ToUint64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetUint(k)
		case "float32":
			k, err := cast.ToFloat64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetFloat(k)
		case "float64":
			k, err := cast.ToFloat64E(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetFloat(k)
		case "string":
			f.SetString(record[i])
		case "bool":
			k, err := cast.ToBoolE(record[i])
			if !options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.SetBool(k)
		case "time.Time":
			t, err := time.Parse(time.RFC3339, record[i])
			if options.IgnoreFieldTypeErrors && err != nil {
				return fmt.Errorf("field %s type conversion failed: %s", headers[i], err)
			}
			f.Set(reflect.ValueOf(&t))
		default:
			if options != nil {
				if options.CustomMarshallingFuncMap != nil {
					if function, ok := options.CustomMarshallingFuncMap[f.Type().String()]; ok {
						err := function(&f, record[i])
						if options.IgnoreFieldTypeErrors && err != nil {
							return fmt.Errorf("field %s type conversion failed for %s: %s", headers[i], f.Type().String(), err)
						}
					} else {
						return fmt.Errorf("no custom unmarshalling function found for type %s", f.Type().String())
					}
				}
			}

		}
	}
	return nil
}

func getFieldNameFromStructTag(tag, key string, s interface{}) (string, error) {
	var rt reflect.Type
	if reflect.TypeOf(s).Kind() == reflect.Ptr {
		rt = reflect.TypeOf(reflect.Indirect(reflect.ValueOf(s)).Interface())
	} else {
		rt = reflect.TypeOf(s)
	}

	if rt.Kind() != reflect.Struct {
		return "", fmt.Errorf("expected struct, got %s", rt.Kind())
	}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		v := strings.Split(f.Tag.Get(key), ",")[0] // use split to ignore tag "options" like omitempty, etc.
		if v == tag {
			return f.Name, nil
		}
	}
	return "", nil
}
