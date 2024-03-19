package template

import (
	"reflect"
	"strings"
	"unsafe"

	"github.com/bgadrian/fastfaker/faker"
)

const (
	MutatedPath             string  = "mutated"
	MutatedPathDefaultValue float64 = 0
	MutateFieldIncrement    float64 = 1
)

type Template interface {
	GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{}
	UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
		fake *faker.Faker) (interface{}, error)
	Compare(document1 interface{}, document2 interface{}) (bool, error)
	GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error)
	GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error)
	GenerateIndexesForSdk() (map[string][]string, error)
	GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any
}

// InitialiseTemplate returns a template as an interface defined by user request.
func InitialiseTemplate(template string) Template {
	switch strings.ToLower(template) {
	case "person":
		return &Person{}
	case "hotel":
		return &Hotel{}
	case "small":
		return &Small{}
	case "product":
		return &Product{}
	default:
		return &Person{}
	}
}

func calculateSizeOfStructRecursive(person interface{}) int {
	value := reflect.ValueOf(person)
	size := int(unsafe.Sizeof(person))

	if value.Kind() != reflect.Struct {
		return size
	}

	numFields := value.NumField()
	for i := 0; i < numFields; i++ {
		field := value.Field(i)
		switch field.Kind() {
		case reflect.String:
			size += len(field.String())
		case reflect.Float64:
			size += int(unsafe.Sizeof(float64(0)))
		case reflect.Slice:
			if field.Type().Elem().Kind() == reflect.String {
				for j := 0; j < field.Len(); j++ {
					size += len(field.Index(j).String())
				}
			}
		case reflect.Struct:
			size += calculateSizeOfStruct(field.Interface())

		case reflect.Map:
			keys := field.MapKeys()
			for _, key := range keys {
				size += len(key.String()) // Assuming keys are strings
				value := field.MapIndex(key)
				if value.IsValid() {
					size += len(value.String())
				}
			}
		}
	}

	return size
}

func calculateSizeOfStruct(person interface{}) int {
	value := reflect.ValueOf(person)
	size := int(unsafe.Sizeof(person))

	//if value.Kind() != reflect.Struct {
	//	return size
	//}
	// if the value is a pointer then dereference it.
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	numFields := value.NumField()
	for i := 0; i < numFields; i++ {
		field := value.Field(i)
		switch field.Kind() {
		case reflect.String:
			size += len(field.String())
		case reflect.Float64:
			size += int(unsafe.Sizeof(float64(0)))
		case reflect.Slice:
			if field.Type().Elem().Kind() == reflect.String {
				for j := 0; j < field.Len(); j++ {
					size += len(field.Index(j).String())
				}
			}
		case reflect.Struct:
			size += calculateSizeOfStructRecursive(field.Interface())

		case reflect.Map:
			keys := field.MapKeys()
			for _, key := range keys {
				size += len(key.String()) // Assuming keys are strings
				value := field.MapIndex(key)
				if value.IsValid() {
					size += len(value.String())
				}
			}
		}
	}

	return size
}
