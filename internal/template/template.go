package template

import (
	"fmt"
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
	GetValues(interface{}) (interface{}, error)
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
	case "person_sql":
		return &PersonSql{}
	case "hotel_sql":
		return &HotelSql{}
	case "small_sql":
		return &SmallSql{}
	default:
		return &Person{}
	}
}

func calculateSizeOfStruct(person interface{}) int {
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
		}
	}

	return size
}

func GetSQLSchema(templateName string, table string, size int) string {
	var query string
	switch templateName {
	case "hotel_sql":
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (template_name VARCHAR(20),id VARCHAR(30) PRIMARY KEY,address VARCHAR(100) NOT NULL,free_parking Bool,city VARCHAR(50),url VARCHAR(50),phone VARCHAR(20),price DOUBLE,avg_rating DOUBLE,free_breakfast Bool,name VARCHAR(50),email VARCHAR(100),padding VARCHAR(%d),mutated DOUBLE)`, table, size)
	case "":
		fallthrough
	case "person_sql":
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(template_name VARCHAR(20),id VARCHAR(30) PRIMARY KEY,first_name VARCHAR(100),age DOUBLE,email VARCHAR(255),gender VARCHAR(10),marital_status VARCHAR(20),hobbies VARCHAR(50),padding VARCHAR(%d),mutated DOUBLE)`, table, size)
	case "small_sql":
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(template_name VARCHAR(20),id VARCHAR(30) PRIMARY KEY,d VARCHAR(%d),mutated DOUBLE')`, table, size)
	case "product_sql":
		query = fmt.Sprintf(`CREATE TABLE  IF NOT EXISTS %s(template_type VARCHAR(20), id VARCHAR(30) PRIMARY KEY, product_name VARCHAR(255), product_link VARCHAR(255), price DECIMAL(10, 2), avg_rating DECIMAL(5, 2), num_sold BIGINT, upload_date DATE, weight DECIMAL(10, 2), quantity BIGINT, seller_name VARCHAR(255), seller_location VARCHAR(255), seller_verified BOOLEAN, value JSONB, mutated DECIMAL(10, 2), padding VARCHAR(%d))`, table, size)

	}
	return query
}
