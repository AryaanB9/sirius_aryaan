package template

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

type SmallSql struct {
	ID           string  `json:"_id" bson:"_id"`
	RandomData   string  `json:"d,omitempty"`
	Mutated      float64 `json:"mutated,omitempty"`
	Value        []interface{}
	TemplateType string `json:"template_type" dynamodbav:"template_type"`
}

func (s *SmallSql) GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{} {
	small := &SmallSql{
		ID:           key,
		TemplateType: "small_sql",
		RandomData:   strings.Repeat(fake.Letter(), documentSize),
		Mutated:      MutatedPathDefaultValue,
	}
	values := []interface{}{&small.TemplateType, &small.ID, &small.RandomData, &small.Mutated}
	small.Value = values
	return small
}

func (s *SmallSql) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {

	small, ok := lastUpdatedDocument.(*SmallSql)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to person template")
	}
	small.RandomData = strings.Repeat(fake.Letter(), documentSize)
	values := []interface{}{&small.TemplateType, &small.ID, &small.RandomData, &small.Mutated}
	small.Value = values
	return small, nil
}

func (s *SmallSql) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	t1, ok := document1.(*SmallSql)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to person template")
	}
	t2, ok := document2.(*SmallSql)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to person template")
	}
	return reflect.DeepEqual(t1, t2), nil
}

func (s *SmallSql) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, nil
}

func (s *SmallSql) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, nil
}

func (s *SmallSql) GenerateIndexesForSdk() (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (s *SmallSql) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {

	return map[string]interface{}{
		"subDocData": fake.Sentence(subDocSize),
	}
}
func (s *SmallSql) GetValues(document interface{}) (interface{}, error) {
	small, ok := document.(*SmallSql)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to small template")
	}
	return small.Value, nil
}
