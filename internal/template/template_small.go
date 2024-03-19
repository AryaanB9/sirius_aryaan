package template

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

type Small struct {
	ID         string  `json:"_id" bson:"_id"`
	RandomData string  `json:"d,omitempty"`
	Mutated    float64 `json:"mutated,omitempty"`
}

func (s *Small) GenerateDocument(fake *faker.Faker, key string, documentSize int, sql bool) interface{} {
	var small *Small
	small = &Small{
		ID:         key,
		RandomData: strings.Repeat(fake.Letter(), documentSize),
		Mutated:    MutatedPathDefaultValue,
	}
	if sql {
		val := []interface{}{small.ID, small.RandomData, small.Mutated}
		return val
	}
	return small
}

func (s *Small) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker, sql bool) (interface{}, error) {

	t, ok := lastUpdatedDocument.(*Small)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to person template")
	}
	t.RandomData = strings.Repeat(fake.Letter(), documentSize)
	if sql {
		val := []interface{}{t.ID, t.RandomData, t.Mutated}
		return val, nil
	}
	return t, nil
}

func (s *Small) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	t1, ok := document1.(*Small)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to person template")
	}
	t2, ok := document2.(*Small)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to person template")
	}
	return reflect.DeepEqual(t1, t2), nil
}

func (s *Small) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, nil
}

func (s *Small) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, nil
}

func (s *Small) GenerateIndexesForSdk() (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (s *Small) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {

	return map[string]interface{}{
		"subDocData": fake.Sentence(subDocSize),
	}
}
