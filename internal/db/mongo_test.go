package db

import (
	"github.com/barkha06/sirius/internal/docgenerator"
	"github.com/barkha06/sirius/internal/meta_data"
	"github.com/barkha06/sirius/internal/template"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"testing"
)

func TestMongoDB(t *testing.T) {
	/*
		This test does the following
		1. Insert in the range of 0-10
		2. Bulk Insert documents in the range of 10-50
		3. Delete in the range of 40-50
		4. Bulk Delete documents in the range of 0-40
	*/

	db, err := ConfigDatabase("mongodb")
	if err != nil {
		t.Fatal(err)
	}
	connStr := "mongodb://Administrator1:password@mongo.cbqeoc.com:27017/"
	username := "Administrator1"
	password := "password"
	if err := db.Connect(connStr, username, password, Extras{}); err != nil {
		t.Error(err)
	}

	m := meta_data.NewMetaData()
	cm1 := m.GetCollectionMetadata("x")

	temp := template.InitialiseTemplate("person")
	g := docgenerator.Generator{
		Template: temp,
	}
	gen := &docgenerator.Generator{
		KeySize:  0,
		DocType:  "json",
		Template: template.InitialiseTemplate("person"),
	}

	// Inserting Documents into MongoDB
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		doc, _ := g.Template.GenerateDocument(docId, &fake, 1024)
		//log.Println(docId, Doc)
		createResult := db.Create(connStr, username, password, KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: i,
		}, Extras{
			Database:   "TestMongoDatabase",
			Collection: "TestingMongoSirius",
		})
		if createResult.GetError() != nil {
			t.Error(createResult.GetError())
		} else {
			log.Println("Inserting", createResult.Key(), " ", createResult.Value())
		}
	}

	// Bulk Inserting Documents into MongoDB
	var keyValues []KeyValue
	for i := int64(10); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		doc, _ := g.Template.GenerateDocument(docId, &fake, 1024)
		//log.Println(docId, Doc)
		keyVal := KeyValue{docId, doc, i}
		keyValues = append(keyValues, keyVal)
	}
	createBulkResult := db.CreateBulk(connStr, username, password, keyValues, Extras{
		Database:   "TestMongoDatabase",
		Collection: "TestingMongoSirius",
	})
	for _, i := range keyValues {
		if createBulkResult.GetError(i.Key) != nil {
			t.Error(createBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Insert, Inserted Key:", i.Key, "| Value:", i.Doc)
		}
	}

	// Deleting Documents from MongoDB
	for i := int64(40); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)

		deleteResult := db.Delete(connStr, username, password, docId, i, Extras{
			Database:   "TestMongoDatabase",
			Collection: "TestingMongoSirius",
		})
		if deleteResult.GetError() != nil {
			t.Error(deleteResult.GetError())
		} else {
			log.Println("Deleting", deleteResult.Key())
		}
	}

	// Bulk Deleting Documents from MongoDB
	keyValues = nil
	for i := int64(0); i < int64(40); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)

		keyVal := KeyValue{docId, nil, i}
		keyValues = append(keyValues, keyVal)
	}
	deleteBulkResult := db.DeleteBulk(connStr, username, password, keyValues, Extras{
		Database:   "TestMongoDatabase",
		Collection: "TestingMongoSirius",
	})
	for _, i := range keyValues {
		if deleteBulkResult.GetError(i.Key) != nil {
			t.Error(deleteBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Deleting, Deleted Key:", i.Key)
		}
	}

	// Closing the Connection to MongoDB
	if err = db.Close(connStr); err != nil {
		t.Error(err)
		t.Fail()
	}
}
