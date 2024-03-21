package db

import (
	"github.com/barkha06/sirius/internal/docgenerator"
	"github.com/barkha06/sirius/internal/meta_data"
	"github.com/barkha06/sirius/internal/template"

	// "github.com/jaswdr/faker"
	"log"

	"github.com/bgadrian/fastfaker/faker"

	"testing"
)

func TestCassandraDB(t *testing.T) {
	/*
		This test does the following
		1. Create a Keyspace and Table
		2. Insert in the range of 0-10. Check the count
		3. Bulk Insert documents in the range of 10-50. Check the count
		4. Update Documents from range 0-10
		5. Bulk Update documents in the range 10-50
		6. Read Docs from 0-10 and check if they are updated
		7. Bulk Read Docs in the range 10-50 and check if they are updated
		8. Delete in the range of 40-50
		9. Bulk Delete documents in the range of 0-40
		10. Delete the Table and Keyspace
	*/

	db, err := ConfigDatabase("cassandra")
	if err != nil {
		t.Fatal(err)
	}
	connStr := "127.0.0.1:9042"
	username := "username"
	password := "password"
	if err := db.Connect(connStr, username, password, Extras{}); err != nil {
		t.Error(err)
	}

	m := meta_data.NewMetaData()
	cm1 := m.GetCollectionMetadata("x")

	temp := template.InitialiseTemplate("hotel")
	g := docgenerator.Generator{
		Template: temp,
	}
	gen := &docgenerator.Generator{
		KeySize:  0,
		DocType:  "json",
		Template: template.InitialiseTemplate("hotel"),
	}

	// Inserting Documents into Cassandra
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)
		//log.Println(docId, Doc)
		createResult := db.Create(connStr, username, password, KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: i,
		}, Extras{
			Keyspace: "test_sirius",
			Table:    "hotels",
		})
		if createResult.GetError() != nil {
			t.Error(createResult.GetError())
		} else {
			log.Println("Inserting", createResult.Key(), " ", createResult.Value())
		}
	}

	// Bulk Inserting Documents into Cassandra
	var keyValues []KeyValue
	for i := int64(10); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)
		//log.Println(docId, Doc)
		keyVal := KeyValue{docId, doc, i}
		keyValues = append(keyValues, keyVal)
	}
	createBulkResult := db.CreateBulk(connStr, username, password, keyValues, Extras{
		Keyspace: "test_sirius",
		Table:    "hotels",
	})
	for _, i := range keyValues {
		if createBulkResult.GetError(i.Key) != nil {
			t.Error(createBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Insert, Inserted Key:", i.Key, "| Value:", i.Doc)
		}
	}

	// Updating Documents into Cassandra
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024) // Original Doc
		doc = g.Template.GenerateDocument(fake, docId, 1024)  // 1 Time Mutated Doc
		//log.Println(docId, doc)
		updateResult := db.Update(connStr, username, password, KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: i,
		}, Extras{
			Keyspace: "test_sirius",
			Table:    "hotels",
		})
		if updateResult.GetError() != nil {
			t.Error(updateResult.GetError())
		} else {
			log.Println("Upserting", updateResult.Key(), " ", updateResult.Value())
		}
	}

	// Bulk Updating Documents into Cassandra
	keyValues = nil
	for i := int64(10); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)
		doc = g.Template.GenerateDocument(fake, docId, 1024) // 1 Time Mutated Doc
		//log.Println(docId, Doc)
		keyVal := KeyValue{docId, doc, i}
		keyValues = append(keyValues, keyVal)
	}
	updateBulkResult := db.UpdateBulk(connStr, username, password, keyValues, Extras{
		Keyspace: "test_sirius",
		Table:    "hotels",
	})
	for _, i := range keyValues {
		if updateBulkResult.GetError(i.Key) != nil {
			t.Error(updateBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Upsert, Inserted Key:", i.Key, "| Value:", i.Doc)
		}
	}

	// TODO Reading Documents into Cassandra

	// TODO Bulk Reading Documents into Cassandra

	// Deleting Documents from Cassandra
	for i := int64(40); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)

		deleteResult := db.Delete(connStr, username, password, docId, i, Extras{
			Keyspace: "test_sirius",
			Table:    "hotels",
		})
		if deleteResult.GetError() != nil {
			t.Error(deleteResult.GetError())
		} else {
			log.Println("Deleting", deleteResult.Key())
		}
	}

	// Bulk Deleting Documents from Cassandra
	keyValues = nil
	for i := int64(0); i < int64(40); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)

		keyVal := KeyValue{docId, nil, i}
		keyValues = append(keyValues, keyVal)
	}
	deleteBulkResult := db.DeleteBulk(connStr, username, password, keyValues, Extras{
		Keyspace: "test_sirius",
		Table:    "hotels",
	})
	for _, i := range keyValues {
		if deleteBulkResult.GetError(i.Key) != nil {
			t.Error(deleteBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Deleting, Deleted Key:", i.Key)
		}
	}

	// Closing the Connection to Cassandra
	if err = db.Close(connStr); err != nil {
		t.Error(err)
		t.Fail()
	}
}
