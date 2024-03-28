package db

import (
	"github.com/AryaanB9/sirius_aryaan/internal/docgenerator"
	"github.com/AryaanB9/sirius_aryaan/internal/meta_data"
	"github.com/AryaanB9/sirius_aryaan/internal/template"
	"log"

	"github.com/bgadrian/fastfaker/faker"

	"testing"
)

func TestColumnarDB(t *testing.T) {
	/*
		This test does the following
		1. Insert in the range of 0-10
		2. Bulk Insert documents in the range of 10-50
		3. Update Documents from range 0-10
		4. Bulk Update documents in the range 10-50
		5. Read Docs from 0-10 and check if they are updated
		6. Bulk Read Docs in the range 10-50 and check if they are updated
		7. Delete in the range of 40-50
		8. Bulk Delete documents in the range of 0-40
	*/

	db, err := ConfigDatabase("columnar")
	if err != nil {
		t.Fatal(err)
	}
	connStr := ""
	username := ""
	password := ""
	extra := Extras{
		Bucket:     "SiriusTest",
		Scope:      "SiriusScope",
		Collection: "SiriusCollection",
	}
	if err := db.Connect(connStr, username, password, extra); err != nil {
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

	//Inserting Documents into Columnar
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)
		doc, err = g.Template.GetValues(doc)
		if err != nil {
			t.Fail()
		}
		//log.Println(docId, Doc)
		createResult := db.Create(connStr, username, password, KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: i,
		}, extra)
		if createResult.GetError() != nil {
			t.Error(createResult.GetError())
		} else {
			log.Println("Inserting", createResult.Key(), " ", createResult.Value())
		}
	}

	// Bulk Inserting Documents into Columnar
	var keyValues []KeyValue
	for i := int64(10); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)
		doc, err = g.Template.GetValues(doc)
		if err != nil {
			t.Fail()
		}
		log.Println(docId, doc)
		keyVal := KeyValue{docId, doc, i}
		keyValues = append(keyValues, keyVal)
	}
	createBulkResult := db.CreateBulk(connStr, username, password, keyValues, extra)
	for _, i := range keyValues {
		if createBulkResult.GetError(i.Key) != nil {
			t.Error(createBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Insert, Inserted Key:", i.Key, "| Value:", i.Doc)
		}
	}

	//Upserting Documents into Columnar
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)

		doc := g.Template.GenerateDocument(fake, docId, 1024) // Original Doc
		doc, err = g.Template.UpdateDocument([]string{}, doc, 1024, fake)
		if err != nil {
			t.Fail()
		}
		//doc, err = g.Template.GetValues(doc)
		if err != nil {
			t.Fail()
		} // 1 Time Mutated Doc
		//log.Println(docId, doc)
		updateResult := db.Update(connStr, username, password, KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: i,
		}, extra)
		if updateResult.GetError() != nil {
			t.Error(updateResult.GetError())
		} else {
			log.Println("Upserting", updateResult.Key(), " ", updateResult.Value())
		}
	}

	// Bulk Updating Documents into Columnar
	keyValues = nil
	for i := int64(10); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)             // Original Doc
		doc, err = g.Template.UpdateDocument([]string{}, doc, 1024, fake) //mutated once
		if err != nil {
			t.Fail()
		}
		//doc, err = g.Template.GetValues(doc)
		if err != nil {
			t.Fail()
		}

		keyVal := KeyValue{docId, doc, i}
		keyValues = append(keyValues, keyVal)
	}
	updateBulkResult := db.UpdateBulk(connStr, username, password, keyValues, extra)
	for _, i := range keyValues {
		if updateBulkResult.GetError(i.Key) != nil {
			t.Error(updateBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Upsert, Inserted Key:", i.Key, "| Value:", i.Doc)
		}
	}

	//  Reading Documents into Columnar
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		createResult := db.Read(connStr, username, password, docId, i, extra)
		if createResult.GetError() != nil {
			t.Error(createResult.GetError())
		} else {
			log.Println("Inserting", createResult.Key(), " ", createResult.Value())
		}
	}
	//  Bulk Reading Documents into Columnar
	keyValues = nil
	for i := int64(0); i < int64(20); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, KeyValue{
			Key:    docId,
			Offset: i,
		})
	}
	readBulkResult := db.ReadBulk(connStr, username, password, keyValues, extra)
	for _, i := range keyValues {
		if readBulkResult.GetError(i.Key) != nil {
			t.Error(readBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Read, Inserted Key:", i.Key, "| Value:", readBulkResult.Value(i.Key))
		}
	}

	//Deleting Documents from Columnar
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)

		deleteResult := db.Delete(connStr, username, password, docId, i, extra)
		if deleteResult.GetError() != nil {
			t.Error(deleteResult.GetError())
		} else {
			log.Println("Deleting", deleteResult.Key())
		}
	}

	// Bulk Deleting Documents from Columnar
	keyValues = nil
	for i := int64(10); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)

		keyVal := KeyValue{docId, nil, i}
		keyValues = append(keyValues, keyVal)
	}
	deleteBulkResult := db.DeleteBulk(connStr, username, password, keyValues, extra)
	for _, i := range keyValues {
		if deleteBulkResult.GetError(i.Key) != nil {
			t.Error(deleteBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Deleting, Deleted Key:", i.Key)
		}
	}

}
