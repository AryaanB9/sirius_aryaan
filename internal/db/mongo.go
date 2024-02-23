package db

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/barkha06/sirius/internal/sdk_mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	connectionManager *sdk_mongo.MongoConnectionManager
}

type perMongoDocResult struct {
	value  interface{}
	error  error
	status bool
	offset int64
}

type mongoOperationResult struct {
	key    string
	result perMongoDocResult
}

func NewMongoConnectionManager() *Mongo {
	return &Mongo{
		connectionManager: sdk_mongo.ConfigMongoConnectionManager(),
	}
}

// Operation Results for Single Operations like Create, Update, Touch and Delete

func newMongoOperationResult(key string, value interface{}, err error, status bool, offset int64) *mongoOperationResult {
	return &mongoOperationResult{
		key: key,
		result: perMongoDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (m *mongoOperationResult) Key() string {
	return m.key
}

func (m *mongoOperationResult) Value() interface{} {
	return m.result.value
}

func (m *mongoOperationResult) GetStatus() bool {
	return m.result.status
}

func (m *mongoOperationResult) GetError() error {
	return m.result.error
}

func (m *mongoOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (m *mongoOperationResult) GetOffset() int64 {
	return m.result.offset
}

// Operation Results for Bulk Operations like Bulk-Create, Bulk-Update, Bulk-Touch and Bulk-Delete

type mongoBulkOperationResult struct {
	keyValues map[string]perMongoDocResult
}

func newMongoBulkOperation() *mongoBulkOperationResult {
	return &mongoBulkOperationResult{
		keyValues: make(map[string]perMongoDocResult),
	}
}

func (m *mongoBulkOperationResult) AddResult(key string, value interface{}, err error, status bool, offset int64) {
	m.keyValues[key] = perMongoDocResult{
		value:  value,
		error:  err,
		status: status,
		offset: offset,
	}
}

func (m *mongoBulkOperationResult) Value(key string) interface{} {
	if x, ok := m.keyValues[key]; ok {
		return x.value
	}
	return nil
}

func (m *mongoBulkOperationResult) GetStatus(key string) bool {
	if x, ok := m.keyValues[key]; ok {
		return x.status
	}
	return false
}

func (m *mongoBulkOperationResult) GetError(key string) error {
	if x, ok := m.keyValues[key]; ok {
		return x.error
	}
	return errors.New("Key not found in bulk operation")
}

func (m *mongoBulkOperationResult) GetExtra(key string) map[string]any {
	if _, ok := m.keyValues[key]; ok {
		return map[string]any{}
	}
	return nil
}

func (m *mongoBulkOperationResult) GetOffset(key string) int64 {
	if x, ok := m.keyValues[key]; ok {
		return x.offset
	}
	return -1
}

func (m *mongoBulkOperationResult) failBulk(keyValue []KeyValue, err error) {
	for _, x := range keyValue {
		m.keyValues[x.Key] = perMongoDocResult{
			value:  x.Doc,
			error:  err,
			status: false,
		}
	}
}

func (m *mongoBulkOperationResult) GetSize() int {
	return len(m.keyValues)
}

func (m Mongo) Connect(connStr, username, password string, extra Extras) error {
	clusterConfig := &sdk_mongo.MongoClusterConfig{
		ConnectionString: connStr,
		Username:         username,
		Password:         password,
	}

	if _, err := m.connectionManager.GetMongoCluster(connStr, username, password, clusterConfig); err != nil {
		return err
	}

	return nil
}

func (m Mongo) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	//fmt.Println("MongoDB Create(): Client:", mongoClient)

	if err := validateStrings(extra.Database); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, errors.New("database is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Collection); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, errors.New("collection is missing"), false,
			keyValue.Offset)
	}
	mongoDatabase := mongoClient.Database(extra.Database)
	mongoCollection := mongoDatabase.Collection(extra.Collection)

	result, err2 := mongoCollection.InsertOne(context.TODO(), keyValue.Doc, nil)

	if err2 != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, err2, false, keyValue.Offset)
	}
	if result == nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful CREATE operation %s ", connStr), false,
			keyValue.Offset)
	}
	return newMongoOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (m Mongo) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient

	if err := validateStrings(extra.Database); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, errors.New("database is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Collection); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, errors.New("collection is missing"), false,
			keyValue.Offset)
	}
	mongoDatabase := mongoClient.Database(extra.Database)
	mongoCollection := mongoDatabase.Collection(extra.Collection)
	filter := bson.M{"_id": keyValue.Key}
	update := bson.M{"$set": keyValue.Doc}
	log.Println(filter, update)
	result, err2 := mongoCollection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))

	if err2 != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, err2, false, keyValue.Offset)
	}
	if result == nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful UPDATE operation %s ", connStr), false,
			keyValue.Offset)
	}
	return newMongoOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
	//TODO implement me
	// panic("implement me")
}

func (m Mongo) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) InsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) UpsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Increment(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) ReplaceSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) ReadSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) DeleteSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {

	result := newMongoBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	//fmt.Println("In CreateBulk(), Mongo Client:", mongoClient)

	if err := validateStrings(extra.Database); err != nil {
		result.failBulk(keyValues, errors.New("database name is missing"))
		return result
	}
	if err := validateStrings(extra.Collection); err != nil {
		result.failBulk(keyValues, errors.New("collection name is missing"))
		return result
	}
	mongoDatabase := mongoClient.Database(extra.Database)
	mongoCollection := mongoDatabase.Collection(extra.Collection)

	var models []mongo.WriteModel
	for _, x := range keyValues {
		model := mongo.NewInsertOneModel().SetDocument(x.Doc)
		models = append(models, model)
	}
	opts := options.BulkWrite().SetOrdered(false)
	mongoBulkWriteResult, err := mongoCollection.BulkWrite(context.TODO(), models, opts)
	if err != nil {
		log.Println("MongoDB CreateBulk(): Bulk Insert Error:", err)
		result.failBulk(keyValues, errors.New("MongoDB CreateBulk(): Bulk Insert Error"))
		return result
	} else if int64(len(keyValues)) != mongoBulkWriteResult.InsertedCount {
		result.failBulk(keyValues, errors.New("MongoDB CreateBulk(): Inserted Count does not match batch size"))
		return result
	}

	// TODO
	// Update the AddResult() function for MongoDB.

	return result
}

// Warmup validates all the fields and checks if MongoDB Database or Collection is there
func (m Mongo) Warmup(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}

	databaseName := extra.Database
	if err := validateStrings(databaseName); err != nil {
		return errors.New("MongoDB Database name is missing")
	}

	collectionName := extra.Collection
	if err := validateStrings(collectionName); err != nil {
		return errors.New("MongoDB Collection name is missing")
	}

	// TODO
	// Checking if the Collection exists or not
	//mongoDatabase := m.connectionManager.Clusters[connStr].MongoClusterClient.Database(databaseName)
	//log.Println("MongoDB Warmup() : mongo database object var:", mongoDatabase)
	//collectionNames, err := mongoDatabase.ListCollections(context.TODO(), nil)
	//if err != nil {
	//	//return errors.New("unable to list Collections for the given MongoDB Database")
	//	return err
	//}
	//
	//for collectionNames.Next(context.TODO()) {
	//	var name string
	//	if err := collectionNames.Decode(&name); err != nil {
	//		log.Println("MongoDB Warmup(): unable to decode Collection Name", err)
	//		return err
	//	}
	//	if name == collectionName {
	//		log.Println("MongoDB Warmup():", collectionName, "Collection exists")
	//	}
	//}
	return nil
}

func (m Mongo) Close(connStr string) error {
	if err := m.connectionManager.Clusters[connStr].MongoClusterClient.Disconnect(context.TODO()); err != nil {
		log.Println("MongoDB Close(): Disconnect failed!")
		return err
	}
	return nil
}

func (m Mongo) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	// panic("implement me")
	result := newMongoBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	// opts1 := options.Client().ApplyURI(connStr)
	// mongoClient, err := mongo.Connect(context.TODO(), opts1)
	if err := validateStrings(extra.Database); err != nil {
		result.failBulk(keyValues, errors.New("database name is missing"))
		return result
	}
	if err := validateStrings(extra.Collection); err != nil {
		result.failBulk(keyValues, errors.New("collection name is missing"))
		return result
	}
	mongoDatabase := mongoClient.Database(extra.Database)
	mongoCollection := mongoDatabase.Collection(extra.Collection)

	var models []mongo.WriteModel
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		filter := bson.M{"_id": x.Key}
		update := bson.M{"$set": x.Doc}
		// result, err2 := mongoCollection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))
		model := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true)
		models = append(models, model)
	}
	opts := options.BulkWrite().SetOrdered(false)
	// log.Println("Lengths: ", len(models), len(keyValues))
	mongoBulkWriteResult, err := mongoCollection.BulkWrite(context.TODO(), models, opts)
	// log.Println(mongoBulkWriteResult)
	if err != nil {
		log.Println("MongoDB CreateBulk(): Bulk Insert Error:", err)
		result.failBulk(keyValues, errors.New("MongoDB UpdateBulk(): Bulk Upsert Error"))
		return result
	} else if int64(len(keyValues)) != mongoBulkWriteResult.ModifiedCount && int64(len(keyValues)) != mongoBulkWriteResult.UpsertedCount {
		// log.Println("count: ", int64(len(keyValues)), mongoBulkWriteResult)
		result.failBulk(keyValues, errors.New("MongoDB UpdateBulk(): Upserted Count does not match batch size"))
		return result
	}

	for _, x := range models {
		upsertOp, ok := x.(*mongo.UpdateOneModel)
		docid := upsertOp.Filter.(bson.M)["_id"]
		value := upsertOp.Update.(bson.M)["$set"]
		// log.Println("docid: value:   ", docid, value)
		if !ok {
			result.AddResult(docid.(string), nil, errors.New("decoding error GetOp"), false, -1)
		} else {
			result.AddResult(docid.(string), value, nil, true, keyToOffset[docid.(string)])
			// log.Println("docid: value:   ", docid, value)
		}
	}
	// mongoClient.Disconnect(context.TODO())
	return result
}

func (m Mongo) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}

// func (m Mongo) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
// 	//TODO implement me
// 	panic("implement me")
// }

// func (m Mongo) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
// 	//TODO implement me
// 	panic("implement me")
// }

// func (m Mongo) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
// 	//TODO implement me
// 	panic("implement me")
// }

// func (m Mongo) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
// 	//TODO implement me
// 	panic("implement me")
// }
