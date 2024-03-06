package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/barkha06/sirius/internal/sdk_cassandra"

	"github.com/gocql/gocql"
)

type Cassandra struct {
	CassandraConnectionManager *sdk_cassandra.CassandraConnectionManager
}

type perCassandraDocResult struct {
	value  interface{}
	error  error
	status bool
	offset int64
}

// cassandraOperationResult stores the result information for Insert, Upsert, Delete and Read.
type cassandraOperationResult struct {
	key    string
	result perCassandraDocResult
}

func NewCassandraConnectionManager() *Cassandra {
	return &Cassandra{
		CassandraConnectionManager: sdk_cassandra.ConfigCassandraConnectionManager(),
	}
}

func newCassandraOperationResult(key string, value interface{}, err error, status bool, offset int64) *cassandraOperationResult {
	return &cassandraOperationResult{
		key: key,
		result: perCassandraDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (c *cassandraOperationResult) Key() string {
	return c.key
}

func (c *cassandraOperationResult) Value() interface{} {
	return c.result.value
}

func (c *cassandraOperationResult) GetStatus() bool {
	return c.result.status
}

func (c *cassandraOperationResult) GetError() error {
	return c.result.error
}

func (c *cassandraOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (c *cassandraOperationResult) GetOffset() int64 {
	return c.result.offset
}

// Operation Results for Bulk Operations like Bulk-Create, Bulk-Update, Bulk-Touch and Bulk-Delete
type cassandraBulkOperationResult struct {
	keyValues map[string]perCassandraDocResult
}

func newCassandraBulkOperation() *cassandraBulkOperationResult {
	return &cassandraBulkOperationResult{
		keyValues: make(map[string]perCassandraDocResult),
	}
}

func (m *cassandraBulkOperationResult) AddResult(key string, value interface{}, err error, status bool, offset int64) {
	m.keyValues[key] = perCassandraDocResult{
		value:  value,
		error:  err,
		status: status,
		offset: offset,
	}
}

func (m *cassandraBulkOperationResult) Value(key string) interface{} {
	if x, ok := m.keyValues[key]; ok {
		return x.value
	}
	return nil
}

func (m *cassandraBulkOperationResult) GetStatus(key string) bool {
	if x, ok := m.keyValues[key]; ok {
		return x.status
	}
	return false
}

func (m *cassandraBulkOperationResult) GetError(key string) error {
	if x, ok := m.keyValues[key]; ok {
		return x.error
	}
	return errors.New("Key not found in bulk operation")
}

func (m *cassandraBulkOperationResult) GetExtra(key string) map[string]any {
	if _, ok := m.keyValues[key]; ok {
		return map[string]any{}
	}
	return nil
}

func (m *cassandraBulkOperationResult) GetOffset(key string) int64 {
	if x, ok := m.keyValues[key]; ok {
		return x.offset
	}
	return -1
}

func (m *cassandraBulkOperationResult) failBulk(keyValue []KeyValue, err error) {
	for _, x := range keyValue {
		m.keyValues[x.Key] = perCassandraDocResult{
			value:  x.Doc,
			error:  err,
			status: false,
		}
	}
}

func (m *cassandraBulkOperationResult) GetSize() int {
	return len(m.keyValues)
}

type perCassandraSubDocResult struct {
	keyValue []KeyValue
	error    error
	status   bool
	offset   int64
}

type cassandraSubDocOperationResult struct {
	key    string
	result perCassandraSubDocResult
}

func newCassandraSubDocOperationResult(key string, keyValue []KeyValue, err error, status bool, offset int64) *cassandraSubDocOperationResult {
	return &cassandraSubDocOperationResult{
		key: key,
		result: perCassandraSubDocResult{
			keyValue: keyValue,
			error:    err,
			status:   status,
			offset:   offset,
		},
	}
}

func (m *cassandraSubDocOperationResult) Key() string {
	return m.key
}

func (m *cassandraSubDocOperationResult) Value(subPath string) (interface{}, int64) {
	for _, x := range m.result.keyValue {
		if x.Key == subPath {
			return x.Doc, x.Offset
		}
	}
	return nil, -1
}

func (m *cassandraSubDocOperationResult) Values() []KeyValue {
	return m.result.keyValue
}

func (m *cassandraSubDocOperationResult) GetError() error {
	return m.result.error
}

func (m *cassandraSubDocOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (m *cassandraSubDocOperationResult) GetOffset() int64 {
	return m.result.offset
}

func (c *Cassandra) Connect(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}
	clusterConfig := &sdk_cassandra.CassandraClusterConfig{
		ClusterConfigOptions: sdk_cassandra.ClusterConfigOptions{
			KeyspaceName: extra.Keyspace,
			NumConns:     extra.NumOfConns,
		},
	}

	if _, err := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, clusterConfig); err != nil {
		log.Println("In Cassandra Connect(), error in GetCluster()")
		return err
	}

	return nil
}

func (c *Cassandra) Warmup(connStr, username, password string, extra Extras) error {
	// TODO
	log.Println("In Cassandra Warmup()")
	if err := validateStrings(connStr, username, password); err != nil {
		log.Println("In Cassandra Warmup(), error:", err)
		return err
	}

	return nil
}

func (c *Cassandra) Close(connStr string) error {
	return c.CassandraConnectionManager.Disconnect(connStr)
}

func (c *Cassandra) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	if err := validateStrings(extra.Keyspace); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Keyspace name is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Table); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Table name is missing"), false,
			keyValue.Offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Create(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Unable to connect to Cassandra!"), false,
			keyValue.Offset)
	}

	// Converting the Document to JSON
	jsonData, errDocToJSON := json.Marshal(keyValue.Doc)
	if errDocToJSON != nil {
		log.Println("In Cassandra Create(), error marshaling JSON:", errDocToJSON)
	}

	//insertQuery := "INSERT INTO " + extra.Table + " JSON '" + string(jsonData) + "'"
	insertQuery := "INSERT INTO " + extra.Table + " JSON ?"

	//errInsert := cassandraSession.Query(insertQuery).Exec()
	errInsert := cassandraSession.Query(insertQuery, jsonData).Exec()
	if errInsert != nil {
		log.Println("In Cassandra Create(), error inserting data:", errInsert)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errInsert, false, keyValue.Offset)
	}
	return newCassandraOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (c *Cassandra) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	if err := validateStrings(extra.Keyspace); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Keyspace name is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Table); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Table name is missing"), false,
			keyValue.Offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Update(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Unable to connect to Cassandra!"), false,
			keyValue.Offset)
	}

	// Converting the Document to JSON
	jsonData, errDocToJSON := json.Marshal(keyValue.Doc)
	if errDocToJSON != nil {
		log.Println("In Cassandra Update(), error marshaling JSON:", errDocToJSON)
	}

	//updateQuery := "UPDATE " + extra.Table + " SET JSON '" + string(jsonData) + "' WHERE id = " + keyValue.Key
	//updateQuery := "INSERT INTO " + extra.Table + " JSON '" + string(jsonData) + "' DEFAULT UNSET"
	updateQuery := "INSERT INTO " + extra.Table + " JSON ? DEFAULT UNSET"

	errUpdate := cassandraSession.Query(updateQuery, jsonData).Exec()
	if errUpdate != nil {
		log.Println("In Cassandra Update(), error updating data:", errUpdate)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errUpdate, false, keyValue.Offset)
	}
	return newCassandraOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (c *Cassandra) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(key, nil, err, false, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Keyspace is missing"), false, offset)
	}
	cassandraSessionObj, err1 := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
	if err1 != nil {
		return newCassandraOperationResult(key, nil, err1, false, offset)
	}
	var result map[string]interface{}

	query := "SELECT * FROM " + keyspaceName + "." + tableName + " WHERE ID = ?"
	iter := cassandraSessionObj.Query(query, key).Iter()
	result = make(map[string]interface{})
	success := iter.MapScan(result)
	if !success {
		if result == nil {
			return newCassandraOperationResult(key, nil,
				fmt.Errorf("result is nil even after successful READ operation %s ", connStr), false,
				offset)
		} else if err := iter.Close(); err != nil {
			return newCassandraOperationResult(key, nil,
				fmt.Errorf("Unsuccessful READ operation %s ", connStr), false,
				offset)
		}
	}
	return newCassandraOperationResult(key, result, nil, true, offset)
}

func (c *Cassandra) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(key, nil, err, false, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Keyspace is missing"), false, offset)
	}

	cassandraSessionObj, err1 := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
	if err1 != nil {
		return newCassandraOperationResult(key, nil, err1, false, offset)
	}
	query := "DELETE FROM " + keyspaceName + "." + tableName + " WHERE ID = ?"
	if err2 := cassandraSessionObj.Query(query, key).Exec(); err2 != nil {
		return newCassandraOperationResult(key, nil,
			fmt.Errorf("unsuccessful Delete %s ", connStr), false, offset)
	}
	return newCassandraOperationResult(key, nil, nil, true, offset)
}

func (c *Cassandra) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(key, nil, err, false, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Keyspace is missing"), false, offset)
	}
	cassandraSessionObj, err1 := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
	if err1 != nil {
		return newCassandraOperationResult(key, nil, err1, false, offset)
	}
	query := fmt.Sprintf("UPDATE %s.%s USING TTL %d WHERE ID = ?", keyspaceName, tableName, extra.Expiry)
	fmt.Println(query)
	if err2 := cassandraSessionObj.Query(query, key).Exec(); err2 != nil {
		return newCassandraOperationResult(key, nil, err2, false, offset)
	}
	return newCassandraOperationResult(key, nil, nil, true, offset)
}

func (c *Cassandra) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Table name is missing"), false, extra.Cas, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Keyspace is missing"), false, extra.Cas, offset)
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra InsertSubDoc(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCouchbaseSubDocOperationResult(key, keyValues, errSessionCreate, false, extra.Cas, offset)
	}
	for _, x := range keyValues {
		columnName := extra.SubDocPath
		if err := validateStrings(columnName); err != nil {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("SubDocPath is missing"), false, extra.Cas, offset)
		}
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			fmt.Println(alterQuery)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
			}
		}
		insertSubDocQuery := fmt.Sprintf("UPDATE %s SET %s='%s' WHERE ID = ?", tableName, columnName, x.Doc)
		errinsertSubDocQuery := cassandraSession.Query(insertSubDocQuery, key).Exec()
		if errinsertSubDocQuery != nil {
			log.Println("In Cassandra InsertSubDoc(), error inserting data:", errinsertSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errinsertSubDocQuery, false, extra.Cas, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) UpsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Table name is missing"), false, extra.Cas, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Keyspace is missing"), false, extra.Cas, offset)
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra UpsertSubDoc(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCouchbaseSubDocOperationResult(key, keyValues, errSessionCreate, false, extra.Cas, offset)
	}
	for _, x := range keyValues {
		columnName := extra.SubDocPath
		if err := validateStrings(columnName); err != nil {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("SubDocPath is missing"), false, extra.Cas, offset)
		}
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			fmt.Println(alterQuery)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
			}
		}
		upsertSubDocQuery := fmt.Sprintf("UPDATE %s SET %s='%s' WHERE ID = ?", tableName, columnName, x.Doc)
		errupsertSubDocQuery := cassandraSession.Query(upsertSubDocQuery, key).Exec()
		if errupsertSubDocQuery != nil {
			log.Println("In Cassandra UpsertSubDoc(), error inserting data:", errupsertSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errupsertSubDocQuery, false, extra.Cas, offset)
		}
		var currentValue float64
		mutationSubDocQuery := fmt.Sprintf("SELECT mutated FROM %s WHERE ID = ?", tableName)
		errMutationSubDocQuery := cassandraSession.Query(mutationSubDocQuery, key).Scan(&currentValue)
		if errMutationSubDocQuery != nil {
			log.Println("In Cassandra UpsertSubDoc(), error fetching current mutated field:", errMutationSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errMutationSubDocQuery, false, extra.Cas, offset)
		}
		mutationIncSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=%f WHERE ID = ?", tableName, "mutated", currentValue+1)
		errMutationIncSubDocQuery := cassandraSession.Query(mutationIncSubDocQuery, key).Exec()
		if errMutationIncSubDocQuery != nil {
			log.Println("In Cassandra UpsertSubDoc(), error updating mutated field:", errMutationIncSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errMutationIncSubDocQuery, false, extra.Cas, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) Increment(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) ReplaceSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Table name is missing"), false, extra.Cas, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Keyspace is missing"), false, extra.Cas, offset)
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra ReplaceSubDoc(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCouchbaseSubDocOperationResult(key, keyValues, errSessionCreate, false, extra.Cas, offset)
	}
	for _, x := range keyValues {
		columnName := extra.SubDocPath
		if err := validateStrings(columnName); err != nil {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("SubDocPath is missing"), false, extra.Cas, offset)
		}
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
			}
		}
		replaceSubDocQuery := fmt.Sprintf("UPDATE %s SET %s='%s' WHERE ID = ?", tableName, columnName, x.Doc)
		errreplaceSubDocQuery := cassandraSession.Query(replaceSubDocQuery, key).Exec()
		if errreplaceSubDocQuery != nil {
			log.Println("In Cassandra ReplaceSubDoc(), error inserting data:", errreplaceSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errreplaceSubDocQuery, false, extra.Cas, offset)
		}
		var currentValue float64
		mutationSubDocQuery := fmt.Sprintf("SELECT mutated FROM %s WHERE ID = ?", tableName)
		errMutationSubDocQuery := cassandraSession.Query(mutationSubDocQuery, key).Scan(&currentValue)
		if errMutationSubDocQuery != nil {
			log.Println("In Cassandra ReplaceSubDoc(), error fetching current mutated field:", errMutationSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errMutationSubDocQuery, false, extra.Cas, offset)
		}
		mutationIncSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=%f WHERE ID = ?", tableName, "mutated", currentValue+1)
		errMutationIncSubDocQuery := cassandraSession.Query(mutationIncSubDocQuery, key).Exec()
		if errMutationIncSubDocQuery != nil {
			log.Println("In Cassandra UpsertSubDoc(), error updating mutated field:", errMutationIncSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errMutationIncSubDocQuery, false, extra.Cas, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) ReadSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Table name is missing"), false, extra.Cas, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Keyspace is missing"), false, extra.Cas, offset)
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra ReadSubDoc(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCouchbaseSubDocOperationResult(key, keyValues, errSessionCreate, false, extra.Cas, offset)
	}
	for range keyValues {
		columnName := extra.SubDocPath
		if err := validateStrings(columnName); err != nil {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("SubDocPath is missing"), false, extra.Cas, offset)
		}
		var result map[string]interface{}
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("No subdocs field found."), false, extra.Cas, offset)
		}
		selectSubDocQuery := fmt.Sprintf("SELECT %s FROM %s WHERE ID = ?", columnName, tableName)
		iter := cassandraSession.Query(selectSubDocQuery, key).Iter()
		result = make(map[string]interface{})
		success := iter.MapScan(result)
		if !success {
			if result == nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("No documents found despite successful subdocread."), false, extra.Cas, offset)
			} else if err := iter.Close(); err != nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Unsuccessful READ operation."), false, extra.Cas, offset)
			}
		}
		if result["subdoc"] == "" {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("No subdocs found."), false, extra.Cas, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) DeleteSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Table name is missing"), false, extra.Cas, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Keyspace is missing"), false, extra.Cas, offset)
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra DeleteSubDoc(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCouchbaseSubDocOperationResult(key, keyValues, errSessionCreate, false, extra.Cas, offset)
	}
	for range keyValues {
		columnName := "subDoc"
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
			}
		}
		delSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=? WHERE ID = ?", tableName, columnName)
		errdelSubDocQuery := cassandraSession.Query(delSubDocQuery, nil, key).Exec()
		if errdelSubDocQuery != nil {
			log.Println("In Cassandra DeleteSubDoc(), error inserting data:", errdelSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errdelSubDocQuery, false, extra.Cas, offset)
		}
		mutationClearSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=%f WHERE ID = ?", tableName, "mutated", 0.0)
		errmutationClearSubDocQuery := cassandraSession.Query(mutationClearSubDocQuery, key).Exec()
		if errmutationClearSubDocQuery != nil {
			log.Println("In Cassandra DeleteSubDoc(), error updating mutated field:", errmutationClearSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errmutationClearSubDocQuery, false, extra.Cas, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {

	result := newCassandraBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	if err := validateStrings(extra.Keyspace); err != nil {
		result.failBulk(keyValues, errors.New("Keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("Table name is missing"))
		return result
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Create(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		result.failBulk(keyValues, errSessionCreate)
		return result
	}

	for _, x := range keyValues {
		cassBatchSize := 10
		cassBatchOp := cassandraSession.NewBatch(gocql.LoggedBatch).WithContext(context.TODO())
		var docArg []interface{}
		for i := 0; i < cassBatchSize; i++ {
			// Converting the Document to JSON
			jsonData, errDocToJSON := json.Marshal(x.Doc)
			if errDocToJSON != nil {
				log.Println("In Cassandra Update(), error marshaling JSON:", errDocToJSON)
			}

			docArg = append(docArg, jsonData)
			cassBatchOp.Entries = append(cassBatchOp.Entries, gocql.BatchEntry{
				Stmt:       "INSERT INTO " + extra.Table + " JSON ?",
				Args:       docArg,
				Idempotent: true,
			})
			docArg = nil
		}

		errBulkInsert := cassandraSession.ExecuteBatch(cassBatchOp)
		if errBulkInsert != nil {
			log.Println("In Cassandra CreateBulk(), ExecuteBatch() Error:", errBulkInsert)
			result.failBulk(keyValues, errBulkInsert)
			return result
		}
		cassBatchOp = nil
	}

	for _, x := range keyValues {
		//log.Println("Successfully inserted document with id:", x.Key)
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (c *Cassandra) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newCassandraBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	if err := validateStrings(extra.Keyspace); err != nil {
		result.failBulk(keyValues, errors.New("Keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("Table name is missing"))
		return result
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Update(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		result.failBulk(keyValues, errSessionCreate)
		return result
	}

	for _, x := range keyValues {
		cassBatchSize := 10
		cassBatchOp := cassandraSession.NewBatch(gocql.LoggedBatch).WithContext(context.TODO())
		var docArg []interface{}
		for i := 0; i < cassBatchSize; i++ {
			// Converting the Document to JSON
			jsonData, errDocToJSON := json.Marshal(x.Doc)
			if errDocToJSON != nil {
				log.Println("In Cassandra UpdateBulk(), error marshaling JSON:", errDocToJSON)
			}

			docArg = append(docArg, jsonData)
			cassBatchOp.Entries = append(cassBatchOp.Entries, gocql.BatchEntry{
				Stmt:       "INSERT INTO " + extra.Table + " JSON ? DEFAULT UNSET",
				Args:       docArg,
				Idempotent: true,
			})
			docArg = nil
		}
		errBulkUpsert := cassandraSession.ExecuteBatch(cassBatchOp)
		if errBulkUpsert != nil {
			log.Println("In Cassandra UpdateBulk(), ExecuteBatch() Error:", errBulkUpsert)
			result.failBulk(keyValues, errBulkUpsert)
			return result
		}
		cassBatchOp = nil
	}

	for _, x := range keyValues {
		//log.Println("Successfully inserted document with id:", x.Key)
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (c *Cassandra) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newCassandraBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	keyToOffset := make(map[string]int64)
	keysToString := "("
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		keysToString += "'" + x.Key + "'" + ","
	}
	keysToString = keysToString[:len(keysToString)-1] + ")"
	if err := validateStrings(extra.Keyspace); err != nil {
		result.failBulk(keyValues, errors.New("Keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("Table name is missing"))
		return result
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Read(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		result.failBulk(keyValues, errSessionCreate)
		return result
	}

	query := "SELECT * FROM " + extra.Table + " WHERE ID IN " + keysToString
	iter := cassandraSession.Query(query).Iter()
	if iter.NumRows() != len(keyValues) {
		result.failBulk(keyValues, errors.New("Unable to perform Bulk Read"))
	}
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		result.AddResult(row["id"].(string), nil, nil, false, keyToOffset[row["id"].(string)])
	}
	return result
}

func (c *Cassandra) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newCassandraBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	if err := validateStrings(extra.Keyspace); err != nil {
		result.failBulk(keyValues, errors.New("Keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("Table name is missing"))
		return result
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Delete(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		result.failBulk(keyValues, errSessionCreate)
		return result
	}

	for _, x := range keyValues {
		cassBatchSize := 10
		cassBatchOp := cassandraSession.NewBatch(gocql.UnloggedBatch).WithContext(context.TODO())
		for i := 0; i < cassBatchSize; i++ {
			cassBatchOp.Entries = append(cassBatchOp.Entries, gocql.BatchEntry{
				Stmt:       "DELETE FROM " + extra.Table + " WHERE ID=?",
				Args:       []interface{}{x.Key},
				Idempotent: true,
			})
		}
		errBulkUpdate := cassandraSession.ExecuteBatch(cassBatchOp)
		if errBulkUpdate != nil {
			log.Println("In Cassandra DeleteBulk(), ExecuteBatch() Error:", errBulkUpdate)
			result.failBulk(keyValues, errBulkUpdate)
			return result
		}
		cassBatchOp = nil
	}

	for _, x := range keyValues {
		//log.Println("Successfully inserted document with id:", x.Key)
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (c *Cassandra) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}
