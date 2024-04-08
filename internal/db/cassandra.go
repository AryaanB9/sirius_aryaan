package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"

	"github.com/AryaanB9/sirius_aryaan/internal/sdk_cassandra"
	"github.com/AryaanB9/sirius_aryaan/internal/template"

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

// Operation Result for SubDoc Operations
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

func (c *cassandraSubDocOperationResult) Key() string {
	return c.key
}

func (c *cassandraSubDocOperationResult) Value(subPath string) (interface{}, int64) {
	for _, x := range c.result.keyValue {
		if x.Key == subPath {
			return x.Doc, x.Offset
		}
	}
	return nil, -1
}

func (c *cassandraSubDocOperationResult) Values() []KeyValue {
	return c.result.keyValue
}

func (c *cassandraSubDocOperationResult) GetError() error {
	return c.result.error
}

func (c *cassandraSubDocOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (c *cassandraSubDocOperationResult) GetOffset() int64 {
	return c.result.offset
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
	if err := validateStrings(connStr, username, password); err != nil {
		log.Println("cassandra warm up:", err)
		return fmt.Errorf("cassandra warm up: %w", err)
	}

	cassSession, errSession := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
	if errSession != nil {
		log.Println("cassandra warm up:", errSession)
		return fmt.Errorf("cassandra warm up: %w", errSession)
	}

	// Checking if the cluster is reachable or not
	errQuery := cassSession.Query("SELECT cluster_name FROM system.local").Exec()
	if errQuery != nil {
		log.Println("cassandra warm up: query cluster to check status:", errQuery)
		return fmt.Errorf("cassandra warm up: query cluster to check status: %w", errQuery)
	}
	return nil
}

func (c *Cassandra) Close(connStr string, _ Extras) error {
	return c.CassandraConnectionManager.Disconnect(connStr)
}

func (c *Cassandra) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	if err := validateStrings(extra.Keyspace); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("insert document into cassandra: keyspace name is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Table); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("insert document into cassandra: table name is missing"), false,
			keyValue.Offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("insert document into cassandra:", errSessionCreate)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, fmt.Errorf("insert document into cassandra: %w", errSessionCreate),
			false, keyValue.Offset)
	}

	// Converting the Document to JSON
	jsonData, errDocToJSON := json.Marshal(keyValue.Doc)
	if errDocToJSON != nil {
		log.Println("insert document into cassandra: marshal json:", errDocToJSON)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, fmt.Errorf("insert document into cassandra: marshal json: %w", errDocToJSON),
			false, keyValue.Offset)
	}

	insertQuery := "INSERT INTO " + extra.Table + " JSON ?"
	errInsert := cassandraSession.Query(insertQuery, jsonData).Exec()
	if errInsert != nil {
		log.Println("insert document into cassandra:", errInsert)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, fmt.Errorf("insert document into cassandra: %w", errInsert), false, keyValue.Offset)
	}
	return newCassandraOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (c *Cassandra) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	if err := validateStrings(extra.Keyspace); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("update document in cassandra: keyspace name is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Table); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("update document in cassandra: table name is missing"), false,
			keyValue.Offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("update document in cassandra:", errSessionCreate)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, fmt.Errorf("update document in cassandra: %w", errSessionCreate),
			false, keyValue.Offset)
	}

	// Converting the Document to JSON
	jsonData, errDocToJSON := json.Marshal(keyValue.Doc)
	if errDocToJSON != nil {
		log.Println("update document in cassandra: marshal json:", errDocToJSON)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, fmt.Errorf("update document in cassandra: marshal json: %w", errDocToJSON),
			false, keyValue.Offset)
	}

	updateQuery := "INSERT INTO " + extra.Table + " JSON ? DEFAULT UNSET"
	errUpdate := cassandraSession.Query(updateQuery, jsonData).Exec()
	if errUpdate != nil {
		log.Println("update document in cassandra:", errUpdate)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, fmt.Errorf("update document in cassandra: %w", errUpdate),
			false, keyValue.Offset)
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
		return newCassandraOperationResult(key, nil, errors.New("read document from cassandra: table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("read document from cassandra: keyspace is missing"), false, offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("read document from cassandra:", errSessionCreate)
		return newCassandraOperationResult(key, nil, fmt.Errorf("read document from cassandra: %w", errSessionCreate),
			false, offset)
	}

	query := "SELECT * FROM " + keyspaceName + "." + tableName + " WHERE ID = ?"
	iter := cassandraSession.Query(query, key).Iter()

	cassReadResult := make(map[string]interface{})
	success := iter.MapScan(cassReadResult)
	if !success {
		if cassReadResult == nil {
			return newCassandraOperationResult(key, nil,
				fmt.Errorf("read document from cassandra: result is nil even after successful read operation"), false, offset)

		} else if err := iter.Close(); err != nil {
			return newCassandraOperationResult(key, nil,
				fmt.Errorf("read document from cassandra: %w", err), false, offset)
		}
	}
	return newCassandraOperationResult(key, cassReadResult, nil, true, offset)
}

func (c *Cassandra) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(key, nil, err, false, offset)
	}

	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("delete document from cassandra: table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("delete document from cassandra: keyspace is missing"), false, offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("delete document from cassandra:", errSessionCreate)
		return newCassandraOperationResult(key, nil, fmt.Errorf("delete document from cassandra: %w", errSessionCreate),
			false, offset)
	}

	query := "DELETE FROM " + keyspaceName + "." + tableName + " WHERE ID = ?"
	err := cassandraSession.Query(query, key).Exec()
	if err != nil {
		return newCassandraOperationResult(key, nil,
			fmt.Errorf("delete document from cassandra: %w", err), false, offset)
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
		return newCassandraOperationResult(key, nil, errors.New("touch document in cassandra: table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("touch document in cassandra: keyspace is missing"), false, offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("touch document in cassandra:", errSessionCreate)
		return newCassandraOperationResult(key, nil, fmt.Errorf("touch document in cassandra: %w", errSessionCreate),
			false, offset)
	}

	query := fmt.Sprintf("UPDATE %s.%s USING TTL %d WHERE ID = ?", keyspaceName, tableName, extra.Expiry)
	if err := cassandraSession.Query(query, key).Exec(); err != nil {
		return newCassandraOperationResult(key, nil, fmt.Errorf("touch document in cassandra: %w", err), false, offset)
	}
	return newCassandraOperationResult(key, nil, nil, true, offset)
}

func (c *Cassandra) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraSubDocOperationResult(key, keyValues, err, false, offset)
	}

	tableName := extra.Table
	keyspaceName := extra.Keyspace
	columnName := extra.SubDocPath
	if err := validateStrings(tableName); err != nil {
		return newCassandraSubDocOperationResult(key, nil, errors.New("insert sub document in cassandra: table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraSubDocOperationResult(key, nil, errors.New("insert sub document in cassandra: keyspace is missing"), false, offset)
	}
	if err := validateStrings(columnName); err != nil {
		return newCassandraSubDocOperationResult(key, keyValues, errors.New("insert sub document in cassandra: sub doc path is missing"), false, offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("insert sub document in cassandra:", errSessionCreate)
		return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("insert sub document in cassandra: %w", errSessionCreate), false, offset)
	}

	for _, x := range keyValues {
		// Checking if the column for sub doc exists or not
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				log.Println("insert sub document in cassandra: add sub doc column:", err)
				return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("insert sub document in cassandra: add sub doc column: %w", err), false, offset)
			}
		}

		// Inserting the sub doc
		insertSubDocQuery := fmt.Sprintf("UPDATE %s SET %s='%s' WHERE ID = ?", tableName, columnName, x.Doc)
		err := cassandraSession.Query(insertSubDocQuery, key).Exec()
		if err != nil {
			log.Println("insert sub document in cassandra:", err)
			return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("insert sub document in cassandra: %w", err), false, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) UpsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraSubDocOperationResult(key, keyValues, err, false, offset)
	}

	tableName := extra.Table
	keyspaceName := extra.Keyspace
	columnName := extra.SubDocPath
	if err := validateStrings(tableName); err != nil {
		return newCassandraSubDocOperationResult(key, nil, errors.New("update sub document in cassandra: table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraSubDocOperationResult(key, nil, errors.New("update sub document in cassandra: keyspace is missing"), false, offset)
	}
	if err := validateStrings(columnName); err != nil {
		return newCassandraSubDocOperationResult(key, keyValues, errors.New("update sub document in cassandra: sub doc path is missing"), false, offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("update sub document in cassandra:", errSessionCreate)
		return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("update sub document in cassandra: %w", errSessionCreate), false, offset)
	}

	for _, x := range keyValues {
		// Checking if the column for sub doc exists or not
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				log.Println("update sub document in cassandra: add sub doc column:", err)
				return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("update sub document in cassandra: add sub doc column: %w", err), false, offset)
			}
		}

		// Updating the Sub Document
		upsertSubDocQuery := fmt.Sprintf("UPDATE %s SET %s='%s' WHERE ID = ?", tableName, columnName, x.Doc)
		errUpsertSubDocQuery := cassandraSession.Query(upsertSubDocQuery, key).Exec()
		if errUpsertSubDocQuery != nil {
			log.Println("update sub document in cassandra:", errUpsertSubDocQuery)
			return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("update sub document in cassandra: %w", errUpsertSubDocQuery), false, offset)
		}

		// Incrementing the 'mutated' Field
		var currentValue float64
		mutationSubDocQuery := fmt.Sprintf("SELECT mutated FROM %s WHERE ID = ?", tableName)
		errMutationSubDocQuery := cassandraSession.Query(mutationSubDocQuery, key).Scan(&currentValue)
		if errMutationSubDocQuery != nil {
			log.Println("update sub document in cassandra: fetch 'mutated' field:", errMutationSubDocQuery)
			return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("update sub document in cassandra: fetch 'mutated' field: %w", errMutationSubDocQuery), false, offset)
		}

		mutationIncSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=%f WHERE ID = ?", tableName, "mutated", currentValue+1)
		errMutationIncSubDocQuery := cassandraSession.Query(mutationIncSubDocQuery, key).Exec()
		if errMutationIncSubDocQuery != nil {
			log.Println("update sub document in cassandra: increment 'mutated' field:", errMutationIncSubDocQuery)
			return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("update sub document in cassandra: increment 'mutated' field: %w", errMutationIncSubDocQuery), false, offset)
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
		return newCassandraSubDocOperationResult(key, keyValues, err, false, offset)
	}

	tableName := extra.Table
	keyspaceName := extra.Keyspace
	columnName := extra.SubDocPath
	if err := validateStrings(tableName); err != nil {
		return newCassandraSubDocOperationResult(key, nil, errors.New("replace sub document in cassandra: table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraSubDocOperationResult(key, nil, errors.New("replace sub document in cassandra: keyspace is missing"), false, offset)
	}
	if err := validateStrings(columnName); err != nil {
		return newCassandraSubDocOperationResult(key, keyValues, errors.New("replace sub document in cassandra: sub doc path is missing"), false, offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("replace sub document in cassandra:", errSessionCreate)
		return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("replace sub document in cassandra: %w", errSessionCreate), false, offset)
	}

	for _, x := range keyValues {
		// Checking if the column for sub doc exists or not
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				log.Println("replace sub document in cassandra: add sub doc column:", err)
				return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("replace sub document in cassandra: add sub doc column: %w", err), false, offset)
			}
		}

		// Replacing the Sub Document
		replaceSubDocQuery := fmt.Sprintf("UPDATE %s SET %s='%s' WHERE ID = ?", tableName, columnName, x.Doc)
		errReplaceSubDocQuery := cassandraSession.Query(replaceSubDocQuery, key).Exec()
		if errReplaceSubDocQuery != nil {
			log.Println("replace sub document in cassandra:", errReplaceSubDocQuery)
			return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("replace sub document in cassandra: %w", errReplaceSubDocQuery), false, offset)
		}

		// Incrementing the 'mutated' Field
		var currentValue float64
		mutationSubDocQuery := fmt.Sprintf("SELECT mutated FROM %s WHERE ID = ?", tableName)
		errMutationSubDocQuery := cassandraSession.Query(mutationSubDocQuery, key).Scan(&currentValue)
		if errMutationSubDocQuery != nil {
			log.Println("replace sub document in cassandra: fetch 'mutated' field:", errMutationSubDocQuery)
			return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("replace sub document in cassandra: fetch 'mutated' field: %w", errMutationSubDocQuery), false, offset)
		}

		mutationIncSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=%f WHERE ID = ?", tableName, "mutated", currentValue+1)
		errMutationIncSubDocQuery := cassandraSession.Query(mutationIncSubDocQuery, key).Exec()
		if errMutationIncSubDocQuery != nil {
			log.Println("replace sub document in cassandra: increment 'mutated' field:", errMutationIncSubDocQuery)
			return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("replace sub document in cassandra: increment 'mutated' field: %w", errMutationIncSubDocQuery), false, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) ReadSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraSubDocOperationResult(key, keyValues, err, false, offset)
	}

	tableName := extra.Table
	keyspaceName := extra.Keyspace
	columnName := extra.SubDocPath
	if err := validateStrings(tableName); err != nil {
		return newCassandraSubDocOperationResult(key, nil, errors.New("read sub document from cassandra: table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraSubDocOperationResult(key, nil, errors.New("read sub document from cassandra: keyspace is missing"), false, offset)
	}
	if err := validateStrings(columnName); err != nil {
		return newCassandraSubDocOperationResult(key, keyValues, errors.New("read sub document from cassandra: sub doc path is missing"), false, offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("read sub document from cassandra:", errSessionCreate)
		return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("read sub document from cassandra: %w", errSessionCreate), false, offset)
	}

	for range keyValues {
		// Checking if the column for sub doc exists or not
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			return newCassandraSubDocOperationResult(key, keyValues, errors.New("read sub document from cassandra: sub doc field not found"), false, offset)
		}

		// Reading the sub document
		selectSubDocQuery := fmt.Sprintf("SELECT %s FROM %s WHERE ID = ?", columnName, tableName)
		iter := cassandraSession.Query(selectSubDocQuery, key).Iter()
		result := make(map[string]interface{})
		success := iter.MapScan(result)
		if !success {
			if result == nil {
				return newCassandraSubDocOperationResult(key, keyValues, errors.New("read sub document from cassandra: result is nil after successful read"), false, offset)
			} else if err := iter.Close(); err != nil {
				return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("read sub document from cassandra: %w", err), false, offset)
			}
		}
		if result[columnName] == "" {
			return newCassandraSubDocOperationResult(key, keyValues, errors.New("read sub document from cassandra: sub doc field empty"), false, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) DeleteSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraSubDocOperationResult(key, keyValues, err, false, offset)
	}

	tableName := extra.Table
	keyspaceName := extra.Keyspace
	columnName := extra.SubDocPath
	if err := validateStrings(tableName); err != nil {
		return newCassandraSubDocOperationResult(key, nil, errors.New("delete sub document from cassandra: table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraSubDocOperationResult(key, nil, errors.New("delete sub document from cassandra: keyspace is missing"), false, offset)
	}
	if err := validateStrings(columnName); err != nil {
		return newCassandraSubDocOperationResult(key, keyValues, errors.New("delete sub document from cassandra: sub doc path is missing"), false, offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("delete sub document from cassandra:", errSessionCreate)
		return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("delete sub document from cassandra: %w", errSessionCreate), false, offset)
	}

	for range keyValues {
		// Checking if the column for sub doc exists or not
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			return newCassandraSubDocOperationResult(key, keyValues, errors.New("delete sub document from cassandra: sub doc column is not present"), false, offset)
		}

		// Deleting the sub document
		delSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=? WHERE ID = ?", tableName, columnName)
		errDelSubDocQuery := cassandraSession.Query(delSubDocQuery, nil, key).Exec()
		if errDelSubDocQuery != nil {
			log.Println("delete sub document from cassandra:", errDelSubDocQuery)
			return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("delete sub document from cassandra: %w", errDelSubDocQuery), false, offset)
		}

		// Updating the 'mutated' field to 0
		mutationClearSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=%f WHERE ID = ?", tableName, "mutated", 0.0)
		errMutationClearSubDocQuery := cassandraSession.Query(mutationClearSubDocQuery, key).Exec()
		if errMutationClearSubDocQuery != nil {
			log.Println("delete sub document from cassandra: update 'mutated' field:", errMutationClearSubDocQuery)
			return newCassandraSubDocOperationResult(key, keyValues, fmt.Errorf("delete sub document from cassandra: update 'mutated' field: %w", errMutationClearSubDocQuery), false, offset)
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
		result.failBulk(keyValues, errors.New("bulk insert documents into cassandra: keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("bulk insert documents into cassandra: table name is missing"))
		return result
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("bulk insert documents into cassandra:", errSessionCreate)
		result.failBulk(keyValues, fmt.Errorf("bulk insert documents into cassandra: %w", errSessionCreate))
		return result
	}

	cassBatchSize := 10
	cassBatchOp := cassandraSession.NewBatch(gocql.LoggedBatch).WithContext(context.TODO())

	for i, x := range keyValues {

		// Converting the Document to JSON
		jsonData, errDocToJSON := json.Marshal(x.Doc)
		if errDocToJSON != nil {
			log.Println("bulk insert documents into cassandra: marshal json:", errDocToJSON)
			result.failBulk(keyValues, fmt.Errorf("bulk insert documents into cassandra: marshal JSON: %w", errDocToJSON))
			return result
		}

		cassBatchOp.Entries = append(cassBatchOp.Entries, gocql.BatchEntry{
			Stmt:       "INSERT INTO " + extra.Table + " JSON ?",
			Args:       []interface{}{jsonData},
			Idempotent: true,
		})

		if (i+1)%cassBatchSize == 0 {
			errBulkInsert := cassandraSession.ExecuteBatch(cassBatchOp)
			if errBulkInsert != nil {
				log.Println("bulk insert documents into cassandra:", errBulkInsert)
				result.failBulk(keyValues, fmt.Errorf("bulk insert documents into cassandra: %w", errBulkInsert))
				return result
			}

			cassBatchOp = nil
			cassBatchOp = cassandraSession.NewBatch(gocql.LoggedBatch).WithContext(context.TODO())
		}
	}

	// For remaining keys
	if len(keyValues)%cassBatchSize != 0 {
		errBulkInsert := cassandraSession.ExecuteBatch(cassBatchOp)
		if errBulkInsert != nil {
			log.Println("bulk insert documents into cassandra:", errBulkInsert)
			result.failBulk(keyValues, fmt.Errorf("bulk insert documents into cassandra: %w", errBulkInsert))
			return result
		}
	}

	for _, x := range keyValues {
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
		result.failBulk(keyValues, errors.New("bulk update documents into cassandra: keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("bulk update documents into cassandra: table name is missing"))
		return result
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("bulk update documents into cassandra:", errSessionCreate)
		result.failBulk(keyValues, fmt.Errorf("bulk update documents into cassandra: %w", errSessionCreate))
		return result
	}

	cassBatchSize := 10
	cassBatchOp := cassandraSession.NewBatch(gocql.LoggedBatch).WithContext(context.TODO())

	for i, x := range keyValues {

		// Converting the Document to JSON
		jsonData, errDocToJSON := json.Marshal(x.Doc)
		if errDocToJSON != nil {
			log.Println("bulk update documents into cassandra: marshal json:", errDocToJSON)
			result.failBulk(keyValues, fmt.Errorf("bulk update documents into cassandra: marshal json: %w", errDocToJSON))
			return result
		}

		cassBatchOp.Entries = append(cassBatchOp.Entries, gocql.BatchEntry{
			Stmt:       "INSERT INTO " + extra.Table + " JSON ? DEFAULT UNSET",
			Args:       []interface{}{jsonData},
			Idempotent: true,
		})

		if (i+1)%cassBatchSize == 0 {
			errBulkUpsert := cassandraSession.ExecuteBatch(cassBatchOp)
			if errBulkUpsert != nil {
				log.Println("bulk update documents into cassandra:", errBulkUpsert)
				result.failBulk(keyValues, fmt.Errorf("bulk update documents into cassandra: %w", errBulkUpsert))
				return result
			}
			cassBatchOp = nil
			cassBatchOp = cassandraSession.NewBatch(gocql.LoggedBatch).WithContext(context.TODO())
		}
	}

	// For remaining keys
	if len(keyValues)%cassBatchSize != 0 {
		errBulkUpsert := cassandraSession.ExecuteBatch(cassBatchOp)
		if errBulkUpsert != nil {
			log.Println("bulk update documents into cassandra:", errBulkUpsert)
			result.failBulk(keyValues, fmt.Errorf("bulk update documents into cassandra: %w", errBulkUpsert))
			return result
		}
	}

	for _, x := range keyValues {
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
		result.failBulk(keyValues, errors.New("bulk read documents from cassandra: keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("bulk read documents from cassandra: table name is missing"))
		return result
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("bulk read documents from cassandra:", errSessionCreate)
		result.failBulk(keyValues, fmt.Errorf("bulk read documents from cassandra: %w", errSessionCreate))
		return result
	}

	query := "SELECT * FROM " + extra.Table + " WHERE ID IN " + keysToString
	iter := cassandraSession.Query(query).Iter()
	if iter.NumRows() != len(keyValues) {
		result.failBulk(keyValues, fmt.Errorf("bulk read documents from cassandra: number of documents read not equal to number of documents requested"))
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
		result.failBulk(keyValues, errors.New("bulk delete documents from cassandra: keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("bulk delete documents from cassandra: table name is missing"))
		return result
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("bulk delete documents from cassandra:", errSessionCreate)
		result.failBulk(keyValues, fmt.Errorf("bulk delete documents from cassandra: %w", errSessionCreate))
		return result
	}

	cassBatchSize := 10
	cassBatchOp := cassandraSession.NewBatch(gocql.UnloggedBatch).WithContext(context.TODO())

	for i, x := range keyValues {
		cassBatchOp.Entries = append(cassBatchOp.Entries, gocql.BatchEntry{
			Stmt:       "DELETE FROM " + extra.Table + " WHERE ID=?",
			Args:       []interface{}{x.Key},
			Idempotent: true,
		})

		if (i+1)%cassBatchSize == 0 {
			errBulkUpdate := cassandraSession.ExecuteBatch(cassBatchOp)
			if errBulkUpdate != nil {
				log.Println("bulk delete documents from cassandra:", errBulkUpdate)
				result.failBulk(keyValues, fmt.Errorf("bulk delete documents from cassandra: %w", errBulkUpdate))
				return result
			}

			cassBatchOp = nil
			cassBatchOp = cassandraSession.NewBatch(gocql.UnloggedBatch).WithContext(context.TODO())
		}
	}

	// For remaining keys
	if len(keyValues)%cassBatchSize != 0 {
		errBulkUpdate := cassandraSession.ExecuteBatch(cassBatchOp)
		if errBulkUpdate != nil {
			log.Println("bulk delete documents from cassandra:", errBulkUpdate)
			result.failBulk(keyValues, fmt.Errorf("bulk delete documents from cassandra: %w", errBulkUpdate))
			return result
		}
	}

	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}

	return result
}

func (c *Cassandra) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

// CreateDatabase creates a Keyspace in Cassandra cluster or Table in a Keyspace.
/*
 *	If only Keyspace name is provided, then a Keyspace will be created if it does not exist.
 *	If both Keyspace and Table name are provided, then a Table will be created in the Keyspace.
 *	NOTE: While creating Keyspace, make sure to provide Extras.CassandraClass and Extras.ReplicationFactor
 *	NOTE: While creating Table, make sure to provide OperationConfig.Template as it will be used to retrieve correct cassandra schema.
 */
func (c *Cassandra) CreateDatabase(connStr, username, password string, extra Extras, templateName string, docSize int) (string, error) {

	resultString := ""
	if connStr == "" || password == "" || username == "" {
		return "", errors.New("create cassandra keyspace or table: connection string or auth parameters not provided")
	}

	if extra.Keyspace == "" {
		return "", errors.New("create cassandra keyspace or table: keyspace name not provided")

	} else if extra.Keyspace != "" && extra.Table == "" {
		// Creating a new Keyspace
		if extra.CassandraClass == "" || extra.ReplicationFactor == 0 {
			log.Println("create keyspace in cassandra: cassandra class or replication factor not provided for creating keyspace")
			return "", errors.New("create keyspace in cassandra: cassandra class or replication factor not provided for creating keyspace")
		}

		cassandraSession, errCreateSession := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
		if errCreateSession != nil {
			log.Println("create keyspace in cassandra:", errCreateSession)
			return "", fmt.Errorf("create keyspace in cassandra: %w", errCreateSession)
		}

		createKeyspaceQuery := fmt.Sprintf(`
							CREATE KEYSPACE IF NOT EXISTS %s
							WITH replication = {
								'class': '%s',
								'replication_factor': %v
							};`, extra.Keyspace, extra.CassandraClass, extra.ReplicationFactor)
		errCreateKeyspace := cassandraSession.Query(createKeyspaceQuery).Exec()
		if errCreateKeyspace != nil {
			log.Println("create keyspace in cassandra: unable to create keyspace:", errCreateKeyspace)
			return "", fmt.Errorf("create keyspace in cassandra: unable to create keyspace: %w", errCreateKeyspace)
		}

		resultString = fmt.Sprintf("Keyspace '%s' created successfully.", extra.Keyspace)

	} else if extra.Keyspace != "" && extra.Table != "" {
		// Creating a new Table. Need to have Template.
		// And, we have to check if Keyspace is created or not.
		if templateName == "" {
			log.Println("create table in cassandra: template name not provided")
			return "", errors.New("create table in cassandra: template name not provided")
		}

		// First getting client on Cluster and checking if the Keyspace exists or not
		cassandraSession, errCreateSession := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
		if errCreateSession != nil {
			log.Println("create table in cassandra:", errCreateSession)
			return "", fmt.Errorf("create table in cassandra: %w", errCreateSession)
		}

		var count int
		checkKeyspaceQuery := fmt.Sprintf("SELECT count(*) FROM system_schema.keyspaces WHERE keyspace_name = '%s'", extra.Keyspace)
		if err := cassandraSession.Query(checkKeyspaceQuery).Scan(&count); err != nil {
			log.Println("create table in cassandra: check keyspace existence:", err.Error())
			return "", fmt.Errorf("create table in cassandra: check keyspace existence: %w", err)
		}

		if count <= 0 {
			// Creating the keyspace as it does not exist
			if extra.CassandraClass == "" || extra.ReplicationFactor == 0 {
				log.Println("create table in cassandra: cassandra class or replication factor not provided for creating keyspace")
				return "", errors.New("create table in cassandra: cassandra class or replication factor not provided for creating keyspace")
			}

			createKeyspaceQuery := fmt.Sprintf(`
							CREATE KEYSPACE IF NOT EXISTS %s
							WITH replication = {
								'class': '%s',
								'replication_factor': %v
							};`, extra.Keyspace, extra.CassandraClass, extra.ReplicationFactor)
			errCreateKeyspace := cassandraSession.Query(createKeyspaceQuery).Exec()
			if errCreateKeyspace != nil {
				log.Println("create keyspace in cassandra:", errCreateKeyspace)
				return "", fmt.Errorf("create keyspace in cassandra: %w", errCreateKeyspace)
			}
		}

		cassandraSession, errCreateSession = c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
		if errCreateSession != nil {
			log.Println("create table in cassandra:", errCreateSession)
			return "", fmt.Errorf("create table in cassandra: %w", errCreateSession)
		}

		cassQueries, err := template.GetCassandraSchema(templateName, extra.Table)
		if err != nil {
			log.Println("create table in cassandra:", err)
			return "", fmt.Errorf("create table in cassandra: %w", err)
		}

		for _, cassQuery := range cassQueries {
			err = cassandraSession.Query(cassQuery).Exec()
			if err != nil {
				log.Println("create table in cassandra:", err)
				return "", fmt.Errorf("create table in cassandra: %w", err)
			}
		}

		resultString = fmt.Sprintf("Table '%s' created successfully in Keyspace '%s'.", extra.Table, extra.Keyspace)
	}

	return resultString, nil
}

// DeleteDatabase deletes a keyspace or table in a cassandra cluster.
/*
 *	If only keyspace name is provided, then the whole keyspace along with all its tables will be deleted.
 *	If keyspace and table name both are provided, then only the table will be deleted.
 */
func (c *Cassandra) DeleteDatabase(connStr, username, password string, extra Extras) (string, error) {

	resultString := ""
	if connStr == "" || password == "" || username == "" {
		return "", errors.New("delete cassandra keyspace or table: connection string or auth parameters not provided")
	}

	if extra.Keyspace == "" {
		return "", errors.New("delete cassandra keyspace or table: keyspace name not provided")

	} else if extra.Keyspace != "" && extra.Table == "" {

		// Deleting the Keyspace in given Cassandra cluster
		cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
		if errSessionCreate != nil {
			log.Println("delete keyspace from cassandra:", errSessionCreate)
			return "", fmt.Errorf("delete keyspace from cassandra: %w", errSessionCreate)
		}

		dropKeyspaceQuery := fmt.Sprintf("DROP KEYSPACE %s", extra.Keyspace)
		errDropKeyspace := cassandraSession.Query(dropKeyspaceQuery).Exec()
		if errDropKeyspace != nil {
			log.Println("delete keyspace from cassandra:", errDropKeyspace)
			return "", fmt.Errorf("delete keyspace from cassandra: %w", errDropKeyspace)
		}

		resultString = fmt.Sprintf("Keyspace '%s' deleted successfully.", extra.Keyspace)

	} else if extra.Keyspace != "" && extra.Table != "" {
		// Deleting the Table in given Keyspace
		cassandraSession, errCreateSession := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
		if errCreateSession != nil {
			log.Println("delete table from cassandra:", errCreateSession)
			return "", fmt.Errorf("delete table from cassandra: %w", errCreateSession)
		}

		dropTableQuery := fmt.Sprintf("DROP TABLE %s", extra.Table)
		errDropTable := cassandraSession.Query(dropTableQuery).Exec()
		if errDropTable != nil {
			log.Println("delete table from cassandra:", errDropTable)
			return "", fmt.Errorf("delete table from cassandra: %w", errDropTable)
		}

		resultString = fmt.Sprintf("Table '%s' deleted successfully from Keyspace '%s'", extra.Table, extra.Keyspace)
	}
	return resultString, nil
}

func (c *Cassandra) ListDatabase(connStr, username, password string, extra Extras) (any, error) {

	dbList := make(map[string][]string)

	if connStr == "" || password == "" || username == "" {
		return nil, errors.New("list cassandra keyspace(s) or table(s): connection string or auth parameters not provided")
	}

	if extra.Keyspace == "" {
		// Since, Keyspace name is not provided, returning all the Keyspaces in the cluster
		cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
		if errSessionCreate != nil {
			log.Println("list cassandra keyspace(s):", errSessionCreate)
			return nil, fmt.Errorf("list cassandra keyspace(s): %w", errSessionCreate)
		}

		var keyspaceName string
		keyspaces := make([]string, 0)

		//listKeyspaceQuery := "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name != 'system' AND keyspace_name != 'system_traces' AND keyspace_name != 'system_auth' AND keyspace_name != 'system_distributed'"
		listKeyspaceQuery := "SELECT keyspace_name FROM system_schema.keyspaces"
		iterKeyspaces := cassandraSession.Query(listKeyspaceQuery).Iter()

		for iterKeyspaces.Scan(&keyspaceName) {
			keyspaces = append(keyspaces, keyspaceName)
		}
		if err := iterKeyspaces.Close(); err != nil {
			log.Println("list cassandra keyspace(s): iterate keyspace names:", err)
			return nil, fmt.Errorf("iterating keyspaces names: iterate keyspace names: %w", err)
		}

		// Appending all the Keyspace names to output
		for _, keyspaceN := range keyspaces {
			dbList[extra.Keyspace] = append(dbList[extra.Keyspace], keyspaceN)
		}

		if dbList == nil || len(keyspaces) == 0 {
			return nil, errors.New("list cassandra keyspace(s): no keyspaces found")
		}

	} else {
		// Since, Keyspace name is provided, returning all the Tables present in the Keyspace
		cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
		if errSessionCreate != nil {
			log.Println("list cassandra table(s):", errSessionCreate)
			return nil, fmt.Errorf("list cassandra table(s): %w", errSessionCreate)
		}

		var tableName string
		tables := make([]string, 0)

		listTableQuery := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
		iterTables := cassandraSession.Query(listTableQuery, extra.Keyspace).Iter()

		for iterTables.Scan(&tableName) {
			tables = append(tables, tableName)
		}
		if err := iterTables.Close(); err != nil {
			log.Println("list cassandra table(s): iterate table names:", err)
			return nil, fmt.Errorf("list cassandra table(s): iterate table names: %w", err)
		}

		for _, tableN := range tables {
			dbList[extra.Keyspace] = append(dbList[extra.Keyspace], tableN)
		}
		if dbList == nil {
			return nil, fmt.Errorf("list cassandra table(s): no tables found for keyspace '%s'", extra.Keyspace)
		}
	}
	return dbList, nil
}

// Count returns the number of rows present in a Cassandra Keyspace.Table.
/*
 *	If there is an error returned then count is returned as -1.
 *	Extras.DbOnLocal == "true", states that Sirius is running on the same machine as Cassandra.
	This way we can get the count in a much faster way and for a large number of rows.
 *	If Sirius is not running on the same machine as Cassandra, then we have to query to get the count of rows.
*/
func (c *Cassandra) Count(connStr, username, password string, extra Extras) (int64, error) {

	var count int64
	if connStr == "" || password == "" || username == "" {
		return -1, errors.New("listing count of rows of cassandra table: connection string or auth parameters not provided")
	}
	if extra.Keyspace == "" {
		return -1, errors.New("listing count of rows of cassandra table: keyspace name not provided")
	}
	if extra.Table == "" {
		return -1, errors.New("listing count of rows of cassandra table: table name not provided")
	}
	if extra.DbOnLocal == "" {
		return -1, errors.New("listing count of rows of cassandra table: database on local is not provided")
	}

	if extra.DbOnLocal == "true" {
		// If cassandra is present on the same machine as sirius
		cmd := exec.Command("sh", "-c", "cqlsh -e \"copy "+extra.Keyspace+"."+extra.Table+" (id) to '/dev/null'\" | sed -n 5p | sed 's/ .*//'")
		cmdOutput, err := cmd.Output()
		if err != nil {
			return -1, errors.New("listing count of rows of cassandra table: unable to parse command output")
		}

		count, err = strconv.ParseInt(strings.TrimSpace(string(cmdOutput)), 10, 64)
		if err != nil {
			return -1, errors.New("listing count of rows of cassandra table: unable to convert command output to an 64 bit integer")
		}

	} else {
		// If cassandra is hosted on another machine
		cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
		if errSessionCreate != nil {
			log.Println("listing count of rows of cassandra table:", errSessionCreate)
			return -1, fmt.Errorf("listing count of rows of cassandra table: %w", errSessionCreate)
		}

		countQuery := "SELECT COUNT(*) FROM " + extra.Table
		if errCount := cassandraSession.Query(countQuery).Scan(&count); errCount != nil {
			log.Println("listing count of rows of cassandra table:", errCount)
			return 0, fmt.Errorf("listing count of rows of cassandra table: %w", errCount)
		}
	}
	return count, nil
}
