package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/barkha06/sirius/internal/sdk_mysql"
	_ "github.com/go-sql-driver/mysql"
)

type Sql struct {
	connectionManager *sdk_mysql.SqlConnectionManager
}

type perSqlDocResult struct {
	value  interface{}
	error  error
	status bool
	offset int64
}

type sqlOperationResult struct {
	key    string
	result perSqlDocResult
}

func NewSqlConnectionManager() *Sql {
	return &Sql{
		connectionManager: sdk_mysql.ConfigSqlConnectionManager(),
	}
}

// Operation Results for Single Operations like Create, Update, Touch and Delete

func newSqlOperationResult(key string, value interface{}, err error, status bool, offset int64) *sqlOperationResult {
	return &sqlOperationResult{
		key: key,
		result: perSqlDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (m *sqlOperationResult) Key() string {
	return m.key
}

func (m *sqlOperationResult) Value() interface{} {
	return m.result.value
}

func (m *sqlOperationResult) GetStatus() bool {
	return m.result.status
}

func (m *sqlOperationResult) GetError() error {
	return m.result.error
}

func (m *sqlOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (m *sqlOperationResult) GetOffset() int64 {
	return m.result.offset
}

// Operation Results for Bulk Operations like Bulk-Create, Bulk-Update, Bulk-Touch and Bulk-Delete

type sqlBulkOperationResult struct {
	keyValues map[string]perSqlDocResult
}

func newSqlBulkOperation() *sqlBulkOperationResult {
	return &sqlBulkOperationResult{
		keyValues: make(map[string]perSqlDocResult),
	}
}

func (m *sqlBulkOperationResult) AddResult(key string, value interface{}, err error, status bool, offset int64) {
	m.keyValues[key] = perSqlDocResult{
		value:  value,
		error:  err,
		status: status,
		offset: offset,
	}
}

func (m *sqlBulkOperationResult) Value(key string) interface{} {
	if x, ok := m.keyValues[key]; ok {
		return x.value
	}
	return nil
}

func (m *sqlBulkOperationResult) GetStatus(key string) bool {
	if x, ok := m.keyValues[key]; ok {
		return x.status
	}
	return false
}

func (m *sqlBulkOperationResult) GetError(key string) error {
	if x, ok := m.keyValues[key]; ok {
		return x.error
	}
	return errors.New("Key not found in bulk operation")
}

func (m *sqlBulkOperationResult) GetExtra(key string) map[string]any {
	if _, ok := m.keyValues[key]; ok {
		return map[string]any{}
	}
	return nil
}

func (m *sqlBulkOperationResult) GetOffset(key string) int64 {
	if x, ok := m.keyValues[key]; ok {
		return x.offset
	}
	return -1
}

func (m *sqlBulkOperationResult) failBulk(keyValue []KeyValue, err error) {
	for _, x := range keyValue {
		m.keyValues[x.Key] = perSqlDocResult{
			value:  x.Doc,
			error:  err,
			status: false,
		}
	}
}

func (m *sqlBulkOperationResult) GetSize() int {
	return len(m.keyValues)
}

// Operation Result for SubDoc Operations
type perSqlSubDocResult struct {
	keyValue []KeyValue
	error    error
	status   bool
	offset   int64
}

type sqlSubDocOperationResult struct {
	key    string
	result perSqlSubDocResult
}

func newSqlSubDocOperationResult(key string, keyValue []KeyValue, err error, status bool, offset int64) *sqlSubDocOperationResult {
	return &sqlSubDocOperationResult{
		key: key,
		result: perSqlSubDocResult{
			keyValue: keyValue,
			error:    err,
			status:   status,
			offset:   offset,
		},
	}
}

func (m *sqlSubDocOperationResult) Key() string {
	return m.key
}

func (m *sqlSubDocOperationResult) Value(subPath string) (interface{}, int64) {
	for _, x := range m.result.keyValue {
		if x.Key == subPath {
			return x.Doc, x.Offset
		}
	}
	return nil, -1
}

func (m *sqlSubDocOperationResult) Values() []KeyValue {
	return m.result.keyValue
}

func (m *sqlSubDocOperationResult) GetError() error {
	return m.result.error
}

func (m *sqlSubDocOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (m *sqlSubDocOperationResult) GetOffset() int64 {
	return m.result.offset
}

func (m Sql) Connect(connStr, username, password string, extra Extras) error {
	clusterConfig := &sdk_mysql.SqlClusterConfig{
		ConnectionString:   connStr,
		Username:           username,
		Password:           password,
		Database:           extra.Database,
		MaxIdleConnections: extra.MaxIdleConnections,
		MaxOpenConnections: extra.MaxOpenConnections,
		MaxIdleTime:        extra.MaxIdleTime,
		MaxLifeTime:        extra.MaxLifeTime,
		Port:               extra.Port,
	}

	if _, err := m.connectionManager.GetSqlClusterObject(connStr, username, password, clusterConfig); err != nil {
		return err
	}
	return nil
}

func (m Sql) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]

	if err := validateStrings(extra.Table); err != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, errors.New("Table name is missing"), false,
			keyValue.Offset)
	}
	doc := keyValue.Doc.([]interface{})
	sqlQuery := fmt.Sprintf("INSERT INTO %s VALUES (%s)", extra.Table, strings.Repeat("?, ", len(doc)-1)+"?")
	result, err2 := sqlClient.ExecContext(context.TODO(), sqlQuery, doc...)
	if err2 != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, err2, false, keyValue.Offset)
	}
	if result == nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful CREATE operation %s ", connStr), false,
			keyValue.Offset)
	}
	return newSqlOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (m Sql) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]

	if err := validateStrings(extra.Table); err != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, errors.New("Table name is missing"), false,
			keyValue.Offset)
	}
	doc := keyValue.Doc.([]interface{})
	sqlQuery := fmt.Sprintf("REPLACE INTO %s VALUES (%s)", extra.Table, strings.Repeat("?, ", len(doc)-1)+"?")
	result, err2 := sqlClient.ExecContext(context.TODO(), sqlQuery, doc...)
	if err2 != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, err2, false, keyValue.Offset)
	}
	if result == nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful UPDATE operation %s ", connStr), false,
			keyValue.Offset)
	}
	return newSqlOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (m Sql) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		return newSqlOperationResult(key, nil, err, false, offset)
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]

	if err := validateStrings(extra.Table); err != nil {
		return newSqlOperationResult(key, nil, err, false, offset)
	}
	sqlQuery := fmt.Sprintf("Select * from %s where ID = ?", extra.Table)
	result, err2 := sqlClient.QueryContext(context.TODO(), sqlQuery, key)
	if err2 != nil {
		return newSqlOperationResult(key, nil, err2, false, offset)
	}
	if result == nil {
		return newSqlOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful UPDATE operation %s ", connStr), false,
			offset)
	}
	defer result.Close()
	log.Println(result.Columns())
	cols, err := result.Columns()
	log.Println(cols, len(cols))
	if err != nil {
		return newSqlOperationResult(key, nil, err, false, offset)
	}
	results := make(map[string]interface{})
	row := make([]interface{}, len(cols))
	for i := range row {
		var val interface{}
		row[i] = &val
	}
	for result.Next() {
		err = result.Scan(row...)
		if err != nil {
			return newSqlOperationResult(key, nil, err, false, offset)
		}
		for i, col := range cols {
			if val, ok := (*row[i].(*interface{})).([]byte); ok {
				results[col] = string(val)
			} else {
				results[col] = *(row[i].(*interface{}))
			}
		}

		// log.Println(results)
	}
	return newSqlOperationResult(key, results, nil, true, offset)

}

func (m Sql) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {

	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		return newSqlOperationResult(key, nil, err, false, offset)
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]

	if err := validateStrings(extra.Table); err != nil {
		return newSqlOperationResult(key, nil, err, false, offset)
	}

	sqlQuery := fmt.Sprintf("DELETE FROM %s WHERE ID = ?", extra.Table)
	result, err2 := sqlClient.ExecContext(context.TODO(), sqlQuery, key)
	if err2 != nil {
		return newSqlOperationResult(key, nil, err2, false, offset)
	}
	if result == nil {
		return newSqlOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful CREATE operation %s ", connStr), false,
			offset)
	}
	return newSqlOperationResult(key, nil, nil, true, offset)

}

func (m Sql) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newSqlBulkOperation()
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("empty table name"))
		return result
	}
	sqlQuery := fmt.Sprintf("DELETE FROM %s WHERE ID IN (%s)", extra.Table, strings.Repeat("?, ", len(keyValues)-1)+"?")
	stmt, err := sqlClient.PrepareContext(context.TODO(), sqlQuery)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	defer stmt.Close()

	// Extract the keys from keyValues
	keys := make([]interface{}, len(keyValues))
	keyToOffset := make(map[string]int64)
	for i, x := range keyValues {
		keys[i] = x.Key
		keyToOffset[x.Key] = x.Offset
	}
	bulkResult, err := stmt.ExecContext(context.TODO(), keys...)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if bulkResult == nil {
		result.failBulk(keyValues, errors.New("delete successful but result is nil"))
		return result
	}

	rowsAffected, err := bulkResult.RowsAffected()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if rowsAffected == 0 {
		result.failBulk(keyValues, errors.New("no IDs were deleted"))
		return result
	}
	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (m Sql) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newSqlBulkOperation()
	if err := validateStrings(connStr, username, password, extra.Database, extra.Table); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("empty table name"))
		return result
	}
	baseSQL := fmt.Sprintf("INSERT INTO %s VALUES ", extra.Table)

	// Prepare the VALUES clause
	var valueArgs []interface{}
	length := len(keyValues[0].Doc.([]interface{}))
	queryArg := strings.Repeat("("+strings.Repeat("?, ", length-1)+"?),", len(keyValues))
	queryArg = queryArg[:len(queryArg)-1]
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		doc := x.Doc.([]interface{})
		valueArgs = append(valueArgs, doc...)
	}

	sqlQuery := baseSQL + queryArg

	// Execute the bulk insert operation
	bulkResult, err := sqlClient.ExecContext(context.TODO(), sqlQuery, valueArgs...)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if bulkResult == nil {
		result.failBulk(keyValues, errors.New("create successful but result is nil"))
		return result
	}
	rowsAffected, err := bulkResult.RowsAffected()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if rowsAffected == 0 {
		result.failBulk(keyValues, errors.New("Zero rows affected"))
		return result
	}

	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (m Sql) Warmup(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		return err
	}
	if err := validateStrings(extra.Table); err != nil {
		return errors.New("table name is missing")
	}
	return nil
}

// extras should be a parameter. Needs change
func (m Sql) Close(connStr string) error {
	if err := m.connectionManager.Clusters[connStr].Close(); err != nil {
		log.Println("Sql Close(): Disconnect failed!")
		return err
	}
	return nil
}

func (m Sql) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newSqlBulkOperation()
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("empty table name"))
		return result
	}
	baseSQL := fmt.Sprintf("REPLACE INTO %s VALUES ", extra.Table)

	// Prepare the VALUES clause
	var valueArgs []interface{}
	if keyValues[0].Doc == nil {
		result.failBulk(keyValues, errors.New("empty doc"))
		return result
	}
	length := len(keyValues[0].Doc.([]interface{}))
	queryArg := strings.Repeat("("+strings.Repeat("?, ", length-1)+"?),", len(keyValues))
	queryArg = queryArg[:len(queryArg)-1]
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		doc := x.Doc.([]interface{})
		valueArgs = append(valueArgs, doc...)
	}

	sqlQuery := baseSQL + queryArg

	// Execute the bulk insert operation
	bulkResult, err := sqlClient.ExecContext(context.TODO(), sqlQuery, valueArgs...)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if bulkResult == nil {
		result.failBulk(keyValues, errors.New("create successful but result is nil"))
		return result
	}
	rowsAffected, err := bulkResult.RowsAffected()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if rowsAffected == 0 {
		result.failBulk(keyValues, errors.New("Zero rows affected"))
		return result
	}

	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (m Sql) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newSqlBulkOperation()
	if err := validateStrings(connStr, username, password, extra.Database, extra.Table); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]
	sqlQuery := fmt.Sprintf("Select * FROM %s WHERE ID IN (%s)", extra.Table, strings.Repeat("?, ", len(keyValues)-1)+"?")
	stmt, err := sqlClient.PrepareContext(context.TODO(), sqlQuery)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	defer stmt.Close()

	keys := make([]interface{}, len(keyValues))
	keyToOffset := make(map[string]int64)
	for i, x := range keyValues {
		keys[i] = x.Key
		keyToOffset[x.Key] = x.Offset
	}
	bulkResult, err := stmt.QueryContext(context.TODO(), keys...)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if bulkResult == nil {
		result.failBulk(keyValues, errors.New("read successful but result is nil"))
		return result
	}
	defer bulkResult.Close()
	cols, err := bulkResult.Columns()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	results := make(map[string]interface{})
	row := make([]interface{}, len(cols))
	for i := range row {
		var val interface{}
		row[i] = &val
	}
	failed := make(map[string]bool)
	for bulkResult.Next() {
		err = bulkResult.Scan(row...)
		if err != nil {
			result.failBulk(keyValues, err)
			return result
		}
		for i, col := range cols {
			if val, ok := (*row[i].(*interface{})).([]byte); ok {
				results[col] = string(val)
			} else {
				results[col] = *(row[i].(*interface{}))
			}
		}
		failed[results["ID"].(string)] = true
		result.AddResult(results["ID"].(string), results, nil, true, keyToOffset[results["ID"].(string)])
	}

	for _, x := range keyValues {
		_, ok := failed[x.Key]
		if !ok {
			result.AddResult(x.Key, nil, errors.New("document not found"), false, x.Offset)
		}
	}
	return result
}
func (m Sql) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO implement me
	panic("implement me")
}
func (m Sql) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	return nil
}

func (m Sql) UpsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Sql) Increment(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Sql) ReplaceSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")

}

func (m Sql) ReadSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	// TODO implement me
	return nil
}

func (m Sql) DeleteSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	return nil
}
func (m Sql) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	return nil
}
