package db

import (
	"encoding/json"
	"errors"
	"github.com/couchbase/gocb/v2"
	"log"

	"github.com/AryaanB9/sirius_aryaan/internal/sdk_columnar"
)

type columnarOperationResult struct {
	key    string
	result perDocResult
}

func newColumnarOperationResult(key string, value interface{}, err error, status bool, offset int64) *columnarOperationResult {
	return &columnarOperationResult{
		key: key,
		result: perDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (c *columnarOperationResult) Key() string {
	return c.key
}

func (c *columnarOperationResult) Value() interface{} {
	return c.result.value
}

func (c *columnarOperationResult) GetStatus() bool {
	return c.result.status
}

func (c *columnarOperationResult) GetError() error {
	return c.result.error
}

func (c *columnarOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (c *columnarOperationResult) GetOffset() int64 {
	return c.result.offset
}

type columnarBulkOperationResult struct {
	keyValues map[string]perDocResult
}

func newColumnarBulkOperation() *columnarBulkOperationResult {
	return &columnarBulkOperationResult{
		keyValues: make(map[string]perDocResult),
	}
}

func (m *columnarBulkOperationResult) AddResult(key string, value interface{}, err error, status bool, offset int64) {
	m.keyValues[key] = perDocResult{
		value:  value,
		error:  err,
		status: status,
		offset: offset,
	}
}

func (m *columnarBulkOperationResult) Value(key string) interface{} {
	if x, ok := m.keyValues[key]; ok {
		return x.value
	}
	return nil
}

func (m *columnarBulkOperationResult) GetStatus(key string) bool {
	if x, ok := m.keyValues[key]; ok {
		return x.status
	}
	return false
}

func (m *columnarBulkOperationResult) GetError(key string) error {
	if x, ok := m.keyValues[key]; ok {
		return x.error
	}
	return errors.New("Key not found in bulk operation")
}

func (m *columnarBulkOperationResult) GetExtra(key string) map[string]any {
	if _, ok := m.keyValues[key]; ok {
		return map[string]any{}
	}
	return nil
}

func (m *columnarBulkOperationResult) GetOffset(key string) int64 {
	if x, ok := m.keyValues[key]; ok {
		return x.offset
	}
	return -1
}

func (m *columnarBulkOperationResult) failBulk(keyValue []KeyValue, err error) {
	for _, x := range keyValue {
		m.keyValues[x.Key] = perDocResult{
			value:  x.Doc,
			error:  err,
			status: false,
		}
	}
}

func (m *columnarBulkOperationResult) GetSize() int {
	return len(m.keyValues)
}

type Columnar struct {
	ConnectionManager *sdk_columnar.ConnectionManager
}

func NewColumnarConnectionManager() *Columnar {
	return &Columnar{
		ConnectionManager: sdk_columnar.ConfigConnectionManager(),
	}
}
func (c *Columnar) ValidateConfig(extras Extras) (string, string, string) {
	bucket := "Default"
	scope := "Default"
	collection := "Default"
	if extras.Bucket != "" {
		bucket = extras.Bucket
	}
	if extras.Scope != "" {
		scope = extras.Scope
	}
	if extras.Collection != "" {
		collection = extras.Collection
	}
	return bucket, scope, collection
}

func (c *Columnar) Connect(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}
	clusterConfig := &sdk_columnar.ClusterConfig{}

	if _, err := c.ConnectionManager.GetCluster(connStr, username, password, clusterConfig); err != nil {
		log.Println("In Columnar Connect(), error in GetCluster()")
		return err
	}

	return nil
}

func (c *Columnar) Warmup(connStr, username, password string, extra Extras) error {

	if err := validateStrings(connStr, username, password); err != nil {
		log.Println("In Columnar Warmup(), error:", err)
		return err
	}

	//log.Println("In Columnar Warmup()")

	// Pinging the Cluster
	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster

	pingRes, errPing := cbCluster.Ping(&gocb.PingOptions{
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeAnalytics},
	})
	if errPing != nil {
		//log.Print("In Columnar Warmup(), error while pinging:", errPing)
		return errPing
	}

	for service, pingReports := range pingRes.Services {
		if service != gocb.ServiceTypeAnalytics {
			log.Println("We got a service type that we didn't ask for!")
		}
		for _, pingReport := range pingReports {
			if pingReport.State != gocb.PingStateOk {
				log.Printf(
					"Node %s at remote %s is not OK, error: %s, latency: %s\n",
					pingReport.ID, pingReport.Remote, pingReport.Error, pingReport.Latency.String(),
				)
			} else {
				log.Printf(
					"Node %s at remote %s is OK, latency: %s\n",
					pingReport.ID, pingReport.Remote, pingReport.Latency.String(),
				)
			}
		}
	}

	return nil
}

func (c *Columnar) Close(connStr string, extra Extras) error {
	return c.ConnectionManager.Disconnect(connStr)
}

func (c *Columnar) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newColumnarOperationResult(keyValue.Key, nil, err, false, keyValue.Offset)
	}
	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster
	bucket, scope, collection := c.ValidateConfig(extra)
	query := "insert into " + bucket + "." + scope + "." + collection + " ($doc) "
	params := map[string]interface{}{
		"doc": keyValue.Doc,
	}
	results, errAnalyticsQuery := cbCluster.AnalyticsQuery(query, &gocb.AnalyticsOptions{NamedParameters: params})
	err := results.Close()
	if err != nil {
		return newColumnarOperationResult(keyValue.Key, nil, err, false, keyValue.Offset)
	}
	if errAnalyticsQuery != nil {
		return newColumnarOperationResult(keyValue.Key, nil, errAnalyticsQuery, false, keyValue.Offset)
	}
	//if results.Err() != nil {
	//	return newColumnarOperationResult(keyValue.Key, nil, results.Err(), false, keyValue.Offset)
	//}
	return newColumnarOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (c *Columnar) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newColumnarOperationResult(keyValue.Key, nil, err, false, keyValue.Offset)
	}
	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster
	bucket, scope, collection := c.ValidateConfig(extra)
	query := "UPSERT into " + bucket + "." + scope + "." + collection + " ($doc) "
	params := map[string]interface{}{
		"doc": keyValue.Doc,
	}
	results, errAnalyticsQuery := cbCluster.AnalyticsQuery(query, &gocb.AnalyticsOptions{NamedParameters: params})
	err := results.Close()
	if err != nil {
		return newColumnarOperationResult(keyValue.Key, nil, err, false, keyValue.Offset)
	}
	if errAnalyticsQuery != nil {
		return newColumnarOperationResult(keyValue.Key, nil, errAnalyticsQuery, false, keyValue.Offset)
	}
	return newColumnarOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (c *Columnar) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newColumnarOperationResult(key, nil, err, false, offset)
	}

	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster
	bucket, scope, collection := c.ValidateConfig(extra)
	var query string
	if extra.Query != "" {
		query = extra.Query
	} else {
		query = "Select * from " + bucket + "." + scope + "." + collection + " where id  ='" + key + "'"
	}
	results, errAnalyticsQuery := cbCluster.AnalyticsQuery(query, &gocb.AnalyticsOptions{})
	if errAnalyticsQuery != nil {
		return newColumnarOperationResult(key, nil, errAnalyticsQuery, false, offset)
	}
	var resultDisplay map[string]interface{}
	if results.Next() {
		err := results.Row(&resultDisplay)
		if err != nil {
			return newColumnarOperationResult(key, nil, err, false, offset)
		}
	}
	err := results.Close()
	if err != nil {
		return newColumnarOperationResult(key, nil, err, false, offset)
	}
	return newColumnarOperationResult(key, resultDisplay, nil, true, offset)
}

func (c *Columnar) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster
	bucket, scope, collection := c.ValidateConfig(extra)
	query := "Delete  from " + bucket + "." + scope + "." + collection + " where id = $id;"
	params := map[string]interface{}{
		"id": key,
	}
	results, errAnalyticsQuery := cbCluster.AnalyticsQuery(query, &gocb.AnalyticsOptions{NamedParameters: params})
	if errAnalyticsQuery != nil {
		return newColumnarOperationResult(key, nil, errAnalyticsQuery, false, offset)
	}
	err := results.Close()
	if err != nil {
		return newColumnarOperationResult(key, nil, err, false, offset)
	}
	return newColumnarOperationResult(key, nil, nil, true, offset)
}

func (c *Columnar) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newColumnarBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster
	bucket, scope, collection := c.ValidateConfig(extra)
	//dataValues := make([]interface{}, len(keyValues))
	var dataValues []interface{}
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		dataValues = append(dataValues, x.Doc)
	}
	dv, _ := json.Marshal(dataValues)
	query := "insert into " + bucket + "." + scope + "." + collection + string(dv)
	results, errAnalyticsQuery := cbCluster.AnalyticsQuery(query, &gocb.AnalyticsOptions{})
	if errAnalyticsQuery != nil {
		result.failBulk(keyValues, errAnalyticsQuery)
		return result
	}
	err := results.Close()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	for _, x := range keyValues {
		result.AddResult(x.Key, x.Doc, nil, true, keyToOffset[x.Key])
	}
	return result

}

func (c *Columnar) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newColumnarBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster
	bucket, scope, collection := c.ValidateConfig(extra)
	var dataValues []interface{}
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		dataValues = append(dataValues, x.Doc)
	}
	dv, _ := json.Marshal(dataValues)
	query := "upsert into " + bucket + "." + scope + "." + collection + string(dv)
	results, errAnalyticsQuery := cbCluster.AnalyticsQuery(query, &gocb.AnalyticsOptions{})
	if errAnalyticsQuery != nil {
		result.failBulk(keyValues, errAnalyticsQuery)
		return result
	}
	err := results.Close()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	for _, x := range keyValues {
		result.AddResult(x.Key, x.Doc, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (c *Columnar) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newColumnarBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster
	bucket, scope, collection := c.ValidateConfig(extra)
	docIDs := []string{}
	OffsetTokey := make(map[int64]string)
	for _, x := range keyValues {
		OffsetTokey[x.Offset] = x.Key
		docIDs = append(docIDs, x.Key)
	}
	query := "Select * from " + bucket + "." + scope + "." + collection + "  where id in $ids order by id asc;"
	params := map[string]interface{}{
		"ids": docIDs,
	}
	results, errAnalyticsQuery := cbCluster.AnalyticsQuery(query, &gocb.AnalyticsOptions{NamedParameters: params})
	if errAnalyticsQuery != nil {
		result.failBulk(keyValues, errAnalyticsQuery)
		return result
	}

	offset := keyValues[0].Offset
	for results.Next() {
		var resultDisplay map[string]interface{}
		err := results.Row(&resultDisplay)
		if err != nil {
			result.AddResult(OffsetTokey[offset], nil, err, false, offset)
		} else {
			result.AddResult(OffsetTokey[offset], resultDisplay, nil, true, offset)
		}
		offset += 1
	}
	err := results.Close()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	return result
}

func (c *Columnar) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newColumnarBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster
	bucket, scope, collection := c.ValidateConfig(extra)
	docIDs := []string{}
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		docIDs = append(docIDs, x.Key)
	}
	query := "delete from " + bucket + "." + scope + "." + collection + " where id in $ids"
	params := map[string]interface{}{
		"ids": docIDs,
	}
	results, errAnalyticsQuery := cbCluster.AnalyticsQuery(query, &gocb.AnalyticsOptions{NamedParameters: params})
	if errAnalyticsQuery != nil {
		result.failBulk(keyValues, errAnalyticsQuery)
		return result
	}
	err := results.Close()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	for _, x := range keyValues {
		result.AddResult(x.Key, x.Doc, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (c *Columnar) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) UpsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) Increment(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) ReplaceSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) ReadSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) DeleteSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) CreateDatabase(connStr, username, password string, extra Extras, templateName string, docSize int) (string, error) {
	// TODO
	panic("Implement the function")
}
func (c *Columnar) DeleteDatabase(connStr, username, password string, extra Extras) (string, error) {
	// TODO
	panic("Implement the function")
}
func (c *Columnar) Count(connStr, username, password string, extra Extras) (int64, error) {
	// TODO
	panic("Implement the function")
}
func (c *Columnar) ListDatabase(connStr, username, password string, extra Extras) (any, error) {
	// TODO
	panic("Implement the function")
}
