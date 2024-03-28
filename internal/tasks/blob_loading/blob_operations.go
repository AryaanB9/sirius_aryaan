package blob_loading

import (
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/AryaanB9/sirius_aryaan/internal/db"
	"github.com/AryaanB9/sirius_aryaan/internal/docgenerator"
	"github.com/AryaanB9/sirius_aryaan/internal/external_storage"
	"github.com/AryaanB9/sirius_aryaan/internal/task_result"
	"github.com/AryaanB9/sirius_aryaan/internal/task_state"
	"github.com/AryaanB9/sirius_aryaan/internal/tasks"
	"github.com/AryaanB9/sirius_aryaan/internal/template"

	"github.com/bgadrian/fastfaker/faker"
	"github.com/dnlo/struct2csv"
	"github.com/linkedin/goavro/v2"
	"github.com/parquet-go/parquet-go"
)

func createS3Bucket(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValue external_storage.KeyValue
	key := seed
	docId := gen.BuildKey(key)
	keyValue = external_storage.KeyValue{
		Key:    docId,
		Doc:    extra.Bucket,
		Offset: 0,
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	folderResult := extStorage.CreateBucket(keyValue, extra)

	if folderResult.GetError() != nil {
		if db.CheckAllowedInsertError(folderResult.GetError()) && rerun {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
		} else {
			result.IncrementFailure(initTime, keyValue.Key, folderResult.GetError(), false, folderResult.GetExtra(),
				keyValue.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: keyValue.Offset}
		}
	} else {
		state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
	}
}

func deleteS3Bucket(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValue external_storage.KeyValue
	key := seed
	docId := gen.BuildKey(key)
	keyValue = external_storage.KeyValue{
		Key:    docId,
		Doc:    nil,
		Offset: 0,
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	folderResult := extStorage.DeleteBucket(keyValue, extra)

	if folderResult.GetError() != nil {
		if db.CheckAllowedInsertError(folderResult.GetError()) && rerun {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
		} else {
			result.IncrementFailure(initTime, keyValue.Key, folderResult.GetError(), false, folderResult.GetExtra(),
				keyValue.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: keyValue.Offset}
		}
	} else {
		state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
	}
}

func insertFolder(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValue external_storage.KeyValue
	key := seed
	docId := gen.BuildKey(key)
	keyValue = external_storage.KeyValue{
		Key:    docId,
		Doc:    extra.FolderPath,
		Offset: 0,
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	folderResult := extStorage.CreateFolder(keyValue, extra)

	if folderResult.GetError() != nil {
		if db.CheckAllowedInsertError(folderResult.GetError()) && rerun {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
		} else {
			result.IncrementFailure(initTime, keyValue.Key, folderResult.GetError(), false, folderResult.GetExtra(),
				keyValue.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: keyValue.Offset}
		}
	} else {
		state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
	}
}

func deleteFolder(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValue external_storage.KeyValue
	key := seed
	docId := gen.BuildKey(key)
	keyValue = external_storage.KeyValue{
		Key:    docId,
		Doc:    nil,
		Offset: 0,
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	folderResult := extStorage.DeleteFolder(keyValue, extra)

	if folderResult.GetError() != nil {
		if db.CheckAllowedInsertError(folderResult.GetError()) && rerun {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
		} else {
			result.IncrementFailure(initTime, keyValue.Key, folderResult.GetError(), false, folderResult.GetExtra(),
				keyValue.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: keyValue.Offset}
		}
	} else {
		state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
	}
}

func insertFiles(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValues []external_storage.KeyValue
	var docsArray []interface{}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := gen.Template.GenerateDocument(fake, docId, operationConfig.DocSize)
		keyValues = append(keyValues, external_storage.KeyValue{
			Key:    docId,
			Doc:    nil,
			Offset: offset,
		})
		docsArray = append(docsArray, doc)
	}

	// Handling the Different File Formats
	templateName := strings.ToLower(operationConfig.TemplateName)
	var fileToUpload []byte
	var errDocToFileFormat error

	switch strings.ToLower(extra.FileFormat) {
	case "json":
		fileToUpload, errDocToFileFormat = json.MarshalIndent(docsArray, "", "  ")
		if errDocToFileFormat != nil {
			log.Println("In operations.go insertFiles(), error marshaling JSON:", errDocToFileFormat)
		}

	case "avro":
		var bufferAvroFile bytes.Buffer

		// Parsing the AVRO Schema
		avroSchema, err := template.GetAvroSchema(templateName)
		if err != nil {
			log.Println("In operations.go insertFiles(), error getting avro schema:", err)
		}
		// Getting the codec which is used in conversion between binary and struct (in form of [string]interface{})
		codec, err := goavro.NewCodec(avroSchema)
		if err != nil {
			log.Println("In operations.go insertFiles(), error parsing avro schema:", err)
		}

		// Converting the documents into avro binary
		for _, x := range docsArray {
			avroDoc, err := codec.BinaryFromNative(nil, template.StructToMap(x))
			if err != nil {
				log.Println("In operations.go insertFiles(), error converting into avro:", err)
			}
			_, err = bufferAvroFile.Write(avroDoc)
			if err != nil {
				log.Println("In operations.go insertFiles(), writing avro data to buffer failed:", err)
			}
		}

		fileToUpload = bufferAvroFile.Bytes()
		bufferAvroFile.Reset()

		// Converting Binary to Struct. For one doc
		//docFromAvro, _, _ := codec.NativeFromBinary(fileToUpload)
		//log.Println(template.StringMapToHotel(docFromAvro.(map[string]interface{})))

	case "csv":
		bufferCsvFile := &bytes.Buffer{}
		csvFileWriter := struct2csv.NewWriter(bufferCsvFile)

		switch templateName {
		case "hotel":
			err := csvFileWriter.WriteColNames(*docsArray[0].(*template.Hotel))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into csv:", err)
			}
			for _, x := range docsArray {
				err := csvFileWriter.WriteStruct(*x.(*template.Hotel))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into csv:", err)
				}
			}
		case "person":
			err := csvFileWriter.WriteColNames(*docsArray[0].(*template.Person))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into csv:", err)
			}
			for _, x := range docsArray {
				err := csvFileWriter.WriteStruct(*x.(*template.Person))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into csv:", err)
				}
			}
		case "product":
			err := csvFileWriter.WriteColNames(*docsArray[0].(*template.Product))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into csv:", err)
			}
			for _, x := range docsArray {
				err := csvFileWriter.WriteStruct(*x.(*template.Product))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into csv:", err)
				}
			}
		default:
			panic("invalid template name")
		}
		csvFileWriter.Flush()
		fileToUpload = bufferCsvFile.Bytes()
		bufferCsvFile.Reset()

	case "tsv":
		bufferTsvFile := &bytes.Buffer{}
		tsvFileWriter := struct2csv.NewWriter(bufferTsvFile)
		var tabDelimiter rune
		tabDelimiter = '\t'
		tsvFileWriter.SetComma(tabDelimiter)

		switch templateName {
		case "hotel":
			err := tsvFileWriter.WriteColNames(*docsArray[0].(*template.Hotel))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into tsv:", err)
			}
			for _, x := range docsArray {
				err := tsvFileWriter.WriteStruct(*x.(*template.Hotel))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into tsv:", err)
				}
			}
		case "person":
			err := tsvFileWriter.WriteColNames(*docsArray[0].(*template.Person))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into tsv:", err)
			}
			for _, x := range docsArray {
				err := tsvFileWriter.WriteStruct(*x.(*template.Person))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into tsv:", err)
				}
			}
		case "product":
			err := tsvFileWriter.WriteColNames(*docsArray[0].(*template.Product))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into tsv:", err)
			}
			for _, x := range docsArray {
				err := tsvFileWriter.WriteStruct(*x.(*template.Product))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into tsv:", err)
				}
			}
		default:
			panic("invalid template name or template name not supported")
		}
		tsvFileWriter.Flush()
		fileToUpload = bufferTsvFile.Bytes()
		bufferTsvFile.Reset()

	case "parquet":
		bufferParquetFile := new(bytes.Buffer)
		var writer *parquet.GenericWriter[interface{}]

		// Defining the parquet schema and then creating a parquet writer instance with buffer as output
		if strings.ToLower(templateName) == "hotel" {
			schema := parquet.SchemaOf(new(template.Hotel))
			writer = parquet.NewGenericWriter[interface{}](bufferParquetFile, schema)

		} else if strings.ToLower(templateName) == "person" {
			schema := parquet.SchemaOf(new(template.Person))
			writer = parquet.NewGenericWriter[interface{}](bufferParquetFile, schema)

		} else if strings.ToLower(templateName) == "product" {
			schema := parquet.SchemaOf(new(template.Product))
			writer = parquet.NewGenericWriter[interface{}](bufferParquetFile, schema)
		}

		// Writing the array of documents to parquet writer
		_, err := writer.Write(docsArray)
		if err != nil {
			log.Println("In operations.go insertFiles(), error writing document into parquet:", err)
		}

		// Closing the writer to flush buffers and write the file footer.
		if err := writer.Close(); err != nil {
			log.Println("In operations.go insertFiles(), error flushing Parquet writer:", err)
		}

		fileToUpload = bufferParquetFile.Bytes()
		bufferParquetFile.Reset()

	default:
		panic("invalid file format or file format not supported")
	}

	// Emptying the created array of docs
	docsArray = nil

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := extStorage.CreateFiles(&fileToUpload, keyValues, extra)

	// Emptying created file
	fileToUpload = nil

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			if db.CheckAllowedInsertError(bulkResult.GetError(x.Key)) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			} else {
				result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}

	}
	keyValues = nil
}

func deleteFiles(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValues []external_storage.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, external_storage.KeyValue{
			Key: docId,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := extStorage.DeleteFiles(keyValues, extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			if db.CheckAllowedDeletetError(bulkResult.GetError(x.Key)) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			} else {
				result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}
	}
}

func insertFilesInFolders(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	insertFiles(start, end, seed, operationConfig, rerun, gen, state, result, databaseInfo, extra, wg)
}

func updateFilesInFolder(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	insertFiles(start, end, seed, operationConfig, rerun, gen, state, result, databaseInfo, extra, wg)
}

func deleteFilesInFolder(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValues []external_storage.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, external_storage.KeyValue{
			Key: docId,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := extStorage.DeleteFilesInFolder(keyValues, extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			if db.CheckAllowedDeletetError(bulkResult.GetError(x.Key)) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			} else {
				result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}
	}
}
