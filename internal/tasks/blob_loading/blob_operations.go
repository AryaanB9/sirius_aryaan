package blob_loading

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/external_storage"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/template"

	"github.com/bgadrian/fastfaker/faker"
	"github.com/dnlo/struct2csv"
	"github.com/linkedin/goavro/v2"
	"github.com/parquet-go/parquet-go"
)

func createBucket(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
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

func deleteBucket(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
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

func insertFile(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValues []external_storage.KeyValue
	fileFormat := strings.ToLower(extra.FileFormat) // file format can be like "json" or "json, avro" or "json,parquet"
	if fileFormat == "" {
		log.Println("creating files to insert: file format not provided")
		return
	}

	templateName := strings.ToLower(operationConfig.TemplateName)
	pathToFileOnDisk := TempFolderPath + generateRandomString(24) + "." + fileFormat

	// Validating file format
	if checkFileFormat := external_storage.ValidateFileFormat(fileFormat); checkFileFormat {
		// Checking if file format extension in FilePath is correct
		_, file := filepath.Split(extra.FilePath)
		temp := strings.Split(file, ".")
		tempFormat := temp[len(temp)-1]
		if tempFormat != fileFormat {
			log.Println("creating files to insert: file format of file path and file format provided does not match")
			return
		}
	} else {
		log.Println("creating files to insert: file format provided is invalid or is not supported")
		return
	}

	// The file will be created on disk
	file, err := os.OpenFile(pathToFileOnDisk, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Println("creating files to insert: opening output file:", err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("creating files to insert: closing output file:", err)
			return
		}
	}(file)

	// Handling the Different File Formats
	switch fileFormat {
	case "json":

		encoder := json.NewEncoder(file)

		timeFileGenStart := time.Now()
		for offset := start; offset < end; offset++ {
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

			if err := encoder.Encode(doc); err != nil {
				log.Println("creating files to insert: encoding output json file:", err)
				return
			}
		}
		log.Println("time to create file", pathToFileOnDisk, "on disk:", time.Now().Sub(timeFileGenStart))

	case "avro":

		// Parsing the AVRO Schema
		avroSchema, err := template.GetAvroSchema(templateName)
		if err != nil {
			log.Println("creating files to insert: getting avro schema:", err)
			return
		}
		// Getting the codec which is used in conversion between binary and struct (in form of [string]interface{})
		codec, err := goavro.NewCodec(avroSchema)
		if err != nil {
			log.Println("creating files to insert: parsing avro schema:", err)
			return
		}

		timeFileGenStart := time.Now()
		for offset := start; offset < end; offset++ {
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

			// Converting the struct into avro binary. First struct is converted into map[string]interface{}, then that
			// is converted to avro binary.
			avroBytes, err := codec.BinaryFromNative(nil, template.StructToMap(doc))
			if err != nil {
				log.Println("creating files to insert: converting into avro:", err)
				return
			}

			// Writing the avro bytes to file
			_, err = file.Write(avroBytes)
			if err != nil {
				log.Println("creating files to insert: writing doc to avro file:", err)
				return
			}
		}
		log.Println("time to create file", pathToFileOnDisk, "on disk:", time.Now().Sub(timeFileGenStart))

	case "csv":
		csvFileWriter := struct2csv.NewWriter(file)

		timeFileGenStart := time.Now()
		for offset := start; offset < end; offset++ {
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

			if offset == start {
				// Writing the column names for the csv file
				doc, _ := gen.Template.GetValues(doc)
				err := csvFileWriter.WriteColNames(doc)
				if err != nil {
					log.Println("creating files to insert: writing header into csv:", err)
					return
				}
			}

			// Writing the csv row to file
			doc, _ = gen.Template.GetValues(doc)
			err = csvFileWriter.WriteStruct(doc)
			if err != nil {
				log.Println("creating files to insert: writing into csv file:", err)
				return
			}
		}
		csvFileWriter.Flush()
		log.Println("time to create file", pathToFileOnDisk, "on disk:", time.Now().Sub(timeFileGenStart))

	case "tsv":
		tsvFileWriter := struct2csv.NewWriter(file)
		var tabDelimiter rune
		tabDelimiter = '\t'
		tsvFileWriter.SetComma(tabDelimiter)

		timeFileGenStart := time.Now()
		for offset := start; offset < end; offset++ {
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

			if offset == start {
				// Writing the column names for the tsv file
				doc, _ := gen.Template.GetValues(doc)
				err := tsvFileWriter.WriteColNames(doc)
				if err != nil {
					log.Println("creating files to insert: writing headers into tsv:", err)
					return
				}
			}

			// Writing the tsv row to file
			doc, _ = gen.Template.GetValues(doc)
			err = tsvFileWriter.WriteStruct(doc)
			if err != nil {
				log.Println("creating files to insert: writing into tsv file:", err)
				return
			}
		}
		tsvFileWriter.Flush()
		log.Println("time to create file", pathToFileOnDisk, "on disk:", time.Now().Sub(timeFileGenStart))

	case "parquet":

		var writer *parquet.GenericWriter[interface{}]

		// Defining the parquet schema and then creating a parquet writer instance with buffer as output
		schema := parquet.SchemaOf(template.InitialiseTemplate(templateName))
		writer = parquet.NewGenericWriter[interface{}](file, schema)

		timeFileGenStart := time.Now()
		for offset := start; offset < end; offset++ {
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

			// Writing the array of documents to parquet writer
			_, err := writer.Write([]interface{}{doc})
			if err != nil {
				log.Println("creating files to insert: writing document into parquet:", err)
				return
			}
		}

		// Closing the writer to flush buffers and write the file footer.
		if err := writer.Close(); err != nil {
			log.Println("creating files to insert: flushing parquet writer:", err)
			return
		}
		log.Println("time to create file", pathToFileOnDisk, "on disk:", time.Now().Sub(timeFileGenStart))

	default:
		panic("creating files to insert: invalid file format or file format not supported")
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := extStorage.CreateFile(pathToFileOnDisk, keyValues, extra)

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

func deleteFile(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValues []external_storage.KeyValue
	for offset := start; offset < end; offset++ {
		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, external_storage.KeyValue{
			Key: docId,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := extStorage.DeleteFile(keyValues, extra)

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

	insertFile(start, end, seed, operationConfig, rerun, gen, state, result, databaseInfo, extra, wg)
}

func updateFilesInFolder(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	insertFile(start, end, seed, operationConfig, rerun, gen, state, result, databaseInfo, extra, wg)
}

func deleteFilesInFolder(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValues []external_storage.KeyValue
	for offset := start; offset < end; offset++ {
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
