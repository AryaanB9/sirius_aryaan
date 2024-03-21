package tasks

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AryaanB9/sirius_aryaan/internal/db"
	"github.com/AryaanB9/sirius_aryaan/internal/docgenerator"
	"github.com/AryaanB9/sirius_aryaan/internal/err_sirius"
	"github.com/AryaanB9/sirius_aryaan/internal/meta_data"
	"github.com/AryaanB9/sirius_aryaan/internal/task_result"
	"github.com/AryaanB9/sirius_aryaan/internal/task_state"
	"github.com/AryaanB9/sirius_aryaan/internal/template"

	"golang.org/x/sync/errgroup"
)

type GenericLoadingTask struct {
	IdentifierToken string `json:"identifierToken" doc:"true"`
	DatabaseInformation
	ExternalStorageExtras external_storage.ExternalStorageExtras `json:"externalStorageExtras" doc:"true"`
	ResultSeed            int64                                  `json:"resultSeed" doc:"false"`
	TaskPending           bool                                   `json:"taskPending" doc:"false"`
	State                 *task_state.TaskState                  `json:"State" doc:"false"`
	MetaData              *meta_data.CollectionMetaData          `json:"metaData" doc:"false"`
	OperationConfig       *OperationConfig                       `json:"operationConfig" doc:"true"`
	Operation             string                                 `json:"operation" doc:"false"`
	Result                *task_result.TaskResult                `json:"-" doc:"false"`
	gen                   *docgenerator.Generator                `json:"-" doc:"false"`
	req                   *Request                               `json:"-" doc:"false"`
	rerun                 bool                                   `json:"-" doc:"false"`
	lock                  sync.Mutex                             `json:"-" doc:"false"`
}

func (t *GenericLoadingTask) Describe() string {
	return "Do operation between range from [start,end)"
}

func (t *GenericLoadingTask) MetaDataIdentifier() string {
	if t.DBType == db.CouchbaseDb {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr, t.Extra.Bucket, t.Extra.Scope, t.Extra.Collection}, ":")
	} else if t.DBType == db.MongoDb {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr, t.Extra.Database, t.Extra.Collection}, ":")
	} else if t.DBType == db.CassandraDb {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr, t.Extra.Keyspace, t.Extra.Table}, ":")
	} else if t.DBType == external_storage.AmazonS3Storage {
		return strings.Join([]string{t.IdentifierToken, t.ExternalStorageExtras.AwsAccessKey, t.ExternalStorageExtras.AwsRegion}, ":")
	} else {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr}, ":")
	}
}

func (t *GenericLoadingTask) CheckIfPending() bool {
	return t.TaskPending
}

// Config configures the insert task
func (t *GenericLoadingTask) Config(req *Request, reRun bool) (int64, error) {
	t.TaskPending = true
	t.req = req

	if t.req == nil {
		t.TaskPending = false
		return 0, err_sirius.RequestIsNil
	}

	if t.DBType == external_storage.AmazonS3Storage {
		if extStorage, err := external_storage.ConfigExternalStorage(t.DBType); err != nil {
			return 0, err
		} else {
			if err = extStorage.Connect(t.ExternalStorageExtras); err != nil {
				return 0, err
			}
		}
	} else {
		if database, err := db.ConfigDatabase(t.DBType); err != nil {
			return 0, err
		} else {
			if err = database.Connect(t.ConnStr, t.Username, t.Password, t.Extra); err != nil {
				return 0, err
			}
		}
	}

	t.lock = sync.Mutex{}
	t.rerun = reRun

	if t.Operation == "" {
		return 0, err_sirius.InternalErrorSetOperationType
	}

	if !reRun {
		t.ResultSeed = int64(time.Now().UnixNano())

		if err := ConfigureOperationConfig(t.OperationConfig); err != nil {
			t.TaskPending = false
			return 0, err
		}

		if err := configExtraParameters(t.DBType, &t.Extra); err != nil {
			return 0, err
		}

		t.req.Lock()
		t.MetaData = t.req.MetaData.GetCollectionMetadata(t.MetaDataIdentifier())
		if t.OperationConfig.End+t.MetaData.Seed > t.MetaData.SeedEnd {
			t.req.AddToSeedEnd(t.MetaData, (t.OperationConfig.End+t.MetaData.Seed)-(t.MetaData.SeedEnd))
		}
		t.req.Unlock()

	} else {
		_ = task_result.DeleteResultFile(t.ResultSeed)
		log.Println("retrying :- ", t.Operation, t.IdentifierToken, t.ResultSeed)
	}

	if t.State == nil {
		t.State = task_state.ConfigTaskState(t.ResultSeed)
	}
	if t.State.StateChannel == nil {
		t.State.SetupStoringKeys()
	}

	t.gen = docgenerator.ConfigGenerator(
		t.OperationConfig.KeySize,
		t.OperationConfig.DocSize,
		template.InitialiseTemplate(t.OperationConfig.TemplateName))

	return t.ResultSeed, nil
}

func (t *GenericLoadingTask) TearUp() error {

	t.Result.StopStoringResult()
	t.Result.Success = t.OperationConfig.End - t.OperationConfig.Start - t.Result.Failure
	if err := t.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", t.MetaDataIdentifier(), " ", t.ResultSeed, " ", t.Operation)
	}
	t.Result = nil

	t.State.StopStoringState()
	if err := t.State.SaveTaskSateOnDisk(); err != nil {
		log.Println("not able to save state into ", t.MetaDataIdentifier(), " ", t.ResultSeed, " ", t.Operation)
	}

	t.TaskPending = false
	return t.req.SaveRequestIntoFile()
}

func (t *GenericLoadingTask) Do() {

	t.Result = task_result.ConfigTaskResult(t.Operation, t.ResultSeed)

	if t.DBType == external_storage.AmazonS3Storage {
		externalStorage, err := external_storage.ConfigExternalStorage(t.DBType)
		if err != nil {
			t.Result.ErrorOther = err.Error()
			t.Result.FailWholeBulkOperation(t.OperationConfig.Start, t.OperationConfig.End,
				err, t.State, t.gen, t.MetaData.Seed)
			_ = t.TearUp()
			return
		}

		err = externalStorage.Warmup(t.ExternalStorageExtras)
		if err != nil {
			t.Result.ErrorOther = err.Error()
			t.Result.FailWholeBulkOperation(t.OperationConfig.Start, t.OperationConfig.End,
				err, t.State, t.gen, t.MetaData.Seed)
			_ = t.TearUp()
			return
		}
		loadDocumentsInBatches(t)
		_ = t.TearUp()

	} else {
		database, err := db.ConfigDatabase(t.DBType)
		if err != nil {
			t.Result.ErrorOther = err.Error()
			t.Result.FailWholeBulkOperation(t.OperationConfig.Start, t.OperationConfig.End,
				err, t.State, t.gen, t.MetaData.Seed)
			_ = t.TearUp()
			return
		}

		err = database.Warmup(t.ConnStr, t.Username, t.Password, t.Extra)
		if err != nil {
			t.Result.ErrorOther = err.Error()
			t.Result.FailWholeBulkOperation(t.OperationConfig.Start, t.OperationConfig.End,
				err, t.State, t.gen, t.MetaData.Seed)
			_ = t.TearUp()
			return
		}

		loadDocumentsInBatches(t)

		_ = t.TearUp()
	}
}

// loadDocumentsInBatches divides the load into batches and will allocate to one of go routine.
func loadDocumentsInBatches(task *GenericLoadingTask) {

	fmt.Println()
	log.Println("identifier : ", task.MetaDataIdentifier())
	log.Println("operation :", task.Operation)
	log.Println("result ", task.ResultSeed)
	fmt.Println(task.OperationConfig)

	// This is stop processing loading operation if user has already cancelled all the tasks from sirius
	if task.req.ContextClosed() {
		return
	}

	wg := &sync.WaitGroup{}
	numOfBatches := int64(0)

	// default batch size is calculated by dividing the total operations in equal quantity to each thread.
	batchSize := int64(5000)

	if task.DBType == "dynamodb" {
		task.Extra.SDKBatchSize = 25
  }

	/*
		 * 	For External Storage operations, we need to handle the batchSize and numOfBatches differently.
		 * 	To insert a single file we need to use 1 thread (or 1 batch) and in that thread bulk documents will be created
			and appended to create the file.
		 * 	Therefore, numOfBatches shall be = 1 for most File operations, except for InsertFilesInFoldersOperation and
			UpdateFilesInFolderOperation for which we have handled the bulk loading explicitly.
	*/
	if task.DBType == external_storage.AmazonS3Storage {
		if task.Extra.SDKBatchSize > 0 {
			batchSize = int64(task.Extra.SDKBatchSize)
		} else {
			batchSize = task.OperationConfig.End - task.OperationConfig.Start
		}
	} else {
		// if we are using sdk Batching call, then fetch the batch size from extras.
		// current default value of a batch for SDK batching is 100 but will be picked from os.env
		//if CheckBulkOperation(task.Operation) {
		if task.Extra.SDKBatchSize > 0 {
			batchSize = (task.OperationConfig.End - task.OperationConfig.Start) / int64(task.Extra.SDKBatchSize)
		} else {
			envBatchSize := os.Getenv("sirius_sdk_batch_size")
			if len(envBatchSize) == 0 {
				batchSize = 100
			} else {
				if x, e := strconv.Atoi(envBatchSize); e != nil {
					batchSize = int64(x)
				}
			}
			//}
		}

		if batchSize > (task.OperationConfig.End-task.OperationConfig.Start)/int64(MaxThreads) {
			batchSize = (task.OperationConfig.End - task.OperationConfig.Start) / int64(MaxThreads)
		}
	}

	if batchSize > 0 {
		numOfBatches = (task.OperationConfig.End - task.OperationConfig.Start) / batchSize
	}
	remainingItems := (task.OperationConfig.End - task.OperationConfig.Start) - (numOfBatches * batchSize)

	// ==========================================================================================
	//         Handling the Loading Task for a few special External Storage Operations         //
	// ==========================================================================================

	/*
	 * Specifically handling the loading for InsertFilesInFoldersOperation and UpdateFilesInFolderOperation.
	 * As for these operations, we need to create multiple files and in each file we need to bulk insert multiple documents.
	 * So each file creation will be in a new batch which takes its own thread. So multiple batches will be created for multiple files insert.
	 * numOfBatches = number of file paths which is simply the number of files to be inserted.
	 * batchSize = Num of Docs to be generated to get the file size in range [MinFileSize, MaxFileSize-1]
	 */
	if task.Operation == InsertFilesInFoldersOperation {
		// Num of Batches = Num Folders * Max Depth * Folders per Depth * Num Files per Folder = len(filePaths)
		folderPaths := generateFolderPaths(task.ExternalStorageExtras.NumFolders, task.ExternalStorageExtras.MaxFolderDepth,
			task.ExternalStorageExtras.FoldersPerDepth, task.ExternalStorageExtras.FolderLevelNames)
		filePaths := generateFilePaths(folderPaths, task.ExternalStorageExtras.FilesPerFolder, task.ExternalStorageExtras.FileFormat)
		numOfBatches = int64(len(filePaths))

		log.Println("folder paths")
		log.Println(folderPaths)

		// Creating all the necessary folders before inserting files.
		// Skipping first index 0 as it is root "" path
		as3 := external_storage.NewAmazonS3ConnectionManager()
		err := as3.Connect(task.ExternalStorageExtras)
		if err != nil {
			log.Println("In handler.go getInfoTask(), err connecting to aws:", err)
			return
		}
		for i := 1; i < len(folderPaths); i++ {
			task.ExternalStorageExtras.FolderPath = folderPaths[i]
			_ = as3.CreateFolder(external_storage.KeyValue{}, task.ExternalStorageExtras)
		}
		err = as3.Close(task.ExternalStorageExtras.AwsAccessKey)
		if err != nil {
			log.Println("In handler.go getInfoTask(), err disconnecting :", err)
			return
		}

		log.Println("file paths")
		log.Println(filePaths)

		// Batch Size = Num of Docs to generate to get file size somewhere between MinFileSize and MaxFileSize
		var documentSize int64
		if task.ExternalStorageExtras.MaxFileSize != 0 && task.ExternalStorageExtras.MinFileSize != 0 {
			diffMinMax := task.ExternalStorageExtras.MaxFileSize - task.ExternalStorageExtras.MinFileSize
			if diffMinMax <= 0 {
				documentSize = 1048576 // 1 MB
			} else {
				documentSize = task.ExternalStorageExtras.MinFileSize + rand.Int63n(diffMinMax)
			}
		} else {
			documentSize = 1048576 // 1 MB
		}

		log.Println("documentSize =", documentSize)
		batchSize := documentSize / int64(task.OperationConfig.DocSize)

		log.Println("batchSize:", batchSize, "numOfBatches:", numOfBatches)
		t1 := time.Now()
		for i := int64(0); i < numOfBatches; i++ {
			// Making required changes in order to start operation of File Insert
			batchStart := i * batchSize
			batchEnd := (i + 1) * batchSize
			task.ExternalStorageExtras.FilePath = filePaths[i]
			extractedFileFormat := filepath.Ext(filePaths[i])               // E.g. returns ".json"
			task.ExternalStorageExtras.FileFormat = extractedFileFormat[1:] // Now it is "json"

			// Starting the Insert Task
			t := newLoadingTask(batchStart+task.OperationConfig.Start,
				batchEnd+task.OperationConfig.Start,
				task.MetaData.Seed,
				task.OperationConfig,
				task.Operation,
				task.rerun,
				task.gen,
				task.State,
				task.Result,
				task.DatabaseInformation,
				task.Extra,
				task.ExternalStorageExtras,
				task.req,
				task.MetaDataIdentifier(),
				wg)
			loadBatch(task, t, batchStart, batchEnd)
			wg.Add(1)
		}

		wg.Wait()
		log.Println("result ", task.ResultSeed, " time took: ", time.Now().Sub(t1))
		log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
		log.Println()
		return
	} else if task.Operation == UpdateFilesInFolderOperation {
		// Here, we have to update files in a single folder. So we generate the file paths w.r.t that folder
		filePaths := generateFilePaths([]string{task.ExternalStorageExtras.FolderPath}, task.ExternalStorageExtras.FilesPerFolder, task.ExternalStorageExtras.FileFormat)
		numOfBatches = int64(len(filePaths))

		// Just making sure that the given folder is created in S3 before updating files.
		as3 := external_storage.NewAmazonS3ConnectionManager()
		err := as3.Connect(task.ExternalStorageExtras)
		if err != nil {
			log.Println("In handler.go getInfoTask(), err connecting to aws:", err)
			return
		}
		_ = as3.CreateFolder(external_storage.KeyValue{}, task.ExternalStorageExtras)
		err = as3.Close(task.ExternalStorageExtras.AwsAccessKey)
		if err != nil {
			log.Println("In handler.go getInfoTask(), err disconnecting :", err)
			return
		}

		log.Println("file paths")
		log.Println(filePaths)

		// Batch Size = Num of Docs to generate to get file size somewhere between MinFileSize and MaxFileSize
		var documentSize int64
		if task.ExternalStorageExtras.MaxFileSize != 0 && task.ExternalStorageExtras.MinFileSize != 0 {
			diffMinMax := task.ExternalStorageExtras.MaxFileSize - task.ExternalStorageExtras.MinFileSize
			if diffMinMax <= 0 {
				documentSize = 1048576 // 1 MB
			} else {
				documentSize = task.ExternalStorageExtras.MinFileSize + rand.Int63n(diffMinMax)
			}
		} else {
			documentSize = 1048576 // 1 MB
		}

		log.Println("documentSize =", documentSize)
		batchSize := documentSize / int64(task.OperationConfig.DocSize)

		log.Println("batchSize:", batchSize, "numOfBatches:", numOfBatches)
		t1 := time.Now()
		for i := int64(0); i < numOfBatches; i++ {
			// Making required changes in order to start operation of File Insert
			batchStart := i * batchSize
			batchEnd := (i + 1) * batchSize
			task.ExternalStorageExtras.FilePath = filePaths[i]
			extractedFileFormat := filepath.Ext(filePaths[i])               // E.g. returns ".json"
			task.ExternalStorageExtras.FileFormat = extractedFileFormat[1:] // Now it is "json"

			// Starting the Insert Task
			t := newLoadingTask(batchStart+task.OperationConfig.Start,
				batchEnd+task.OperationConfig.Start,
				task.MetaData.Seed,
				task.OperationConfig,
				task.Operation,
				task.rerun,
				task.gen,
				task.State,
				task.Result,
				task.DatabaseInformation,
				task.Extra,
				task.ExternalStorageExtras,
				task.req,
				task.MetaDataIdentifier(),
				wg)
			loadBatch(task, t, batchStart, batchEnd)
			wg.Add(1)
		}

		wg.Wait()
		log.Println("result ", task.ResultSeed, " time took: ", time.Now().Sub(t1))
		log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
		log.Println()
		return
	}

	// ==========================================================================================
	//         Loading the batches for normal Database or External Storage Operations          //
	// ==========================================================================================

	log.Println("batchSize:", batchSize, "numOfBatches:", numOfBatches, "remainingItems:", remainingItems)
	t1 := time.Now()
	for i := int64(0); i < numOfBatches; i++ {
		// if task.DBType == "dynamodb" && i > 0 && i%1000 == 0 {
		// 	time.Sleep(1000 * time.Millisecond)
		// }
		batchStart := i * batchSize
		batchEnd := (i + 1) * batchSize
		t := newLoadingTask(batchStart+task.OperationConfig.Start,
			batchEnd+task.OperationConfig.Start,
			task.MetaData.Seed,
			task.OperationConfig,
			task.Operation,
			task.rerun,
			task.gen,
			task.State,
			task.Result,
			task.DatabaseInformation,
			task.Extra,
			task.ExternalStorageExtras,
			task.req,
			task.MetaDataIdentifier(),
			wg)
		loadBatch(task, t, batchStart, batchEnd)
		wg.Add(1)
	}

	if remainingItems > 0 {
		t := newLoadingTask(
			numOfBatches*batchSize+task.OperationConfig.Start,
			task.OperationConfig.End,
			task.MetaData.Seed,
			task.OperationConfig,
			task.Operation,
			task.rerun,
			task.gen,
			task.State,
			task.Result,
			task.DatabaseInformation,
			task.Extra,
			task.ExternalStorageExtras,
			task.req,
			task.MetaDataIdentifier(),
			wg)
		loadBatch(task, t, numOfBatches*batchSize, task.OperationConfig.End)
		wg.Add(1)
	}

	wg.Wait()
	log.Println("result ", task.ResultSeed, " time took: ", time.Now().Sub(t1))
	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
	log.Println()

}

func (t *GenericLoadingTask) PostTaskExceptionHandling() {

	if t.State == nil {
		t.State = task_state.ConfigTaskState(t.ResultSeed)
	}
	t.State.StopStoringState()

	// For the offset in ignore exceptions :-> move them from error to completed
	shiftErrToCompletedOnIgnore(t.OperationConfig.Exceptions.IgnoreExceptions, t.Result, t.State)

	exceptionList := GetExceptions(t.Result, t.OperationConfig.Exceptions.RetryExceptions)

	for _, exception := range exceptionList {

		routineLimiter := make(chan struct{}, MaxRetryingRoutines)
		dataChannel := make(chan int64, MaxRetryingRoutines)

		failedDocuments := t.Result.BulkError[exception]
		delete(t.Result.BulkError, exception)

		wg := errgroup.Group{}
		for _, x := range failedDocuments {

			t.Result.Failure--
			t.State.RemoveOffsetFromErrSet(x.Offset)

			dataChannel <- x.Offset
			routineLimiter <- struct{}{}

			wg.Go(func() error {

				offset := <-dataChannel
				l := loadingTask{
					start:           offset,
					end:             offset + 1,
					operationConfig: t.OperationConfig,
					seed:            t.MetaData.Seed,
					operation:       t.Operation,
					rerun:           true,
					gen:             t.gen,
					state:           t.State,
					result:          t.Result,
					databaseInfo:    t.DatabaseInformation,
					extra:           t.Extra,
					req:             t.req,
					identifier:      t.IdentifierToken,
					wg:              nil}
				l.Run()

				<-routineLimiter
				return nil
			})
		}
		close(routineLimiter)
		close(dataChannel)
		_ = wg.Wait()
	}

	log.Println("completed retrying:- ", t.Operation, t.IdentifierToken, t.ResultSeed)
	_ = t.TearUp()
}

func (t *GenericLoadingTask) MatchResultSeed(resultSeed string) (bool, error) {
	defer t.lock.Unlock()
	t.lock.Lock()
	if fmt.Sprintf("%d", t.ResultSeed) == resultSeed {
		if t.TaskPending {
			return true, err_sirius.TaskInPendingState
		}
		if t.Result == nil {
			t.Result = task_result.ConfigTaskResult(t.Operation, t.ResultSeed)
		}
		return true, nil
	}
	return false, nil
}

func (t *GenericLoadingTask) SetException(exceptions Exceptions) {
	t.OperationConfig.Exceptions = exceptions
}

func (t *GenericLoadingTask) GetOperationConfig() (*OperationConfig, *task_state.TaskState) {

	return t.OperationConfig, t.State
}
