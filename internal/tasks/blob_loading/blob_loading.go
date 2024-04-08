package blob_loading

import (
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"github.com/couchbaselabs/sirius/internal/external_storage"
	"github.com/couchbaselabs/sirius/internal/meta_data"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/template"

	"golang.org/x/sync/errgroup"
)

type BlobLoadingTask struct {
	IdentifierToken string `json:"identifierToken" doc:"true"`
	tasks.DatabaseInformation
	ExternalStorageExtras external_storage.ExternalStorageExtras `json:"externalStorageExtras" doc:"true"`
	ResultSeed            int64                                  `json:"resultSeed" doc:"false"`
	TaskPending           bool                                   `json:"taskPending" doc:"false"`
	State                 *task_state.TaskState                  `json:"State" doc:"false"`
	MetaData              *meta_data.CollectionMetaData          `json:"metaData" doc:"false"`
	OperationConfig       *OperationConfig                       `json:"operationConfig" doc:"true"`
	Operation             string                                 `json:"operation" doc:"false"`
	Result                *task_result.TaskResult                `json:"-" doc:"false"`
	gen                   *docgenerator.Generator                `json:"-" doc:"false"`
	req                   *tasks.Request                         `json:"-" doc:"false"`
	rerun                 bool                                   `json:"-" doc:"false"`
	lock                  sync.Mutex                             `json:"-" doc:"false"`
}

func (t *BlobLoadingTask) Describe() string {
	return "Do operation between range from [start,end)"
}

func (t *BlobLoadingTask) MetaDataIdentifier() string {
	if t.DBType == external_storage.AmazonS3Storage {
		return strings.Join([]string{t.IdentifierToken, t.ExternalStorageExtras.AwsAccessKey, t.ExternalStorageExtras.AwsRegion}, ":")
	} else {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr}, ":")
	}
}

func (t *BlobLoadingTask) CheckIfPending() bool {
	return t.TaskPending
}

// Config configures the insert task
func (t *BlobLoadingTask) Config(req *tasks.Request, reRun bool) (int64, error) {
	t.TaskPending = true
	t.req = req

	if t.req == nil {
		t.TaskPending = false
		return 0, err_sirius.RequestIsNil
	}

	if extStorage, err := external_storage.ConfigExternalStorage(t.DBType); err != nil {
		return 0, err
	} else {
		if err = extStorage.Connect(t.ExternalStorageExtras); err != nil {
			return 0, err
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

		if err := configExtraParameters(t.DBType, &t.ExternalStorageExtras); err != nil {
			return 0, err
		}

		t.req.Lock()
		t.MetaData = t.req.MetaData.GetCollectionMetadata(t.MetaDataIdentifier())
		if t.OperationConfig.End+t.MetaData.Seed > t.MetaData.SeedEnd {
			t.req.AddToSeedEnd(t.MetaData, (t.OperationConfig.End+t.MetaData.Seed)-(t.MetaData.SeedEnd))
		}
		t.req.Unlock()

		t.State = task_state.ConfigTaskState(t.ResultSeed)

	} else {
		if t.State == nil {
			t.State = task_state.ConfigTaskState(t.ResultSeed)
		} else {
			t.State.SetupStoringKeys()
		}
		_ = task_result.DeleteResultFile(t.ResultSeed)
		log.Println("retrying :- ", t.Operation, t.IdentifierToken, t.ResultSeed)
	}

	t.gen = docgenerator.ConfigGenerator(
		t.OperationConfig.KeySize,
		t.OperationConfig.DocSize,
		template.InitialiseTemplate(t.OperationConfig.TemplateName))

	return t.ResultSeed, nil
}

func (t *BlobLoadingTask) TearUp() error {

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

func (t *BlobLoadingTask) Do() {

	t.Result = task_result.ConfigTaskResult(t.Operation, t.ResultSeed, 0)

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
}

// loadDocumentsInBatches divides the load into batches and will allocate to one of go routine.
func loadDocumentsInBatches(task *BlobLoadingTask) {

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

	/*
		 * 	For External Storage operations, we need to handle the batchSize and numOfBatches differently.
		 * 	To insert a single file we need to use 1 thread (or 1 batch) and in that thread bulk documents will be created
			and appended to create the file.
		 * 	Therefore, numOfBatches shall be = 1 for most File operations, except for InsertFilesInFoldersOperation and
			UpdateFilesInFolderOperation for which we have handled the bulk loading explicitly.
		 *	Batch size becomes the number of documents to be inserted to create a single file hence: Batch Size = End - Start
	*/
	numOfBatches := int64(1)
	batchSize := task.OperationConfig.End - task.OperationConfig.Start
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
	if task.Operation == tasks.InsertFilesInFoldersOperation {

		// Num of Batches = Num Folders * Max Depth * Folders per Depth * Num Files per Folder = len(filePaths)
		folderPaths := GenerateFolderPaths(task.ExternalStorageExtras.NumFolders, task.ExternalStorageExtras.MaxFolderDepth,
			task.ExternalStorageExtras.FoldersPerDepth, task.ExternalStorageExtras.FolderLevelNames)
		filePaths := GenerateFilePaths(folderPaths, task.ExternalStorageExtras.FilesPerFolder, task.ExternalStorageExtras.FileFormat)
		numOfBatches = int64(len(filePaths))

		log.Println("folder paths")
		log.Println(folderPaths)
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
		batchSize = documentSize / int64(task.OperationConfig.DocSize)

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
			t := newBlobLoadingTask(batchStart+task.OperationConfig.Start,
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
			loadBatch(task, t, batchStart, batchEnd, nil)
			wg.Add(1)
		}

		wg.Wait()
		log.Println("result ", task.ResultSeed, " time took: ", time.Now().Sub(t1))
		log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
		log.Println()
		return

	} else if task.Operation == tasks.UpdateFilesInFolderOperation {

		// We have to update the files present in the given folder. So first, we get the bucket directory structure and
		// then get the list of all files in the requested folder. Based on the file formats specified and num of folders
		// to be updated we select the files to update and then pass them in separate batches.
		extStorage, err := external_storage.ConfigExternalStorage(task.DBType)
		if err != nil {
			log.Println("load documents in batch:", err)
			return
		}

		bucketStructure, err := extStorage.GetInfo(task.ExternalStorageExtras)
		if err != nil {
			log.Println("load documents in batch:", err)
			return
		}

		folderPath := task.ExternalStorageExtras.FolderPath
		folderLvl := strings.Count(folderPath, "/") // Stores the depth of the folder
		subFolders := strings.Split(folderPath, "/")

		folder := bucketStructure.(external_storage.Folder)
		tempFolder := folder.Folders
		tempFiles := folder.Files

		// Traversing the folders in the bucket directory structure to get to the required folder of interest
		for i := 0; i < folderLvl; i++ {
			tempFiles = tempFolder[subFolders[i]].Files
			tempFolder = tempFolder[subFolders[i]].Folders
		}

		// Now tempFiles has all the files present in the folder in which we want to update files
		var probableFilePathsToUpdate, filePathsToUpdate []string
		for key := range tempFiles {
			probableFilePathsToUpdate = append(probableFilePathsToUpdate, folderPath+key)
		}

		if check := external_storage.ValidateFileFormat(task.ExternalStorageExtras.FileFormat); !check {
			log.Println("load documents in batch: file format not correct")
			return
		}
		fileFormatsArray := strings.Split(task.ExternalStorageExtras.FileFormat, ",")
		fileFormatsMap := make(map[string]string)

		for i := range fileFormatsArray {
			fileFormatsArray[i] = strings.TrimSpace(fileFormatsArray[i])
			formatExt := "." + fileFormatsArray[i]
			fileFormatsMap[formatExt] = fileFormatsArray[i]
		}
		numFilesToBeUpdated := task.ExternalStorageExtras.FilesPerFolder

		for i := 0; i < len(probableFilePathsToUpdate) && numFilesToBeUpdated > 0; i++ {
			if _, ok := fileFormatsMap[filepath.Ext(probableFilePathsToUpdate[i])]; ok {
				filePathsToUpdate = append(filePathsToUpdate, probableFilePathsToUpdate[i])
				numFilesToBeUpdated--
			}
		}

		// Here, we have to update files in a single folder. So we generate the file paths w.r.t that folder
		//filePaths := GenerateFilePaths([]string{task.ExternalStorageExtras.FolderPath}, task.ExternalStorageExtras.FilesPerFolder, task.ExternalStorageExtras.FileFormat)
		numOfBatches = int64(len(filePathsToUpdate))

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
			task.ExternalStorageExtras.FilePath = filePathsToUpdate[i]
			extractedFileFormat := filepath.Ext(filePathsToUpdate[i])       // E.g. returns ".json"
			task.ExternalStorageExtras.FileFormat = extractedFileFormat[1:] // Now it is "json"

			// Starting the Insert Task
			t := newBlobLoadingTask(batchStart+task.OperationConfig.Start,
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
			loadBatch(task, t, batchStart, batchEnd, nil)
			wg.Add(1)
		}

		wg.Wait()
		log.Println("result ", task.ResultSeed, " time took: ", time.Now().Sub(t1))
		log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
		log.Println()
		return
	}

	// ==========================================================================================
	//               Loading the batches for normal External Storage Operations                //
	// ==========================================================================================

	log.Println("batchSize:", batchSize, "numOfBatches:", numOfBatches, "remainingItems:", remainingItems)
	t1 := time.Now()
	for i := int64(0); i < numOfBatches; i++ {
		batchStart := i * batchSize
		batchEnd := (i + 1) * batchSize
		t := newBlobLoadingTask(batchStart+task.OperationConfig.Start,
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
		loadBatch(task, t, batchStart, batchEnd, nil)
		wg.Add(1)
	}

	if remainingItems > 0 {
		t := newBlobLoadingTask(
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
		loadBatch(task, t, numOfBatches*batchSize, task.OperationConfig.End, nil)
		wg.Add(1)
	}

	wg.Wait()
	log.Println("result ", task.ResultSeed, " time took: ", time.Now().Sub(t1))
	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
	log.Println()

}

func (t *BlobLoadingTask) PostTaskExceptionHandling() {

	shiftErrToCompletedOnIgnore(t.OperationConfig.Exceptions.IgnoreExceptions, t.Result, t.State)

	_ = t.State.SaveTaskSateOnDisk()

	exceptionList := GetExceptions(t.Result, t.OperationConfig.Exceptions.RetryExceptions)

	for _, exception := range exceptionList {
		routineLimiter := make(chan struct{}, tasks.MaxRetryingRoutines)
		dataChannel := make(chan int64, tasks.MaxRetryingRoutines)

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
				l := blobLoadingTask{
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

func (t *BlobLoadingTask) MatchResultSeed(resultSeed string) (bool, error) {
	defer t.lock.Unlock()
	t.lock.Lock()
	if fmt.Sprintf("%d", t.ResultSeed) == resultSeed {
		if t.TaskPending {
			return true, err_sirius.TaskInPendingState
		}
		if t.Result == nil {
			t.Result = task_result.ConfigTaskResult(t.Operation, t.ResultSeed, 0)
		}
		return true, nil
	}
	return false, nil
}

func (t *BlobLoadingTask) SetException(exceptions Exceptions) {
	t.OperationConfig.Exceptions = exceptions
}

func (t *BlobLoadingTask) GetOperationConfig() (*OperationConfig, *task_state.TaskState) {
	return t.OperationConfig, t.State
}
