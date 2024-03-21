package tasks

import (
	"fmt"
	"log"
	"os"
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
	ResultSeed      int64                         `json:"resultSeed" doc:"false"`
	TaskPending     bool                          `json:"taskPending" doc:"false"`
	State           *task_state.TaskState         `json:"State" doc:"false"`
	MetaData        *meta_data.CollectionMetaData `json:"metaData" doc:"false"`
	OperationConfig *OperationConfig              `json:"operationConfig" doc:"true"`
	Operation       string                        `json:"operation" doc:"false"`
	Result          *task_result.TaskResult       `json:"-" doc:"false"`
	gen             *docgenerator.Generator       `json:"-" doc:"false"`
	req             *Request                      `json:"-" doc:"false"`
	rerun           bool                          `json:"-" doc:"false"`
	lock            sync.Mutex                    `json:"-" doc:"false"`
}

func (t *GenericLoadingTask) Describe() string {
	return "Do operation between range from [start,end)"
}

func (t *GenericLoadingTask) MetaDataIdentifier() string {
	if t.DBType == db.CouchbaseDb {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr, t.Extra.Bucket, t.Extra.Scope,
			t.Extra.Collection}, ":")
	} else if t.DBType == db.MongoDb {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr, t.Extra.Collection}, ":")
	} else {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr}, ":")
	}
}

func (t *GenericLoadingTask) CheckIfPending() bool {
	return t.TaskPending
}

// Config configures  the insert task
func (t *GenericLoadingTask) Config(req *Request, reRun bool) (int64, error) {
	t.TaskPending = true
	t.req = req

	if t.req == nil {
		t.TaskPending = false
		return 0, err_sirius.RequestIsNil
	}

	if database, err := db.ConfigDatabase(t.DBType); err != nil {
		return 0, err
	} else {
		if err = database.Connect(t.ConnStr, t.Username, t.Password, t.Extra); err != nil {
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
	// if we are using sdk Batching call, then fetch the batch size from extras.
	// current default value of a batch for SDK batching is 100 but will be picked from os.env
	//if CheckBulkOperation(task.Operation) {
	if task.Extra.SDKBatchSize > 0 {
		batchSize = int64(task.Extra.SDKBatchSize)
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

	if batchSize > 0 {
		numOfBatches = (task.OperationConfig.End - task.OperationConfig.Start) / batchSize
	}
	remainingItems := (task.OperationConfig.End - task.OperationConfig.Start) - (numOfBatches * batchSize)

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
			task.req,
			task.MetaDataIdentifier(),
			wg)
		loadBatch(task, t, numOfBatches*batchSize, task.OperationConfig.End)
		wg.Add(1)
	}

	wg.Wait()
	log.Println("result ", task.ResultSeed, " time took: ", time.Now().Sub(t1))
	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)

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
