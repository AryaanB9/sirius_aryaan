package task_result

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"golang.org/x/sync/errgroup"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	ResultPath         = "./internal/task_result/task_result_logs"
	ResultChannelLimit = 10000000
)

type SDKTiming struct {
	SendTime string `json:"sendTime" doc:"true"`
	AckTime  string `json:"ackTime" doc:"true"`
}

type FailedDocument struct {
	SDKTiming SDKTiming      `json:"sdkTimings" doc:"true"`
	DocId     string         `json:"key" doc:"true"`
	Status    bool           `json:"status"  doc:"true"`
	Extra     map[string]any `json:"extra" doc:"true""`
	Offset    int64          `json:"Offset" doc:"false"`
}

type SingleOperationResult struct {
	SDKTiming   SDKTiming `json:"sdkTimings" doc:"true"`
	ErrorString string    `json:"errorString"  doc:"true"`
	Status      bool      `json:"status"  doc:"true"`
	Cas         uint64    `json:"cas"  doc:"true"`
}

type FailedQuery struct {
	Query       string `json:"query" doc:"true"`
	ErrorString string `json:"errorString" doc:"true"`
}

type ResultHelper struct {
	initTime string
	docId    string
	err      error
	status   bool
	extra    map[string]any
	offset   int64
}

// TaskResult defines the type of result stored in a response after an operation.
type TaskResult struct {
	Count         int64                            `json:"Count"`
	ResultSeed    int64                            `json:"resultSeed"`
	Operation     string                           `json:"operation"`
	ErrorOther    string                           `json:"otherErrors"`
	Success       int64                            `json:"success"`
	Failure       int64                            `json:"failure"`
	BulkError     map[string][]FailedDocument      `json:"bulkErrors"`
	RetriedError  map[string][]FailedDocument      `json:"retriedError"`
	QueryError    map[string][]FailedQuery         `json:"queryErrors"`
	SingleResult  map[string]SingleOperationResult `json:"singleResult"`
	ResultChannel chan ResultHelper                `json:"-"`
	lock          sync.Mutex                       `json:"-"`
	ctx           context.Context                  `json:"-"`
	cancel        context.CancelFunc               `json:"-"`
}

// ConfigTaskResult returns a new instance of TaskResult
func ConfigTaskResult(operation string, resultSeed int64, count int64) *TaskResult {
	ctx, cancel := context.WithCancel(context.Background())
	taskResult := &TaskResult{}

	if result, err := ReadResultFromFile(fmt.Sprintf("%d", resultSeed), false); err == nil {
		taskResult = result
		taskResult.ResultChannel = make(chan ResultHelper, ResultChannelLimit)
		taskResult.ctx = ctx
		taskResult.cancel = cancel
		taskResult.lock = sync.Mutex{}
	} else {
		taskResult = &TaskResult{
			Count:         count,
			ResultSeed:    resultSeed,
			Operation:     operation,
			BulkError:     make(map[string][]FailedDocument),
			RetriedError:  make(map[string][]FailedDocument),
			QueryError:    make(map[string][]FailedQuery),
			SingleResult:  make(map[string]SingleOperationResult),
			ResultChannel: make(chan ResultHelper, count),
			lock:          sync.Mutex{},
			ctx:           ctx,
			cancel:        cancel,
		}
	}

	defer func() {
		taskResult.StoreResult()
	}()

	return taskResult
}

// IncrementFailure saves the failure count of doc loading operation.
func (t *TaskResult) IncrementFailure(initTime, docId string, err error, status bool, extra map[string]any,
	offset int64) {

	t.ResultChannel <- ResultHelper{
		initTime: initTime,
		docId:    docId,
		err:      err,
		status:   status,
		extra:    extra,
		offset:   offset,
	}
}

// IncrementQueryFailure saves the failure count of query running operation.
func (t *TaskResult) IncrementQueryFailure(query string, err error) {
	t.lock.Lock()
	t.Failure++
	v, errorString := db.CheckSDKException(err)
	t.QueryError[v] = append(t.QueryError[v], FailedQuery{
		Query:       query,
		ErrorString: errorString,
	})
	t.lock.Unlock()
}

// SaveResultIntoFile stores the task result on a file. It returns an error if saving fails.
func (t *TaskResult) SaveResultIntoFile() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, ResultPath, fmt.Sprintf("%d", t.ResultSeed))
	content, err := json.MarshalIndent(t, "", "\t")
	if err != nil {
		return err
	}
	err = os.WriteFile(fileName, content, 0644)
	if err != nil {
		return err
	}
	return nil
}

// ReadResultFromFile reads the task result stored on a file. It returns the task result
// and possible error if task result file is missing, in processing or record file deleted.
func ReadResultFromFile(seed string, deleteRecord bool) (*TaskResult, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	fileName := filepath.Join(cwd, ResultPath, seed)
	result := &TaskResult{}
	content, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("no such result found, reasons:[No such Task, In process, Record Deleted]")
	}
	if err := json.Unmarshal(content, result); err != nil {
		return nil, err
	}
	// deleting the file after reading it to save disk space.
	if deleteRecord {
		if err := os.Remove(fileName); err != nil {
			log.Println("Manually clean " + fileName)
		}
	}
	return result, nil
}

func (t *TaskResult) CreateSingleErrorResult(initTime, docId string, errorString string, status bool, cas uint64) {
	defer t.lock.Unlock()
	t.lock.Lock()
	if !status {
		t.Failure++
	}
	t.SingleResult[docId] = SingleOperationResult{
		SDKTiming: SDKTiming{
			SendTime: initTime,
			AckTime:  time.Now().UTC().Format(time.RFC850),
		},
		ErrorString: errorString,
		Status:      status,
		Cas:         cas,
	}
}

func (t *TaskResult) FailWholeBulkOperation(start, end int64, err error, state *task_state.TaskState,
	gen *docgenerator.Generator, seed int64) {

	const routineLimit = 10
	routineLimiter := make(chan struct{}, routineLimit)
	dataChannel := make(chan int64, routineLimit)

	wg := errgroup.Group{}
	initTime := time.Now().UTC().Format(time.RFC850)
	for i := start; i < end; i++ {
		routineLimiter <- struct{}{}
		dataChannel <- i
		wg.Go(func() error {
			offset := <-dataChannel
			docId := gen.BuildKey(offset + seed)
			t.IncrementFailure(initTime, docId, err, false, nil, offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			<-routineLimiter
			return nil
		})
	}
	_ = wg.Wait()
}

func (t *TaskResult) FailWholeSingleOperation(docIds []string, err error) {
	t.Failure = int64(len(docIds))

	const routineLimit = 10
	routineLimiter := make(chan struct{}, routineLimit)
	dataChannel := make(chan string, routineLimit)
	initTime := time.Now().UTC().Format(time.RFC850)
	wg := errgroup.Group{}

	for _, docId := range docIds {
		routineLimiter <- struct{}{}
		dataChannel <- docId

		wg.Go(func() error {
			t.CreateSingleErrorResult(initTime, <-dataChannel, err.Error(), false, 0)
			<-routineLimiter
			return nil
		})
	}
	_ = wg.Wait()
}

func (t *TaskResult) StoreResult() {
	go func() {
		resultList := make([]ResultHelper, 0, t.Count)
		d := time.NewTicker(10 * time.Second)
		defer d.Stop()
		if t.ctx.Err() != nil {
			log.Print("Ctx closed for StoreResult()")
			return
		}
		for {
			select {
			case <-t.ctx.Done():
				{
					for s := range t.ResultChannel {
						resultList = append(resultList, s)
					}
					t.StoreResultList(resultList)
					resultList = resultList[:0]
					return
				}
			case s := <-t.ResultChannel:
				{
					resultList = append(resultList, s)
				}
			}
		}
	}()
}

func (t *TaskResult) StoreResultList(resultList []ResultHelper) {
	for _, x := range resultList {
		if x.err == nil {
			continue
		}
		t.Failure++
		v, errorString := db.CheckSDKException(x.err)
		if v == "unknown exception" {
			v += errorString
		}
		t.BulkError[v] = append(t.BulkError[v], FailedDocument{
			SDKTiming: SDKTiming{
				SendTime: x.initTime,
				AckTime:  time.Now().UTC().Format(time.RFC850),
			},
			DocId:  x.docId,
			Status: x.status,
			Extra:  x.extra,
			Offset: x.offset,
		})
	}
	t.Success = t.Count - t.Failure
	_ = t.SaveResultIntoFile()
	log.Println(t.ResultSeed, "result saved on disk")
}

func (t *TaskResult) StopStoringResult() {
	if t.ctx.Err() != nil {
		return
	}
	t.cancel()
	close(t.ResultChannel)
}

// DeleteResultFile deletes the result file
func DeleteResultFile(resultSeed int64) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, ResultPath, fmt.Sprintf("%d", resultSeed))

	if err := os.Remove(fileName); err != nil {
		log.Println("Manually clean " + fileName)
		return err
	}
	return nil
}
