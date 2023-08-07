package tasks

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

type SingleReadTask struct {
	IdentifierToken string                  `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket          string                  `json:"bucket" doc:"true"`
	Scope           string                  `json:"scope,omitempty" doc:"true"`
	Collection      string                  `json:"collection,omitempty" doc:"true"`
	OperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
	Operation       string                  `json:"operation" doc:"false"`
	ResultSeed      int64                   `json:"resultSeed" doc:"false"`
	TaskPending     bool                    `json:"taskPending" doc:"false"`
	result          *task_result.TaskResult `json:"-" doc:"false"`
	req             *Request                `json:"-" doc:"false"`
}

func (task *SingleReadTask) Describe() string {
	return "Single read task reads key value in couchbase and validates.\n"
}

func (task *SingleReadTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleReadTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleReadTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleReadTask) Config(req *Request, reRun bool) (int64, error) {
	task.TaskPending = true
	task.req = req

	if task.req == nil {
		task.TaskPending = false
		return 0, fmt.Errorf("request.Request struct is nil")
	}

	task.req.ReconnectionManager()
	if _, err := task.req.connectionManager.GetCluster(task.ClusterConfig); err != nil {
		task.TaskPending = false
		return 0, err
	}

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = SingleReadOperation

		if task.Bucket == "" {
			task.Bucket = DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}

		if err := configSingleOperationConfig(task.OperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
		}
	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SingleReadTask) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}
	task.result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleReadTask) Do() error {

	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	if err1 != nil {
		task.result.ErrorOther = err1.Error()
		var docIds []string
		for _, kV := range task.OperationConfig.KeyValue {
			docIds = append(docIds, kV.Key)
		}
		task.result.FailWholeSingleOperation(docIds, err1)
		return task.tearUp()
	}

	singleReadDocuments(task, collectionObject)

	task.result.Success = int64(len(task.OperationConfig.KeyValue)) - task.result.Failure
	return task.tearUp()
}

// singleDeleteDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleReadDocuments(task *SingleReadTask, collectionObject *sdk.CollectionObject) {

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan interface{}, MaxConcurrentRoutines)

	group := errgroup.Group{}

	for _, data := range task.OperationConfig.KeyValue {
		routineLimiter <- struct{}{}
		dataChannel <- data

		group.Go(func() error {
			keyValue := <-dataChannel
			kV, ok := keyValue.(KeyValue)
			if !ok {
				log.Println(task.Operation, task.CollectionIdentifier(), task.ResultSeed, errors.New("unable to decode Key Value for single crud"))
				<-routineLimiter
				return errors.New("unable to decode Key Value for single crud")
			}

			result, err := collectionObject.Collection.Get(kV.Key, nil)
			if err != nil {
				task.result.CreateSingleErrorResult(kV.Key, err.Error(),
					false, 0)
				<-routineLimiter
				return err
			}

			if task.OperationConfig.ReadYourOwnWrite {

				var resultFromHost map[string]any
				if err := result.Content(&resultFromHost); err != nil {
					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
						false, 0)
					<-routineLimiter
					return err
				}

				resultFromHostBytes, err := json.Marshal(resultFromHost)
				if err != nil {
					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
						false, 0)
					<-routineLimiter
					return err
				}
				resultFromDocBytes, err := json.Marshal(kV.Doc)
				if err != nil {
					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
						false, 0)
					<-routineLimiter
					return err
				}

				if !bytes.Equal(resultFromHostBytes, resultFromDocBytes) {
					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
						false, 0)
					<-routineLimiter
					return err
				}
			}

			task.result.CreateSingleErrorResult(kV.Key, "", true, uint64(result.Cas()))
			<-routineLimiter
			return nil
		})
	}

	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleReadTask) PostTaskExceptionHandling(_ *sdk.CollectionObject) {
}

func (task *SingleReadTask) GetResultSeed() string {
	return fmt.Sprintf("%d", task.result.ResultSeed)
}

func (task *SingleReadTask) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleReadTask) SetException(exceptions Exceptions) {

}
