package blob_loading

import (
	"encoding/json"
	"errors"
	"github.com/AryaanB9/sirius_aryaan/internal/external_storage"
	"github.com/shettyh/threadpool"
	"log"
	"time"

	"github.com/AryaanB9/sirius_aryaan/internal/docgenerator"
	"github.com/AryaanB9/sirius_aryaan/internal/err_sirius"
	"github.com/AryaanB9/sirius_aryaan/internal/task_result"
	"github.com/AryaanB9/sirius_aryaan/internal/task_state"
	"github.com/AryaanB9/sirius_aryaan/internal/tasks"
)

// Exceptions will have list of errors to be ignored or to be retried
type Exceptions struct {
	IgnoreExceptions []string `json:"ignoreExceptions,omitempty" doc:"true"`
	RetryExceptions  []string `json:"retryExceptions,omitempty" doc:"true"`
}

// OperationConfig contains all the configuration for document operation.
type OperationConfig struct {
	DocSize        int        `json:"docSize" doc:"true"`
	DocType        string     `json:"docType,omitempty" doc:"true"`
	KeySize        int        `json:"keySize,omitempty" doc:"true"`
	TemplateName   string     `json:"template" doc:"true"`
	Start          int64      `json:"start" doc:"true"`
	End            int64      `json:"end" doc:"true"`
	FieldsToChange []string   `json:"fieldsToChange" doc:"true"`
	Exceptions     Exceptions `json:"exceptions,omitempty" doc:"true"`
}

func (o *OperationConfig) String() string {
	if o == nil {
		return "nil config"
	}
	b, err := json.Marshal(&o)
	log.Println(string(b))
	if err != nil {
		return ""
	}
	return ""
}

// ConfigureOperationConfig configures and validate the OperationConfig
func ConfigureOperationConfig(o *OperationConfig) error {
	if o == nil {
		return err_sirius.ParsingOperatingConfig
	}
	if o.DocType == "" {
		o.DocType = docgenerator.JsonDocument
	}

	if o.KeySize > docgenerator.DefaultKeySize {
		o.KeySize = docgenerator.DefaultKeySize
	}
	if o.DocSize <= 0 {
		o.DocSize = docgenerator.DefaultDocSize
	}
	if o.Start < 0 {
		o.Start = 0
		o.End = 0
	}
	if o.Start > o.End {
		o.End = o.Start
		return err_sirius.MalformedOperationRange
	}
	if o.TemplateName == "" {
		return err_sirius.InvalidTemplateName
	}
	return nil
}

func GetExceptions(result *task_result.TaskResult, RetryExceptions []string) []string {
	var exceptionList []string
	if len(RetryExceptions) == 0 {
		for exception, _ := range result.BulkError {
			exceptionList = append(exceptionList, exception)
		}
	} else {
		exceptionList = RetryExceptions
	}
	return exceptionList
}

// shiftErrToCompletedOnIgnore will ignore retrying operation for offset lying in ignore exception category
func shiftErrToCompletedOnIgnore(ignoreExceptions []string, result *task_result.TaskResult, state *task_state.TaskState) {
	for _, exception := range ignoreExceptions {
		for _, failedDocs := range result.BulkError[exception] {
			state.AddOffsetToCompleteSet(failedDocs.Offset)
		}
		for _, failedDocs := range result.BulkError[exception] {
			state.RemoveOffsetFromErrSet(failedDocs.Offset)
		}
		delete(result.BulkError, exception)
	}
}

func configExtraParameters(dbType string, extStorageExtras *external_storage.ExternalStorageExtras) error {

	switch dbType {
	case external_storage.AmazonS3Storage:
		if extStorageExtras.Bucket == "" {
			return err_sirius.BucketNotProvided
		}
	}

	return nil
}

// loadBatch will enqueue the batch to thread pool. if the queue is full,
// it will wait for sometime any thread to pick it up.
func loadBatch(task *BlobLoadingTask, t *blobLoadingTask, batchStart int64, batchEnd int64, _ *threadpool.ThreadPool) {

	retryBatchCounter := 10
	for ; retryBatchCounter > 0; retryBatchCounter-- {
		if err := tasks.Pool.Execute(t); err == nil {
			break
		}
		time.Sleep(1 * time.Minute)
	}
	if retryBatchCounter == 0 {
		task.Result.FailWholeBulkOperation(batchStart, batchEnd, errors.New("internal error, "+
			"sirius is overloaded"), task.State, task.gen, task.MetaData.Seed)
	}
}
