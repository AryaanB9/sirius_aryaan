package tasks

import (
	"sync"

	"github.com/barkha06/sirius/internal/db"
	"github.com/barkha06/sirius/internal/docgenerator"
	"github.com/barkha06/sirius/internal/external_storage"
	"github.com/barkha06/sirius/internal/task_result"
	"github.com/barkha06/sirius/internal/task_state"
)

type BulkTask interface {
	Task
	PostTaskExceptionHandling()
	MatchResultSeed(resultSeed string) (bool, error)
	SetException(exceptions Exceptions)
	GetOperationConfig() (*OperationConfig, *task_state.TaskState)
	MetaDataIdentifier() string
}

type loadingTask struct {
	start                 int64
	end                   int64
	operationConfig       *OperationConfig
	seed                  int64
	operation             string
	rerun                 bool
	gen                   *docgenerator.Generator
	state                 *task_state.TaskState
	result                *task_result.TaskResult
	databaseInfo          DatabaseInformation
	extra                 db.Extras
	externalStorageExtras external_storage.ExternalStorageExtras
	req                   *Request
	identifier            string
	wg                    *sync.WaitGroup
}

func newLoadingTask(start, end, seed int64, operationConfig *OperationConfig,
	operation string, rerun bool, gen *docgenerator.Generator,
	state *task_state.TaskState, result *task_result.TaskResult, databaseInfo DatabaseInformation,
	extra db.Extras, externalStorageExtras external_storage.ExternalStorageExtras, req *Request, identifier string, wg *sync.WaitGroup) *loadingTask {
	return &loadingTask{
		start:                 start,
		end:                   end,
		seed:                  seed,
		operationConfig:       operationConfig,
		operation:             operation,
		rerun:                 rerun,
		gen:                   gen,
		state:                 state,
		result:                result,
		databaseInfo:          databaseInfo,
		externalStorageExtras: externalStorageExtras,
		extra:                 extra,
		req:                   req,
		identifier:            identifier,
		wg:                    wg,
	}
}

func (l *loadingTask) Run() {
	switch l.operation {
	case InsertOperation:
		{
			insertDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case UpsertOperation:
		{
			upsertDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.req, l.identifier, l.wg)
		}
	case DeleteOperation:
		{
			deleteDocuments(l.start, l.end, l.seed, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case ReadOperation:
		{
			readDocuments(l.start, l.end, l.seed, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case TouchOperation:
		{
			touchDocuments(l.start, l.end, l.seed, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case SubDocInsertOperation:
		{
			subDocInsertDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case SubDocDeleteOperation:
		{
			subDocDeleteDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case SubDocReadOperation:
		{
			subDocReadDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case SubDocReplaceOperation:
		{
			subDocReplaceDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.req, l.identifier, l.wg)
		}
	case SubDocUpsertOperation:
		{
			subDocUpsertDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.req, l.identifier, l.wg)
		}

	case BulkInsertOperation:
		{
			bulkInsertDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case BulkUpsertOperation:
		{
			bulkUpsertDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.req, l.identifier, l.wg)
		}
	case BulkDeleteOperation:
		{
			bulkDeleteDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case BulkReadOperation:
		{
			bulkReadDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case BulkTouchOperation:
		{
			bulkTouchDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case ValidateDocOperation:
		{
			validateDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}

	case S3BucketCreateOperation:
		{
			createS3Bucket(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case S3BucketDeleteOperation:
		{
			deleteS3Bucket(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case FolderInsertOperation:
		{
			insertFolder(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case FolderDeleteOperation:
		{
			deleteFolder(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case FileInsertOperation:
		{
			insertFiles(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}

	case FileUpdateOperation:
		{
			insertFiles(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case FileDeleteOperation:
		{
			deleteFiles(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case InsertFilesInFoldersOperation:
		{
			insertFilesInFolders(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case UpdateFilesInFolderOperation:
		{
			updateFilesInFolder(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case DeleteFilesInFolderOperation:
		{
			deleteFilesInFolder(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	}
}
