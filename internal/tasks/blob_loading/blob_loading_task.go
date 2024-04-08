package blob_loading

import (
	"sync"

	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/external_storage"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
)

type BlobTask interface {
	tasks.Task
	PostTaskExceptionHandling()
	MatchResultSeed(resultSeed string) (bool, error)
	SetException(exceptions Exceptions)
	GetOperationConfig() (*OperationConfig, *task_state.TaskState)
	MetaDataIdentifier() string
}

type blobLoadingTask struct {
	start                 int64
	end                   int64
	operationConfig       *OperationConfig
	seed                  int64
	operation             string
	rerun                 bool
	gen                   *docgenerator.Generator
	state                 *task_state.TaskState
	result                *task_result.TaskResult
	databaseInfo          tasks.DatabaseInformation
	extra                 db.Extras
	externalStorageExtras external_storage.ExternalStorageExtras
	req                   *tasks.Request
	identifier            string
	wg                    *sync.WaitGroup
}

func newBlobLoadingTask(start, end, seed int64, operationConfig *OperationConfig,
	operation string, rerun bool, gen *docgenerator.Generator,
	state *task_state.TaskState, result *task_result.TaskResult, databaseInfo tasks.DatabaseInformation,
	extra db.Extras, externalStorageExtras external_storage.ExternalStorageExtras, req *tasks.Request, identifier string, wg *sync.WaitGroup) *blobLoadingTask {
	return &blobLoadingTask{
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

func (l *blobLoadingTask) Run() {
	switch l.operation {

	case tasks.BucketCreateOperation:
		{
			createBucket(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case tasks.BucketDeleteOperation:
		{
			deleteBucket(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case tasks.FolderInsertOperation:
		{
			insertFolder(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case tasks.FolderDeleteOperation:
		{
			deleteFolder(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case tasks.FileInsertOperation:
		{
			insertFile(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}

	case tasks.FileUpdateOperation:
		{
			insertFile(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case tasks.FileDeleteOperation:
		{
			deleteFile(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case tasks.InsertFilesInFoldersOperation:
		{
			insertFilesInFolders(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case tasks.UpdateFilesInFolderOperation:
		{
			updateFilesInFolder(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case tasks.DeleteFilesInFolderOperation:
		{
			deleteFilesInFolder(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	}
}
