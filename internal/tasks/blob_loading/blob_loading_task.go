package blob_loading

import (
	"sync"

	"github.com/AryaanB9/sirius_aryaan/internal/db"
	"github.com/AryaanB9/sirius_aryaan/internal/docgenerator"
	"github.com/AryaanB9/sirius_aryaan/internal/external_storage"
	"github.com/AryaanB9/sirius_aryaan/internal/task_result"
	"github.com/AryaanB9/sirius_aryaan/internal/task_state"
	"github.com/AryaanB9/sirius_aryaan/internal/tasks"
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

	case tasks.S3BucketCreateOperation:
		{
			createS3Bucket(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case tasks.S3BucketDeleteOperation:
		{
			deleteS3Bucket(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
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
			insertFiles(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}

	case tasks.FileUpdateOperation:
		{
			insertFiles(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.externalStorageExtras, l.wg)
		}
	case tasks.FileDeleteOperation:
		{
			deleteFiles(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
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
