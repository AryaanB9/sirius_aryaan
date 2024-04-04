package tasks

const (
	DefaultIdentifierToken               = "default"
	WatchIndexDuration            int    = 120
	InsertOperation               string = "insert"
	BulkInsertOperation           string = "bulkInsert"
	QueryOperation                string = "query"
	DeleteOperation               string = "delete"
	BulkDeleteOperation           string = "bulkDelete"
	UpsertOperation               string = "upsert"
	BulkUpsertOperation           string = "bulkUpsert"
	ReadOperation                 string = "read"
	BulkReadOperation             string = "bulkRead"
	TouchOperation                string = "touch"
	BulkTouchOperation            string = "bulkTouch"
	ValidateOperation             string = "validate"
	ValidateDocOperation          string = "validateDoc"
	CreatePrimaryIndex            string = "createPrimaryIndex"
	CreateIndex                   string = "createIndex"
	BuildIndex                    string = "buildIndex"
	RetryExceptionOperation       string = "retryException"
	SubDocInsertOperation         string = "subDocInsert"
	SubDocDeleteOperation         string = "subDocDelete"
	SubDocUpsertOperation         string = "subDocUpsert"
	SubDocReadOperation           string = "subDocRead"
	SubDocReplaceOperation        string = "subDocReplace"
	BucketWarmUpOperation         string = "BucketWarmUp"
	CreateDBOperation             string = "createDatabase"
	DeleteDBOperation             string = "deleteCollection"
	ListDBOperation               string = "createDatabase"
	CountOperation                string = "countDocuments"
	BucketCreateOperation         string = "createS3Bucket"
	BucketDeleteOperation         string = "deleteS3Bucket"
	FolderInsertOperation         string = "folderInsert"
	FolderDeleteOperation         string = "folderDelete"
	FileInsertOperation           string = "fileInsert"
	FileUpdateOperation           string = "fileUpdate"
	FileDeleteOperation           string = "fileDelete"
	InsertFilesInFoldersOperation string = "insertFilesInFolders"
	UpdateFilesInFolderOperation  string = "updateFilesInFolder"
	DeleteFilesInFolderOperation  string = "deleteFilesInFolder"
	GetInfoOperation              string = "getInfo"
)

func CheckBulkOperation(operation string) bool {
	switch operation {
	case BulkInsertOperation, BulkUpsertOperation, BulkReadOperation, BulkDeleteOperation, BulkTouchOperation:
		return true
	default:
		return false
	}
}
