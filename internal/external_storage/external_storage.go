package external_storage

import (
	"sync"

	"github.com/couchbaselabs/sirius/internal/err_sirius"
)

const (
	AmazonS3Storage = "awsS3"
)

type FileOperationResult interface {
	Value(key string) interface{}
	GetStatus(key string) bool
	GetError(key string) error
	GetExtra(key string) map[string]any
	GetOffset(key string) int64
	GetSize() int
}

type FolderOperationResult interface {
	Key() string
	GetValue() interface{}
	GetError() error
	GetExtra() map[string]any
	GetOffset() int64
}

type ExternalStorage interface {
	Connect(extra ExternalStorageExtras) error
	CreateBucket(keyValue KeyValue, extra ExternalStorageExtras) FolderOperationResult
	DeleteBucket(keyValue KeyValue, extra ExternalStorageExtras) FolderOperationResult
	CreateFolder(keyValue KeyValue, extra ExternalStorageExtras) FolderOperationResult
	DeleteFolder(keyValue KeyValue, extra ExternalStorageExtras) FolderOperationResult
	CreateFile(pathToFileOnDisk string, keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult
	UpdateFile(pathToFileOnDisk string, keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult
	DeleteFile(keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult
	CreateFilesInFolder(keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult
	UpdateFilesInFolder(keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult
	DeleteFilesInFolder(keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult
	GetInfo(extra ExternalStorageExtras) (interface{}, error)
	Warmup(extra ExternalStorageExtras) error
	Close(clusterIdentifier string) error
}

var awsS3 *AmazonS3

var lock = &sync.Mutex{}

func ConfigExternalStorage(dbType string) (ExternalStorage, error) {
	switch dbType {
	case AmazonS3Storage:
		if awsS3 == nil {
			lock.Lock()
			defer lock.Unlock()
			if awsS3 == nil {
				awsS3 = NewAmazonS3ConnectionManager()
			}
		}
		return awsS3, nil
	default:
		return nil, err_sirius.InvalidExternalStorage
	}
}
