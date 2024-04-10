package external_storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/couchbaselabs/sirius/internal/sdk_s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type AmazonS3 struct {
	connectionManager *sdk_s3.S3ConnectionManager
}

type perAmazonS3DocResult struct {
	value  interface{}
	error  error
	status bool
	offset int64
}

type perAmazonS3FolderResult struct {
	value  interface{}
	error  error
	status bool
	offset int64
}

type amazonS3FileOperationResult struct {
	keyValues map[string]perAmazonS3DocResult
}

type amazonS3FolderOperationResult struct {
	key    string
	result perAmazonS3FolderResult
}

func NewAmazonS3ConnectionManager() *AmazonS3 {
	return &AmazonS3{
		connectionManager: sdk_s3.ConfigS3ConnectionManager(),
	}
}

// ========================================================================
//                 Operation Result for File Operations                  //
// ========================================================================

// newAmazonS3FileOperation creates and returns the amazonS3FileOperationResult
func newAmazonS3FileOperation() *amazonS3FileOperationResult {
	return &amazonS3FileOperationResult{
		keyValues: make(map[string]perAmazonS3DocResult),
	}
}

// AddResult is used to add a result for amazonS3FileOperationResult. If successful then err = nil and status = true,
// else status = false and err != nil
func (as3 *amazonS3FileOperationResult) AddResult(key string, value interface{}, err error, status bool, offset int64) {
	as3.keyValues[key] = perAmazonS3DocResult{
		value:  value,
		error:  err,
		status: status,
		offset: offset,
	}
}

func (as3 *amazonS3FileOperationResult) Value(key string) interface{} {
	if x, ok := as3.keyValues[key]; ok {
		return x.value
	}
	return nil
}

func (as3 *amazonS3FileOperationResult) GetStatus(key string) bool {
	if x, ok := as3.keyValues[key]; ok {
		return x.status
	}
	return false
}

func (as3 *amazonS3FileOperationResult) GetError(key string) error {
	if x, ok := as3.keyValues[key]; ok {
		return x.error
	}
	return errors.New("key not found in file operation")
}

func (as3 *amazonS3FileOperationResult) GetExtra(key string) map[string]any {
	if _, ok := as3.keyValues[key]; ok {
		return map[string]any{}
	}
	return nil
}

func (as3 *amazonS3FileOperationResult) GetOffset(key string) int64 {
	if x, ok := as3.keyValues[key]; ok {
		return x.offset
	}
	return -1
}

func (as3 *amazonS3FileOperationResult) failFileOperation(keyValue []KeyValue, err error) {
	for _, x := range keyValue {
		as3.keyValues[x.Key] = perAmazonS3DocResult{
			value:  x.Doc,
			error:  err,
			status: false,
		}
	}
}

func (as3 *amazonS3FileOperationResult) GetSize() int {
	return len(as3.keyValues)
}

// ========================================================================
//                Operation Result for Folder Operations                 //
// ========================================================================

func newAmazonS3FolderOperation(key string, value interface{}, err error, status bool, offset int64) *amazonS3FolderOperationResult {
	return &amazonS3FolderOperationResult{
		key: key,
		result: perAmazonS3FolderResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (as3 *amazonS3FolderOperationResult) Key() string {
	return as3.key
}

func (as3 *amazonS3FolderOperationResult) GetValue() interface{} {
	return as3.result.value
}

func (as3 *amazonS3FolderOperationResult) GetStatus() bool {
	return as3.result.status
}

func (as3 *amazonS3FolderOperationResult) GetError() error {
	return as3.result.error
}

func (as3 *amazonS3FolderOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (as3 *amazonS3FolderOperationResult) GetOffset() int64 {
	return as3.result.offset
}

func (as3 *amazonS3FolderOperationResult) AddResult(key string, value interface{}, err error, status bool, offset int64) {
	as3.key = key
	as3.result = perAmazonS3FolderResult{
		value:  value,
		error:  err,
		status: status,
		offset: offset,
	}
}

func (as3 *amazonS3FolderOperationResult) failFolderOperation(value interface{}, err error) {
	as3.result = perAmazonS3FolderResult{
		value:  value,
		error:  err,
		status: false,
	}
}

type Folder struct {
	NumFolders int               `json:"NumFolders"`
	NumFiles   int               `json:"NumFiles"`
	Files      map[string]File   `json:"Files"`
	Folders    map[string]Folder `json:"Folders"`
}

type File struct {
	Size int64 `json:"Size"`
}

func (as3 AmazonS3) Connect(extra ExternalStorageExtras) error {
	clusterConfig := &sdk_s3.S3ClusterConfig{
		AwsAccessKey:        extra.AwsAccessKey,
		AwsSecretKey:        extra.AwsSecretKey,
		AwsSessionToken:     extra.AwsSessionToken,
		AwsRegion:           extra.AwsRegion,
		S3ConnectionOptions: sdk_s3.S3ConnectionOptions{},
	}

	if _, err := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, clusterConfig); err != nil {
		return err
	}

	return nil
}

func (as3 AmazonS3) CreateBucket(keyValue KeyValue, extra ExternalStorageExtras) FolderOperationResult {

	result := newAmazonS3FolderOperation(keyValue.Key, keyValue.Doc, nil, false, 0)

	if err := validateStrings(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion); err != nil {
		result.failFolderOperation(keyValue, fmt.Errorf("create s3 bucket: aws auth parameters missing: %w", err))
		return result
	}

	if err := validateStrings(extra.Bucket); err != nil {
		result.failFolderOperation(keyValue.Doc, errors.New("create s3 bucket: bucket name is missing"))
		return result
	}

	s3Client, err := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, nil)
	if err != nil {
		result.failFolderOperation(keyValue.Doc, fmt.Errorf("create s3 bucket: get s3 client: %w", err))
		return result
	}

	bucketName := extra.Bucket

	// Adding bucket to S3
	_, errUploadToS3 := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: &bucketName,
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(extra.AwsRegion),
		},
	})
	if errUploadToS3 != nil {
		log.Println("create s3 bucket:", errUploadToS3)
		result.failFolderOperation(keyValue.Doc, fmt.Errorf("create s3 bucket: %w", errUploadToS3))
		return result
	}

	result.AddResult(keyValue.Key, nil, nil, true, keyValue.Offset)
	return result
}

func (as3 AmazonS3) DeleteBucket(keyValue KeyValue, extra ExternalStorageExtras) FolderOperationResult {

	result := newAmazonS3FolderOperation(keyValue.Key, keyValue.Doc, nil, false, 0)

	if err := validateStrings(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion); err != nil {
		result.failFolderOperation(keyValue, fmt.Errorf("delete s3 bucket: aws auth parameters missing: %w", err))
		return result
	}

	if err := validateStrings(extra.Bucket); err != nil {
		result.failFolderOperation(keyValue.Doc, errors.New("delete s3 bucket: bucket name is missing"))
		return result
	}

	s3Client, err := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, nil)
	if err != nil {
		result.failFolderOperation(keyValue.Doc, fmt.Errorf("delete s3 bucket: %w", err))
		return result
	}

	bucketName := extra.Bucket

	// List all objects in the bucket
	numOfOutputKeys := int32(1000)
	listObjectsOutput, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: &bucketName,
	})
	if err != nil {
		log.Println("delete s3 bucket: list objects in bucket:", err)
		result.failFolderOperation(keyValue.Doc, fmt.Errorf("delete s3 bucket: list objects in bucket: %w", err))
		return result
	}

	// Checking if all the objects in the bucket have been listed or not. By default, only a max. of 1000 objects are returned
	// If *listObjectsOutput.IsTruncated == true then it means there are more objects that are present.
	for *listObjectsOutput.IsTruncated {
		numOfOutputKeys = numOfOutputKeys * 10
		listObjectsOutput, err = s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:  &bucketName,
			MaxKeys: &numOfOutputKeys,
		})
		if err != nil {
			log.Println("delete s3 bucket: list objects in bucket:", err)
			result.failFolderOperation(keyValue.Doc, fmt.Errorf("delete s3 bucket: list objects in bucket: %w", err))
			return result
		}
	}

	// Iterate through objects and delete them
	for _, obj := range listObjectsOutput.Contents {
		_, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: &bucketName,
			Key:    obj.Key,
		})
		if err != nil {
			log.Printf("delete s3 bucket: unable to delete object %s: %v\n", *obj.Key, err)
			result.failFolderOperation(keyValue.Doc, fmt.Errorf("delete s3 bucket: delete object %s: %w\n", *obj.Key, err))
			return result
		}
	}

	// Deleting bucket from S3. A bucket can be deleted only after all objects are deleted and the bucket is empty,
	_, errDeleteBucketS3 := s3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: &bucketName,
	})
	if errDeleteBucketS3 != nil {
		log.Println("delete s3 bucket:", errDeleteBucketS3)
		result.failFolderOperation(keyValue.Doc, fmt.Errorf("delete s3 bucket: %w", errDeleteBucketS3))
		return result
	}

	result.AddResult(keyValue.Key, nil, nil, true, keyValue.Offset)
	return result
}

func (as3 AmazonS3) CreateFolder(keyValue KeyValue, extra ExternalStorageExtras) FolderOperationResult {

	result := newAmazonS3FolderOperation(keyValue.Key, keyValue.Doc, nil, false, 0)

	if err := validateStrings(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion); err != nil {
		result.failFolderOperation(keyValue, fmt.Errorf("create folder in s3 bucket: aws auth parameters missing: %w", err))
		return result
	}

	if err := validateStrings(extra.Bucket, extra.FolderPath); err != nil {
		result.failFolderOperation(keyValue.Doc, errors.New("create folder in s3 bucket: bucket name or folder path is missing"))
		return result
	}

	s3Client, err := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, nil)
	if err != nil {
		result.failFolderOperation(keyValue.Doc, fmt.Errorf("create folder in s3 bucket: %w", err))
		return result
	}

	bucketName := extra.Bucket
	objectKey := extra.FolderPath

	// Adding folder to S3
	_, errUploadToS3 := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &objectKey,
	})
	if errUploadToS3 != nil {
		log.Println("create folder in s3 bucket:", errUploadToS3)
		result.failFolderOperation(keyValue.Doc, fmt.Errorf("create folder in s3 bucket: %w", errUploadToS3))
		return result
	}

	result.AddResult(keyValue.Key, nil, nil, true, keyValue.Offset)
	return result
}

// DeleteFolder deletes a folder from s3 bucket. It first deletes all the files and sub folders inside the folder and then proceeds to delete the folder.
func (as3 AmazonS3) DeleteFolder(keyValue KeyValue, extra ExternalStorageExtras) FolderOperationResult {

	result := newAmazonS3FolderOperation(keyValue.Key, keyValue.Doc, nil, false, 0)

	if err := validateStrings(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion); err != nil {
		result.failFolderOperation(keyValue, fmt.Errorf("delete folder in s3 bucket: aws auth parameters missing: %w", err))
		return result
	}

	if err := validateStrings(extra.Bucket, extra.FolderPath); err != nil {
		result.failFolderOperation(keyValue.Doc, errors.New("delete folder in s3 bucket: bucket name or folder path is missing"))
		return result
	}

	s3Client, err := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, nil)
	if err != nil {
		result.failFolderOperation(keyValue.Doc, fmt.Errorf("delete folder in s3 bucket: %w", err))
		return result
	}

	bucketName := extra.Bucket
	folderPath := extra.FolderPath

	// List objects in the folder
	numOfOutputKeys := int32(1000)
	listObjectsOutput, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &folderPath,
	})
	if err != nil {
		log.Println("delete folder in s3 bucket: list folder object in S3:", err)
		result.failFolderOperation(keyValue.Doc, fmt.Errorf("delete folder in s3 bucket: list folder object in S3: %w", err))
		return result
	}

	// Checking if all the objects in the bucket have been listed or not. By default, only a max. of 1000 objects are returned
	// If *listObjectsOutput.IsTruncated == true then it means there are more objects that are present.
	for *listObjectsOutput.IsTruncated {
		numOfOutputKeys = numOfOutputKeys * 10
		listObjectsOutput, err = s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:  &bucketName,
			MaxKeys: &numOfOutputKeys,
		})
		if err != nil {
			log.Println("delete folder in s3 bucket: list objects in bucket:", err)
			result.failFolderOperation(keyValue.Doc, fmt.Errorf("delete folder in s3 bucket: list folder object in S3: %w", err))
			return result
		}
	}

	// Delete each object in the folder
	for _, obj := range listObjectsOutput.Contents {
		_, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: &bucketName,
			Key:    obj.Key,
		})
		if err != nil {
			log.Printf("delete folder in s3 bucket: delete object %s in folder: %v\n", *obj.Key, err)
			result.failFolderOperation(keyValue.Doc, fmt.Errorf("delete folder in s3 bucket: delete object %s in folder: %w", *obj.Key, err))
			return result
		}
	}

	// Delete the folder itself
	_, errDeleteFromS3 := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: &bucketName,
		Key:    &folderPath,
	})
	if errDeleteFromS3 != nil {
		log.Println("delete folder in s3 bucket:", errDeleteFromS3)
		result.failFolderOperation(keyValue.Doc, fmt.Errorf("delete folder in s3 bucket: %w", errDeleteFromS3))
		return result
	}

	result.AddResult(keyValue.Key, nil, nil, true, keyValue.Offset)
	return result
}

func (as3 AmazonS3) CreateFile(pathToFileOnDisk string, keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult {

	result := newAmazonS3FileOperation()

	if err := validateStrings(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion); err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("upload file to s3: aws auth parameters missing: %w", err))
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	if err := validateStrings(extra.Bucket, extra.FileFormat, extra.FilePath); err != nil {
		result.failFileOperation(keyValues, errors.New("upload file to s3: bucket name or file format or file path is missing"))
		return result
	}

	s3Client, err := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, nil)
	if err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("upload file to s3: %w", err))
		return result
	}

	bucketName := extra.Bucket
	objectKey := extra.FilePath

	// creating and uploading all the folders to S3 before inserting the file
	folderPath := filepath.Dir(objectKey)
	err = extractFolderAndUploadToS3(context.TODO(), s3Client, bucketName, folderPath)
	if err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("upload file to s3: %w", err))
		return result
	}

	// Sending File to S3 using S3 Manager Uploader

	filePathToUpload, err := os.Open(pathToFileOnDisk)
	if err != nil {
		log.Println("upload file to s3: open file on disk:", err)
		result.failFileOperation(keyValues, fmt.Errorf("upload file to s3: open file on disk: %w", err))
		return result
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("upload file to s3: close file on disk:", err)
			result.failFileOperation(keyValues, fmt.Errorf("upload file to s3: close file on disk: %w", err))
		}
	}(filePathToUpload)

	// Create an uploader instance
	s3Uploader := manager.NewUploader(s3Client)

	// Upload the file to S3
	_, err = s3Uploader.Upload(
		context.TODO(),
		&s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   filePathToUpload,
		},
	)
	if err != nil {
		log.Println("upload file to s3: upload file to S3:", err)
		result.failFileOperation(keyValues, fmt.Errorf("upload file to s3: %w", err))
		return result
	}

	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (as3 AmazonS3) UpdateFile(pathToFileOnDisk string, keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult {

	result := newAmazonS3FileOperation()

	if err := validateStrings(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion); err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("update file on s3: aws auth parameters missing: %w", err))
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	if err := validateStrings(extra.Bucket, extra.FileFormat, extra.FilePath); err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("update file on s3: bucket name or file format or file path is missing"))
		return result
	}

	s3Client, err := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, nil)
	if err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("update file on s3: %w", err))
		return result
	}

	bucketName := extra.Bucket
	objectKey := extra.FilePath

	folderPath := filepath.Dir(objectKey)
	err = extractFolderAndUploadToS3(context.TODO(), s3Client, bucketName, folderPath)
	if err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("update file on s3: %w", err))
		return result
	}

	// Sending File to S3 using S3 Manager Uploader
	// Open the file
	filePathToUpload, err := os.Open(pathToFileOnDisk)
	if err != nil {
		log.Println("update file on s3: open file on disk:", err)
		result.failFileOperation(keyValues, fmt.Errorf("update file on s3: open file on disk: %w", err))
		return result
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("update file on s3: close file on disk:", err)
			result.failFileOperation(keyValues, fmt.Errorf("update file on s3: close file on disk: %w", err))
		}
	}(filePathToUpload)

	// Create an uploader instance
	s3Uploader := manager.NewUploader(s3Client)

	// Upload the file to S3
	_, err = s3Uploader.Upload(
		context.TODO(),
		&s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   filePathToUpload,
		},
	)
	if err != nil {
		log.Println("update file on s3: upload file to S3:", err)
		result.failFileOperation(keyValues, fmt.Errorf("update file on s3: upload file to S3:: %w", err))
		return result
	}

	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (as3 AmazonS3) DeleteFile(keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult {

	result := newAmazonS3FileOperation()

	if err := validateStrings(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion); err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("delete file from s3: aws auth parameters missing: %w", err))
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	if err := validateStrings(extra.Bucket, extra.FilePath); err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("delete file from s3: bucket name or file format or file path is missing"))
		return result
	}

	s3Client, err := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, nil)
	if err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("delete file from s3: %w", err))
		return result
	}

	bucketName := extra.Bucket
	objectKey := extra.FilePath

	// Deleting the file from S3
	_, errDeleteS3Object := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: &bucketName,
		Key:    &objectKey,
	})
	if errDeleteS3Object != nil {
		log.Println("delete file from s3:", errDeleteS3Object)
		result.failFileOperation(keyValues, fmt.Errorf("delete file from s3: %w", errDeleteS3Object))
		return result
	}

	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (as3 AmazonS3) CreateFilesInFolder(keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult {
	// This operation is handled in blob_loading.go and using CreateFile()
	return newAmazonS3FileOperation()
}

func (as3 AmazonS3) UpdateFilesInFolder(keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult {
	// This operation is handled in blob_loading.go and using CreateFile()
	return newAmazonS3FileOperation()
}

// DeleteFilesInFolder deletes files in a specified folder.
/*
 * The number of files to be deleted is to be specified in ExternalStorageExtras.NumFiles
 * If ExternalStorageExtras.NumFiles = 0, then it will delete all the files in the folder
 * The file formats specified in ExternalStorageExtras.FileFormat will be deleted.
 * If the ExternalStorageExtras.FileFormat is not provided then files of all supported formats will be deleted.
 */
func (as3 AmazonS3) DeleteFilesInFolder(keyValues []KeyValue, extra ExternalStorageExtras) FileOperationResult {

	result := newAmazonS3FileOperation()

	if err := validateStrings(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion); err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("delete files in a folder from s3: aws auth parameters missing: %w", err))
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	if err := validateStrings(extra.Bucket); err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("delete files in a folder from s3: bucket name"))
		return result
	}

	s3Client, err := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, nil)
	if err != nil {
		result.failFileOperation(keyValues, fmt.Errorf("delete files in a folder from s3: %w", err))
		return result
	}

	bucketName := extra.Bucket
	objectKey := extra.FolderPath
	fileFormat := extra.FileFormat

	// Validating file format
	if checkFileFormat := ValidateFileFormat(fileFormat); !checkFileFormat {
		result.failFileOperation(keyValues, errors.New("delete files in a folder from s3: file format provided is invalid or is not supported"))
		return result
	}

	fileFormatsArray := strings.Split(fileFormat, ",")
	fileFormatsMap := make(map[string]string) // stores all the file extensions which are to be deleted
	if fileFormat == "" {
		for key := range GetSupportedFileFormats() {
			keyExt := "." + key
			fileFormatsMap[keyExt] = key
		}
		log.Println("delete files in a folder from s3: file format not provided. All file formats will be considered for deletion")
	} else {
		for _, format := range fileFormatsArray {
			format = strings.TrimSpace(format)
			formatExt := "." + format // "json" -> ".json"
			fileFormatsMap[formatExt] = format
		}
	}

	// List objects in the folder
	numOfOutputKeys := int32(1000)
	listObjectsOutput, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &objectKey,
	})
	if err != nil {
		log.Println("delete files in a folder from s3: list objects of folder in s3:", err)
		result.failFileOperation(keyValues, fmt.Errorf("delete files in a folder from s3: list objects of folder in s3: %w", err))
		return result
	}

	// Checking if all the objects in the bucket have been listed or not. By default, only a max. of 1000 objects are returned
	// If *listObjectsOutput.IsTruncated == true then it means there are more objects that are present.
	for *listObjectsOutput.IsTruncated {
		numOfOutputKeys = numOfOutputKeys * 10
		listObjectsOutput, err = s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:  &bucketName,
			MaxKeys: &numOfOutputKeys,
		})
		if err != nil {
			log.Println("delete files in a folder from s3: list objects in bucket:", err)
			result.failFileOperation(keyValues, fmt.Errorf("delete files in a folder from s3: list objects of folder in s3: %w", err))
			return result
		}
	}

	// Defining the number of files to be deleted.
	if extra.FilesPerFolder == 0 {
		extra.FilesPerFolder = int64(len(listObjectsOutput.Contents))
	}
	numFilesToBeDeleted := extra.FilesPerFolder

	// When we get the list of objects from S3, it has all the files of the prefix folder as well as all the files contained
	// in all the sub folders that might be present. Hence, we need to filter the results.
	var actualFilesToBeDeleted []string
	folderLevel := strings.Count(objectKey, "/")
	for _, obj := range listObjectsOutput.Contents {
		if strings.Count(*obj.Key, "/") != folderLevel {
			// Ignoring all the sub folders and their files
			continue
		}
		// Only the files present in the specified FolderPath will be deleted
		actualFilesToBeDeleted = append(actualFilesToBeDeleted, *obj.Key)
	}

	// Delete the number of objects as specified in numFiles and of the file formats as specified in fileFormat
	for _, filePath := range actualFilesToBeDeleted {
		// We extract the file extension and then check if it has been marked to be deleted
		fileExtension := filepath.Ext(filePath)

		if _, ok := fileFormatsMap[fileExtension]; ok {
			_, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: &bucketName,
				Key:    aws.String(filePath),
			})
			if err != nil {
				log.Printf("delete files in a folder from s3: delete object %s: %v\n", filePath, err)
				result.AddResult(filePath, nil, fmt.Errorf("delete files in a folder from s3: delete object %s: %w", filePath, err), false, 0)
			}
			numFilesToBeDeleted--
			if numFilesToBeDeleted == 0 {
				break
			}
		}
		if numFilesToBeDeleted == 0 {
			break
		}
	}

	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

// GetInfo returns the directory structure of a bucket in S3. Example:
/*
{
	"NumFolders": 1,
	"NumFiles": 2,
	"Files": {
		"file_1.csv": {
			"Size": 53723
		},
		"file_1.json": {
			"Size": 150900
		}
	},
	"Folders": {
		"folder_level_0_4jSdF7YT": {
			"NumFolders": 0,
			"NumFiles": 1,
			"Files": {
				"file_1.csv": {
					"Size": 54906
				}
			},
			"Folders": {}
		}
	}
}
*/
func (as3 AmazonS3) GetInfo(extra ExternalStorageExtras) (interface{}, error) {

	if err := validateStrings(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion); err != nil {
		return nil, fmt.Errorf("get s3 bucket directory structure: aws auth parameters missing: %w", err)
	}

	if err := validateStrings(extra.Bucket); err != nil {
		return nil, errors.New("get s3 bucket directory structure: bucket name is missing")
	}

	s3Client, err := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, nil)
	if err != nil {
		return nil, fmt.Errorf("get s3 bucket directory structure: %w", err)
	}

	bucketName := extra.Bucket
	rootFolder := Folder{}
	prefix := ""
	var numOfOutputKeys = int32(1000)

	listObjectsOutput, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &prefix,
	})
	if err != nil {
		log.Println("get s3 bucket directory structure: list objects in bucket:", err.Error())
		return nil, fmt.Errorf("get s3 bucket directory structure: list objects in bucket: %w", err)
	}

	// We check if the output of list objects is truncated, then we increase the size of ListObjectsV2Input.MaxKeys
	// determines the maximum number of keys returned.
	for *listObjectsOutput.IsTruncated {
		numOfOutputKeys = numOfOutputKeys * 10
		listObjectsOutput, err = s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:  &bucketName,
			Prefix:  &prefix,
			MaxKeys: &numOfOutputKeys,
		})
		if err != nil {
			log.Println("get s3 bucket directory structure: list objects in bucket:", err.Error())
			return nil, fmt.Errorf("get s3 bucket directory structure: list objects in bucket: %w", err)
		}
	}

	rootFolder.Files = make(map[string]File)
	rootFolder.Folders = make(map[string]Folder)
	lvl := 1

	for _, obj := range listObjectsOutput.Contents {
		if *obj.Key == prefix || len(*obj.Key) == 0 {
			// Skip if the object is the folder itself, or if it is empty
			continue
		}
		tempString := *obj.Key
		if tempString[len(tempString)-1] == '/' && strings.Count(tempString, "/") == lvl {
			// For a sub folder
			subFolder := Folder{}
			traverseFolder(context.TODO(), s3Client, bucketName, *obj.Key, &subFolder, lvl+1)
			rootFolder.Folders[tempString[len(prefix):len(tempString)-1]] = subFolder
			rootFolder.NumFolders++

		} else if tempString[len(tempString)-1] != '/' && strings.Count(tempString, "/") == lvl-1 {
			// For file
			rootFolder.Files[tempString[len(prefix):]] = File{Size: *obj.Size}
			rootFolder.NumFiles++
		}
	}

	return rootFolder, nil
}

// Warmup is used to check if we are able to connect to S3 cluster by listing the buckets in s3 cluster
func (as3 AmazonS3) Warmup(extra ExternalStorageExtras) error {
	s3Client, errClient := as3.connectionManager.GetS3Cluster(extra.AwsAccessKey, extra.AwsSecretKey, extra.AwsSessionToken, extra.AwsRegion, nil)
	if errClient != nil {
		log.Println("s3 warm up:", errClient)
		return fmt.Errorf("s3 warm up: %w", errClient)
	}

	// Checking if the cluster is reachable or not, by listing the buckets
	_, err := s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		fmt.Println("s3 warm up: list buckets:", err)
		return fmt.Errorf("s3 warm up: list buckets: %w", err)
	}
	return nil
}

// Close closes the Amazon S3 Client connection.
// For Amazon S3, the clusterIdentifier is the AwsAccessKey
func (as3 AmazonS3) Close(clusterIdentifier string) error {
	delete(as3.connectionManager.Clusters, clusterIdentifier)
	return nil
}
