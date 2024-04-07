package external_storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/AryaanB9/sirius_aryaan/internal/err_sirius"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type KeyValue struct {
	Key    string
	Doc    interface{}
	Offset int64
}

type ExternalStorageExtras struct {
	AwsAccessKey     string            `json:"awsAccessKey,omitempty" doc:"true"`
	AwsSecretKey     string            `json:"awsSecretKey,omitempty" doc:"true"`
	AwsSessionToken  string            `json:"awsSessionToken,omitempty" doc:"true"`
	AwsRegion        string            `json:"awsRegion,omitempty" doc:"true"`
	Bucket           string            `json:"bucket,omitempty" doc:"true"`
	FilePath         string            `json:"filePath,omitempty" doc:"true"`
	FolderPath       string            `json:"folderPath,omitempty" doc:"true"`
	FolderLevelNames map[string]string `json:"folderLevelNames,omitempty" doc:"true"`
	FileFormat       string            `json:"fileFormat,omitempty" doc:"true"`
	MinFileSize      int64             `json:"minFileSize,omitempty" doc:"true"`
	MaxFileSize      int64             `json:"maxFileSize,omitempty" doc:"true"`
	NumFolders       int64             `json:"numFolders,omitempty" doc:"true"`
	MaxFolderDepth   int64             `json:"maxFolderDepth,omitempty" doc:"true"`
	FoldersPerDepth  int64             `json:"foldersPerDepth,omitempty" doc:"true"`
	FilesPerFolder   int64             `json:"filesPerFolder,omitempty" doc:"true"`
	NumFiles         int64             `json:"numFiles,omitempty" doc:"true"`
}

func validateStrings(values ...string) error {
	for _, v := range values {
		if v == "" {
			return fmt.Errorf("%s %w", v, err_sirius.InvalidInfo)
		}
	}
	return nil
}

// extractFolderAndUploadToS3 extracts all the folder paths from file path and adding all the folder objects to S3 before inserting file to S3.
func extractFolderAndUploadToS3(ctx context.Context, s3Client *s3.Client, bucketName, folderPath string) error {

	for folderPath != "/" && folderPath != "." {
		folderObjKey := folderPath + "/"
		_, errUploadToS3 := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucketName,
			Key:    &folderObjKey,
		})
		if errUploadToS3 != nil {
			log.Println("creating folder: unable to upload folder to S3:", errUploadToS3)
			return errors.New("creating folder: unable to upload folder to S3: " + errUploadToS3.Error())
		}
		folderPath = filepath.Dir(folderPath)
	}
	return nil
}

// traverseFolder traverses through the whole bucket in S3 and returns the directory structure
/*
 *	The bucket must have empty objects for the folders. E.g. an object having path : "folder1/folder2/"
 *	Returns the size of the files in bytes
 */
func traverseFolder(ctx context.Context, s3Client *s3.Client, bucketName, prefix string, folder *Folder, lvl int) {

	numOfOutputKeys := int32(1000)
	listObjectsOutput, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &prefix,
	})
	if err != nil {
		log.Println("traversing a folder in bucket: unable to list objects in folder:", err)
		return
	}

	// We check if the output of list objects is truncated, then we increase the size of ListObjectsV2Input.MaxKeys
	// determines the maximum number of keys returned.
	for *listObjectsOutput.IsTruncated {
		numOfOutputKeys = numOfOutputKeys * 10
		listObjectsOutput, err = s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  &bucketName,
			Prefix:  &prefix,
			MaxKeys: &numOfOutputKeys,
		})
		if err != nil {
			log.Println("traversing a folder in bucket: unable to list objects in folder:", err)
			return
		}
	}

	folder.Files = make(map[string]File)
	folder.Folders = make(map[string]Folder)

	for _, obj := range listObjectsOutput.Contents {
		if *obj.Key == prefix || len(*obj.Key) == 0 {
			// Skip if the object is the folder itself, or if it is empty
			continue
		}
		tempString := *obj.Key
		if tempString[len(tempString)-1] == '/' && strings.Count(tempString, "/") == lvl {
			// For a sub folder
			subFolder := Folder{}
			traverseFolder(ctx, s3Client, bucketName, *obj.Key, &subFolder, lvl+1)

			folder.Folders[tempString[len(prefix):len(tempString)-1]] = subFolder
			folder.NumFolders++
		} else if tempString[len(tempString)-1] != '/' && strings.Count(tempString, "/") == lvl-1 {
			// For file
			folder.Files[tempString[len(prefix):]] = File{Size: *obj.Size}
			folder.NumFiles++
		}
	}
}

// GetSupportedFileFormats returns the list of supported file formats for external storage file operations
func GetSupportedFileFormats() map[string]string {
	return map[string]string{
		"json":    ".json",
		"csv":     ".csv",
		"tsv":     ".tsv",
		"avro":    ".avro",
		"parquet": ".parquet",
	}
}

// ValidateFileFormat validates whether the provided file format is supported or not.
func ValidateFileFormat(fileFormat string) bool {
	if fileFormat == "" {
		return false
	}

	checkFileFormat := false
	fileFormats := strings.Split(fileFormat, ",") // file format can be like "json" or "json, avro" or "json,parquet"
	supportedFileFormats := GetSupportedFileFormats()

	// Checking if the file format is supported
	for _, format := range fileFormats {
		format = strings.TrimSpace(format)
		if format == "" {
			continue
		}
		if _, ok := supportedFileFormats[format]; ok {
			checkFileFormat = true
		} else {
			checkFileFormat = false
			break
		}
	}

	return checkFileFormat
}
