package tasks

import (
	"log"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
)

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
	SingleInsertOperation         string = "singleInsert"
	SingleDeleteOperation         string = "singleDelete"
	SingleUpsertOperation         string = "singleUpsert"
	SingleReadOperation           string = "singleRead"
	SingleTouchOperation          string = "singleTouch"
	SingleReplaceOperation        string = "singleReplace"
	CreatePrimaryIndex            string = "createPrimaryIndex"
	CreateIndex                   string = "createIndex"
	BuildIndex                    string = "buildIndex"
	RetryExceptionOperation       string = "retryException"
	SubDocInsertOperation         string = "subDocInsert"
	SubDocDeleteOperation         string = "subDocDelete"
	SubDocUpsertOperation         string = "subDocUpsert"
	SubDocReadOperation           string = "subDocRead"
	SubDocReplaceOperation        string = "subDocReplace"
	SingleSubDocInsertOperation   string = "singleSubDocInsert"
	SingleSubDocUpsertOperation   string = "singleSubDocUpsert"
	SingleSubDocReplaceOperation  string = "singleSubDocReplace"
	SingleSubDocDeleteOperation   string = "singleSubDocDelete"
	SingleSubDocReadOperation     string = "singleSubDocRead"
	SingleDocValidateOperation    string = "SingleDocValidate"
	BucketWarmUpOperation         string = "BucketWarmUp"
	CreateDBOperation             string = "createDatabase"
	DeleteDBOperation             string = "deleteCollection"
	ListDBOperation               string = "createDatabase"
	CountOperation                string = "countDocuments"
	S3BucketCreateOperation       string = "createS3Bucket"
	S3BucketDeleteOperation       string = "deleteS3Bucket"
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

func buildKeyAndValues(doc map[string]any, result map[string]any, startString string) {
	for key, value := range doc {
		if subDoc, ok := value.(map[string]any); ok {
			buildKeyAndValues(subDoc, result, key+".")
		} else {
			result[startString+key] = value
		}
	}
}

func CompareDocumentsIsSame(host map[string]any, document1 map[string]any, document2 map[string]any) bool {

	hostMap := make(map[string]any)
	buildKeyAndValues(host, hostMap, "")

	document1Map := make(map[string]any)
	buildKeyAndValues(document1, document1Map, "")

	document2Map := make(map[string]any)
	buildKeyAndValues(document2, document2Map, "")

	for key, value := range hostMap {
		if v1, ok := document1Map[key]; ok {
			if !reflect.DeepEqual(value, v1) {
				return false
			}
		} else if v2, ok := document2Map[key]; ok {
			if !reflect.DeepEqual(v2, value) {
				return false
			}
		} else {
			// TODO  fix_the_validation_of_missing_Keys
			continue
		}
	}

	return true
}

//=================================================================================
//                   Helper Functions for External Storage                       //
//=================================================================================

// generateRandomString is used to generate a random string of provided length.
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seed := rand.NewSource(time.Now().UnixNano())
	random := rand.New(seed)

	result := make([]byte, length)
	for i := range result {
		result[i] = charset[random.Intn(len(charset))]
	}
	return string(result)
}

// getFolderNameFromDictionary returns a folder name for the given level and fill the required pattern
/*
* folderLvlNames maps levels with folder names such as "level_1":"folder_name_{string}"
* The folder name has a pattern to be filled such as {string} or {int}
* Replacing the pattern in `folder_name_{string}` with a random string of length `randStringLength`
* Replacing the pattern in `folder_name_{string}` with a random int in range [0, randInteger]
 */
func getFolderNameFromDictionary(folderLvlNames map[string]string, folderLevel, randStringLength int, randInteger int64) string {

	folderName := folderLvlNames["level_"+strconv.Itoa(folderLevel)]

	if strings.Contains(folderName, "{string}") {
		folderName = strings.Replace(folderName, "{string}", generateRandomString(randStringLength), 1)
	} else if strings.Contains(folderName, "{int}") {
		folderName = strings.Replace(folderName, "{int}", strconv.FormatInt(rand.Int63n(randInteger), 10), 1)
	}

	return folderName
}

// GenerateFolderPaths generates the required number of folders in which files will be inserted.
/*
 * Total Number of Folders generated = numFolders * (random(maxFolderDepth)-1) * foldersPerDepth + 1 // For root folder
 * Returns a slice containing folder paths
 * If folderLevelNames is not provided then a fixed type of naming is used : folder_level_{lvl}_{random-string}
 */
func GenerateFolderPaths(numFolders, maxFolderDepth, foldersPerDepth int64, folderLevelNames map[string]string) []string {
	rootPath := ""
	var folderPaths []string

	folderPaths = append(folderPaths, rootPath)

	if numFolders <= 0 || maxFolderDepth <= 0 {
		return folderPaths
	}

	if folderLevelNames != nil {
		for i := int64(0); i < numFolders; i++ {

			//Randomizing the Depths for different folders.
			depth := rand.Int63n(maxFolderDepth + 1)
			if depth == 0 {
				depth = 1
			}
			//depth := maxFolderDepth

			lvl := 1
			currentPath := rootPath
			for j := int64(0); j < depth-1; j++ {

				folderName1 := getFolderNameFromDictionary(folderLevelNames, lvl, 8, int64(math.Pow(2, 32)))
				path1 := currentPath + folderName1 + "/"
				folderPaths = append(folderPaths, path1)

				for k := int64(0); k < foldersPerDepth-1; k++ {

					folderName2 := getFolderNameFromDictionary(folderLevelNames, lvl+1, 8, int64(math.Pow(2, 32)))
					path2 := path1 + folderName2 + "/"
					folderPaths = append(folderPaths, path2)
				}
				lvl++
				currentPath = path1

				if j+1 == depth-1 {
					folderName2 := getFolderNameFromDictionary(folderLevelNames, lvl, 8, int64(math.Pow(2, 32)))
					path2 := path1 + folderName2 + "/"
					folderPaths = append(folderPaths, path2)
				}
			}
		}
	} else {
		for i := int64(0); i < numFolders; i++ {
			// Randomizing the Depths for different folders.
			depth := rand.Int63n(maxFolderDepth + 1)
			if depth == 0 {
				depth = 1
			}
			//depth := maxFolderDepth
			path := rootPath
			lvl := 0
			for j := int64(0); j < depth-1; j++ {
				path1 := path + "folder_level_" + strconv.Itoa(int(lvl)) + "_" + generateRandomString(8) + "/"
				folderPaths = append(folderPaths, path1)
				for k := int64(0); k < foldersPerDepth-1; k++ {
					path2 := path1 + "folder_level_" + strconv.Itoa(int(lvl)+1) + "_" + generateRandomString(8) + "/"
					folderPaths = append(folderPaths, path2)
				}
				path = path1
				lvl++
			}
		}
	}
	log.Println("Total Folders = ", len(folderPaths))
	return folderPaths
}

// GenerateFilePaths generates the files names with file formats as specified by user and returns the file paths.
/*
 * The number of files to be generated = len(folderPaths) * filesPerFolder
 * Returns a slice containing the full path of file(s)
 * fileFormats is a string containing different file format(s) separated by commas.
	For e.g. "json" or "json, csv" or "json,parquet,csv" or "tsv,csv, avro"
*/
func GenerateFilePaths(folderPaths []string, filesPerFolder int64, fileFormats string) []string {
	var filePaths []string

	// Getting the file formats from the string into a slice. "json, csv,parquet" into ["json", "csv", "parquet"]
	fileFormatsArray := strings.Split(fileFormats, ",")
	for i := range fileFormatsArray {
		fileFormatsArray[i] = strings.TrimSpace(fileFormatsArray[i])
	}
	numFileFormats := len(fileFormatsArray)

	for _, folder := range folderPaths {
		for i := 1; i <= int(filesPerFolder); i++ {
			fileFormatIndex := rand.Intn(numFileFormats)
			filePath := folder + "file_" + strconv.Itoa(i) + "." + fileFormatsArray[fileFormatIndex]
			filePaths = append(filePaths, filePath)
		}
	}
	log.Println("Total Files =", len(filePaths))
	return filePaths
}
