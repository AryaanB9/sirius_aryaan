package blob_loading

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/AryaanB9/sirius_aryaan/internal/docgenerator"
	"github.com/AryaanB9/sirius_aryaan/internal/err_sirius"
	"github.com/AryaanB9/sirius_aryaan/internal/external_storage"
	"github.com/AryaanB9/sirius_aryaan/internal/task_result"
	"github.com/AryaanB9/sirius_aryaan/internal/task_state"
	"github.com/AryaanB9/sirius_aryaan/internal/tasks"

	"github.com/shettyh/threadpool"
)

const (
	TempFolderPath = "temp/"
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
 * Each file is randomly assigned a file format from the given file formats
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
