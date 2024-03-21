package external_storage

import (
	"context"
	"fmt"
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

func my(filePaths []string, root *Folder) Folder {

	//currFolder := *root
	//tempFolder := *root
	//for _, filePath := range filePaths {
	//	dir, file := filepath.Split(filePath)
	//	if file == "" {
	//		// It means a new folder has come up
	//		currFolder.NumFolders++
	//		currFolder.Folders[dir] = Folder{}
	//		tempFolder = currFolder
	//		currFolder = currFolder.Folders[dir]
	//	} else {
	//		currFolder.NumFiles++
	//		currFolder.Files[file] = File{Size: 1}
	//	}
	//}
	return Folder{}
}

func buildDirectoryStructure(filePaths []string) map[string]Folder {
	root := make(map[string]Folder)
	//root := Folder{}
	for _, filePath := range filePaths {
		pathParts := strings.Split(filePath, "/")
		current := root

		for i, part := range pathParts {
			if part == "" {
				continue
			}

			if _, ok := current[part]; !ok {
				current[part] = Folder{
					Files:   make(map[string]File),
					Folders: make(map[string]Folder),
				}
			}

			if i == len(pathParts)-1 {
				current[part] = Folder{
					NumFolders: current[part].NumFolders,
					NumFiles:   current[part].NumFiles + 1,
					Files:      current[part].Files,
					Folders:    current[part].Folders,
				}
			} else {
				current[part] = Folder{
					NumFolders: current[part].NumFolders + 1,
					NumFiles:   current[part].NumFiles,
					Files:      current[part].Files,
					Folders:    current[part].Folders,
				}
			}

			current = current[part].Folders
		}
	}

	return root
}

func listObjectsRecursive(ctx context.Context, client *s3.Client, folder *Folder, bucketName, prefix string) (Folder, error) {
	listObjectsOutput, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &prefix,
	})
	if err != nil {
		return Folder{}, err
	}

	//log.Println("len:", len(listObjectsOutput.Contents))

	maxLevel := 0
	for _, obj := range listObjectsOutput.Contents {
		if obj.Key == nil {
			continue
		}
		key := *obj.Key
		keyArr := strings.Split(key, "/")
		var newArr []string
		for _, x := range keyArr {
			if x != "" {
				newArr = append(newArr, x)
			}
		}

		if maxLevel < len(newArr) {
			maxLevel = len(newArr)
		}
	}

	folder.Files = make(map[string]File)
	folder.Folders = make(map[string]Folder)
	currFolder := folder
	mainFolder := currFolder

	for lvl := 1; lvl <= maxLevel; lvl++ {
		subFolder := Folder{}
		for _, obj := range listObjectsOutput.Contents {
			if obj.Key == nil {
				continue
			}

			key := *obj.Key
			keyArr := strings.Split(key, "/")
			var newArr []string
			for _, x := range keyArr {
				if x != "" {
					newArr = append(newArr, x)
				}
			}

			if len(newArr) == lvl {
				if *obj.Size != 0 {
					subFolder.Files[key] = File{Size: *obj.Size}
					subFolder.NumFiles++
				} else {
					anotherSubFolder := Folder{}
					subFolder.Folders[key] = anotherSubFolder
					subFolder.NumFolders++
				}
			}
		}
		//currFolder.Folders = subFolder
		currFolder.Folders = make(map[string]Folder)
		currFolder.Files = make(map[string]File)
	}

	return *mainFolder, nil
}
