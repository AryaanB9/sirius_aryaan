package external_storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/AryaanB9/sirius_aryaan/internal/docgenerator"
	"github.com/AryaanB9/sirius_aryaan/internal/meta_data"
	"github.com/AryaanB9/sirius_aryaan/internal/template"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dnlo/struct2csv"
	"github.com/linkedin/goavro/v2"
	"github.com/parquet-go/parquet-go"
	"log"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/bgadrian/fastfaker/faker"
)

func getFile(fileFormat, templateName string, docsArray []interface{}) ([]byte, error) {
	var fileToUpload []byte
	var errDocToFileFormat error

	switch strings.ToLower(fileFormat) {
	case "json":
		fileToUpload, errDocToFileFormat = json.MarshalIndent(docsArray, "", "  ")
		if errDocToFileFormat != nil {
			log.Println("In operations.go insertFiles(), error marshaling JSON:", errDocToFileFormat)
		}

	case "avro":
		var bufferAvroFile bytes.Buffer

		// Parsing the AVRO Schema
		avroSchema, err := template.GetAvroSchema(templateName)
		if err != nil {
			log.Println("In operations.go insertFiles(), error getting avro schema:", err)
		}
		// Getting the codec which is used in conversion between binary and struct (in form of [string]interface{})
		codec, err := goavro.NewCodec(avroSchema)
		if err != nil {
			log.Println("In operations.go insertFiles(), error parsing avro schema:", err)
		}

		// Converting the documents into avro binary
		for _, x := range docsArray {
			avroDoc, err := codec.BinaryFromNative(nil, template.StructToMap(x))
			if err != nil {
				log.Println("In operations.go insertFiles(), error converting into avro:", err)
			}
			_, err = bufferAvroFile.Write(avroDoc)
			if err != nil {
				log.Println("In operations.go insertFiles(), writing avro data to buffer failed:", err)
			}
		}

		fileToUpload = bufferAvroFile.Bytes()
		bufferAvroFile.Reset()

		// Converting Binary to Struct. For one doc
		//docFromAvro, _, _ := codec.NativeFromBinary(fileToUpload)
		//log.Println(template.StringMapToHotel(docFromAvro.(map[string]interface{})))

	case "csv":
		bufferCsvFile := &bytes.Buffer{}
		csvFileWriter := struct2csv.NewWriter(bufferCsvFile)

		switch templateName {
		case "hotel":
			err := csvFileWriter.WriteColNames(*docsArray[0].(*template.Hotel))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into csv:", err)
			}
			for _, x := range docsArray {
				err := csvFileWriter.WriteStruct(*x.(*template.Hotel))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into csv:", err)
				}
			}
		case "person":
			err := csvFileWriter.WriteColNames(*docsArray[0].(*template.Person))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into csv:", err)
			}
			for _, x := range docsArray {
				err := csvFileWriter.WriteStruct(*x.(*template.Person))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into csv:", err)
				}
			}
		case "product":
			err := csvFileWriter.WriteColNames(*docsArray[0].(*template.Product))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into csv:", err)
			}
			for _, x := range docsArray {
				err := csvFileWriter.WriteStruct(*x.(*template.Product))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into csv:", err)
				}
			}
		default:
			panic("invalid template name")
		}
		csvFileWriter.Flush()
		fileToUpload = bufferCsvFile.Bytes()
		bufferCsvFile.Reset()

	case "tsv":
		bufferTsvFile := &bytes.Buffer{}
		tsvFileWriter := struct2csv.NewWriter(bufferTsvFile)
		var tabDelimiter rune
		tabDelimiter = '\t'
		tsvFileWriter.SetComma(tabDelimiter)

		switch templateName {
		case "hotel":
			err := tsvFileWriter.WriteColNames(*docsArray[0].(*template.Hotel))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into tsv:", err)
			}
			for _, x := range docsArray {
				err := tsvFileWriter.WriteStruct(*x.(*template.Hotel))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into tsv:", err)
				}
			}
		case "person":
			err := tsvFileWriter.WriteColNames(*docsArray[0].(*template.Person))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into tsv:", err)
			}
			for _, x := range docsArray {
				err := tsvFileWriter.WriteStruct(*x.(*template.Person))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into tsv:", err)
				}
			}
		case "product":
			err := tsvFileWriter.WriteColNames(*docsArray[0].(*template.Product))
			if err != nil {
				log.Println("In operations.go insertFiles(), error writing headers into tsv:", err)
			}
			for _, x := range docsArray {
				err := tsvFileWriter.WriteStruct(*x.(*template.Product))
				if err != nil {
					log.Println("In operations.go insertFiles(), error converting into tsv:", err)
				}
			}
		default:
			panic("invalid template name or template name not supported")
		}
		tsvFileWriter.Flush()
		fileToUpload = bufferTsvFile.Bytes()
		bufferTsvFile.Reset()

	case "parquet":
		bufferParquetFile := new(bytes.Buffer)
		var writer *parquet.GenericWriter[interface{}]

		// Defining the parquet schema and then creating a parquet writer instance with buffer as output
		if strings.ToLower(templateName) == "hotel" {
			schema := parquet.SchemaOf(new(template.Hotel))
			writer = parquet.NewGenericWriter[interface{}](bufferParquetFile, schema)

		} else if strings.ToLower(templateName) == "person" {
			schema := parquet.SchemaOf(new(template.Person))
			writer = parquet.NewGenericWriter[interface{}](bufferParquetFile, schema)

		} else if strings.ToLower(templateName) == "product" {
			schema := parquet.SchemaOf(new(template.Product))
			writer = parquet.NewGenericWriter[interface{}](bufferParquetFile, schema)
		}

		// Writing the array of documents to parquet writer
		_, err := writer.Write(docsArray)
		if err != nil {
			log.Println("In operations.go insertFiles(), error writing document into parquet:", err)
		}

		// Closing the writer to flush buffers and write the file footer.
		if err := writer.Close(); err != nil {
			log.Println("In operations.go insertFiles(), error flushing Parquet writer:", err)
		}

		fileToUpload = bufferParquetFile.Bytes()
		bufferParquetFile.Reset()

	default:
		return nil, errors.New("invalid file format or file format not supported")
	}

	return fileToUpload, nil
}

func TestAmazonS3(t *testing.T) {
	/*
		This test does the following
		1. Create a Bucket
		2. Create a Folder
		3. Insert Files into Folder
		4. Check if all files are inserted
		5. Delete Files in Folder
		6. Delete Folder
		7. Delete Bucket
	*/

	extStorage, err := ConfigExternalStorage("awsS3")
	if err != nil {
		t.Fatal(err)
	}

	awsAccessKey, ok := os.LookupEnv("sirius_s3_access_key")
	if !ok {
		t.Error("aws s3 access key not found")
		t.FailNow()
	}
	awsSecretKey, ok := os.LookupEnv("sirius_s3_secret_key")
	if !ok {
		t.Error("aws s3 secret key not found")
		t.FailNow()
	}
	awsRegion, ok := os.LookupEnv("sirius_s3_region")
	if !ok {
		t.Error("aws s3 region not found")
		t.FailNow()
	}

	extra := ExternalStorageExtras{
		AwsAccessKey:    awsAccessKey,
		AwsSecretKey:    awsSecretKey,
		AwsSessionToken: "empty",
		AwsRegion:       awsRegion,
	}

	if err := extStorage.Connect(extra); err != nil {
		t.Error("connecting to s3:", err)
		t.FailNow()
	}

	// Creating S3 Bucket
	extra.Bucket = "sirius-test-s3"
	result := extStorage.CreateBucket(KeyValue{}, extra)
	if result.GetError() != nil {
		t.Error("creating s3 bucket:", err)
		t.FailNow()
	}

	fileFormats := []string{"json", "csv", "tsv", "avro", "parquet"}

	// Creating Folders. For every file format, e.g. "jsonFolder/", "avroFolder/", and so on
	tempExtra := extra
	folderPaths := []string{}
	for _, fileFormat := range fileFormats {
		tempExtra.FolderPath = fileFormat + "Folder/"
		folderPaths = append(folderPaths, tempExtra.FolderPath)
		extStorage.CreateFolder(KeyValue{}, tempExtra)
	}

	m := meta_data.NewMetaData()
	cm1 := m.GetCollectionMetadata("x")

	gen := &docgenerator.Generator{
		KeySize:  0,
		DocType:  "json",
		Template: template.InitialiseTemplate("hotel"),
	}

	// Inserting Files into every folder
	for i, folder := range folderPaths {
		// Creating files
		var docsArray []interface{}
		var keyValues []KeyValue

		for offset := int64(0); offset < 1000; offset++ {
			key := offset + cm1.Seed
			docId := gen.BuildKey(key)
			fake := faker.NewFastFaker()
			fake.Seed(key)
			doc := gen.Template.GenerateDocument(fake, docId, 1024)
			keyValues = append(keyValues, KeyValue{
				Key:    docId,
				Doc:    nil,
				Offset: offset,
			})
			docsArray = append(docsArray, doc)
		}

		fileToUpload, err := getFile(fileFormats[i], "hotel", docsArray)
		if err != nil {
			t.Error("unable to generate file:", err)
		}
		// Emptying the created array of docs
		docsArray = nil

		tempExtra.FilePath = folder + "doc." + fileFormats[i]
		tempExtra.FileFormat = fileFormats[i]
		bulkResult := extStorage.CreateFiles(&fileToUpload, keyValues, tempExtra)
		for _, keyVal := range keyValues {
			if bulkResult.GetError(keyVal.Key) != nil {
				t.Error(bulkResult.GetError(keyVal.Key))
			}
		}
	}

	// Deleting files in Folder

	for _, folder := range folderPaths {
		tempExtra.FolderPath = folder
		extStorage.DeleteFolder(KeyValue{}, tempExtra)
	}

	// Listing objects
	s3Client, err := awsS3.connectionManager.GetS3Cluster(awsAccessKey, awsSecretKey, "empty", awsRegion, nil)
	if err != nil {
		t.Error("listing s3 bucket objects:", err)
		t.Fail()
	}

	prefix := ""
	listObjectsOutput, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: &extra.Bucket,
		Prefix: &prefix,
	})
	if err != nil {
		t.Error("deleting folder: unable to list folder object in S3:", err)
	}

	slices.Sort(folderPaths)
	if len(listObjectsOutput.Contents) == len(folderPaths) {
		t.Error("folders count does not match")
		t.Fail()
	}
	for i, content := range listObjectsOutput.Contents {
		if folderPaths[i] != *content.Key {
			t.Error("folder name does not match")
			t.Fail()
		}
	}

	// Deleting the S3 Bucket
	result = extStorage.DeleteBucket(KeyValue{}, extra)
	if result.GetError() != nil {
		t.Error("deleting s3 bucket:", err)
		t.Fail()
	}

	// Closing the Connection to Cassandra
	if err = extStorage.Close(awsAccessKey); err != nil {
		t.Error(err)
		t.Fail()
	}
}
