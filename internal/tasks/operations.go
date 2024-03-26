package tasks

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/AryaanB9/sirius_aryaan/internal/external_storage"
	"github.com/AryaanB9/sirius_aryaan/internal/template"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/AryaanB9/sirius_aryaan/internal/db"
	"github.com/AryaanB9/sirius_aryaan/internal/docgenerator"
	"github.com/AryaanB9/sirius_aryaan/internal/task_result"
	"github.com/AryaanB9/sirius_aryaan/internal/task_state"

	"github.com/bgadrian/fastfaker/faker"
	"github.com/dnlo/struct2csv"
	"github.com/linkedin/goavro/v2"
	"github.com/parquet-go/parquet-go"
)

func insertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}
		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)

		doc := gen.Template.GenerateDocument(fake, docId, operationConfig.DocSize)
		doc, err := gen.Template.GetValues(doc)
		initTime := time.Now().UTC().Format(time.RFC850)
		if err != nil {
			result.IncrementFailure(initTime, docId, err, false, nil, offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			continue
		}

		operationResult := database.Create(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, db.KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: offset,
		}, extra)

		if operationResult.GetError() != nil {
			if db.CheckAllowedInsertError(operationResult.GetError()) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
				continue
			} else {
				result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}

		operationResult = nil
		doc = nil

	}

}

func upsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	_ bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, req *Request, identifier string, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)
		originalDoc := gen.Template.GenerateDocument(fake, docId, operationConfig.DocSize)
		originalDoc, err := retracePreviousMutations(req, identifier, offset, originalDoc, gen, fake,
			result.ResultSeed)

		docUpdated, err2 := gen.Template.UpdateDocument(operationConfig.FieldsToChange, originalDoc,
			operationConfig.DocSize, fake)
		if err2 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err2, false, nil, offset)
			continue
		}
		docUpdated, err = gen.Template.GetValues(docUpdated)
		if err != nil {
			result.IncrementFailure(initTime, docId, err, false, nil, offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			continue
		}

		operationResult := database.Update(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, db.KeyValue{
			Key:    docId,
			Doc:    docUpdated,
			Offset: offset,
		}, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}

		operationResult = nil
		docUpdated = nil
	}
}

func deleteDocuments(start, end, seed int64, rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		initTime := time.Now().UTC().Format(time.RFC850)
		operationResult := database.Delete(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, docId, offset,
			extra)

		if operationResult.GetError() != nil {
			if db.CheckAllowedDeletetError(operationResult.GetError()) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
				continue

			} else {
				result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func readDocuments(start, end, seed int64, _ bool, gen *docgenerator.Generator, state *task_state.TaskState,
	result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		initTime := time.Now().UTC().Format(time.RFC850)
		operationResult := database.Read(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, docId, offset,
			extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func touchDocuments(start, end, seed int64, _ bool, gen *docgenerator.Generator, state *task_state.TaskState,
	result *task_result.TaskResult, databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		initTime := time.Now().UTC().Format(time.RFC850)
		operationResult := database.Touch(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, docId, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func subDocInsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, value := range gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize) {
			keyValues = append(keyValues, db.KeyValue{
				Key:    subPath,
				Doc:    value,
				Offset: subPathOffset,
			})
			subPathOffset++
		}

		operationResult := database.InsertSubDoc(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password,
			docId, keyValues, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func subDocReadDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, _ := range gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize) {
			keyValues = append(keyValues, db.KeyValue{
				Key:    subPath,
				Offset: subPathOffset,
			})
			subPathOffset++
		}

		operationResult := database.ReadSubDoc(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password,
			docId, keyValues, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func subDocUpsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	_ bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, req *Request, identifier string, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		subDocumentMap := gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize)
		if _, err1 := retracePreviousSubDocMutations(req, identifier, offset, gen, fake, result.ResultSeed,
			subDocumentMap); err1 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err1, false, nil, offset)
			continue
		}

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, value := range gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize) {
			keyValues = append(keyValues, db.KeyValue{
				Key:    subPath,
				Doc:    value,
				Offset: subPathOffset,
			})
			subPathOffset++
		}

		operationResult := database.UpsertSubDoc(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password,
			docId, keyValues, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func subDocDeleteDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, _ := range gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize) {
			keyValues = append(keyValues, db.KeyValue{
				Key:    subPath,
				Offset: subPathOffset,
			})
			subPathOffset++
		}

		operationResult := database.DeleteSubDoc(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password,
			docId, keyValues, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func subDocReplaceDocuments(start, end, seed int64, operationConfig *OperationConfig,
	_ bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, req *Request, identifier string, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		subDocumentMap := gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize)
		if _, err1 := retracePreviousSubDocMutations(req, identifier, offset, gen, fake, result.ResultSeed,
			subDocumentMap); err1 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err1, false, nil, offset)
			continue
		}

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, value := range gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize) {
			keyValues = append(keyValues, db.KeyValue{
				Key:    subPath,
				Doc:    value,
				Offset: subPathOffset,
			})
			subPathOffset++
		}

		operationResult := database.ReplaceSubDoc(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password,
			docId, keyValues, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func bulkInsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	//_, dbErr := db.ConfigDatabase(databaseInfo.DBType)

	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}
	var keyValues []db.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := gen.Template.GenerateDocument(fake, docId, operationConfig.DocSize)
		doc, err := gen.Template.GetValues(doc)
		if err != nil {
			result.FailWholeBulkOperation(start, end, err, state, gen, seed)
			return
		}
		keyValues = append(keyValues, db.KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: offset,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := database.CreateBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues,
		extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			if db.CheckAllowedInsertError(bulkResult.GetError(x.Key)) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			} else {
				result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}

	}

	//for _, x := range keyValues {
	//
	//	result.IncrementFailure(initTime, x.Key, errors.New("Test"), false, nil, x.Offset)
	//	state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
	//
	//}
	keyValues = nil
}

func bulkUpsertDocuments(start int64, end int64, seed int64, operationConfig *OperationConfig, rerun bool,
	gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, req *Request, identifier string,
	wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}
	var keyValues []db.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		originalDoc := gen.Template.GenerateDocument(fake, docId, operationConfig.DocSize)
		originalDoc, _ = retracePreviousMutations(req, identifier, offset, originalDoc, gen, fake,
			result.ResultSeed)

		docUpdated, _ := gen.Template.UpdateDocument(operationConfig.FieldsToChange, originalDoc,
			operationConfig.DocSize, fake)
		docUpdated, err := gen.Template.GetValues(docUpdated)
		if err != nil {
			result.FailWholeBulkOperation(start, end, err, state, gen, seed)
			return
		}
		keyValues = append(keyValues, db.KeyValue{
			Key:    docId,
			Doc:    docUpdated,
			Offset: offset,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := database.UpdateBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues,
		extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
				x.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}

	}
}

func bulkDeleteDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	var keyValues []db.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, db.KeyValue{
			Key:    docId,
			Offset: offset,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := database.DeleteBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues,
		extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			if db.CheckAllowedDeletetError(bulkResult.GetError(x.Key)) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			} else {
				result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}
	}
}

func bulkReadDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	var keyValues []db.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, db.KeyValue{
			Key:    docId,
			Offset: offset,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := database.ReadBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues,
		extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
				x.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}

		} else {
			// log.Println(bulkResult.Value(x.Key))
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}
	}
}

func bulkTouchDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	var keyValues []db.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, db.KeyValue{
			Key: docId,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := database.TouchBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues,
		extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
				x.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}
	}
}

func validateColumnar(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	var keyValues []db.KeyValue
	var docIDs []string
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		docIDs = append(docIDs, docId)
		keyValues = append(keyValues, db.KeyValue{
			Key:    docId,
			Offset: offset,
		})
	}
	initTime := time.Now().UTC().Format(time.RFC850)
	columnarClient := db.NewColumnarConnectionManager()
	dbErr = columnarClient.Connect(extra.ConnStr, extra.Username, extra.Password, extra)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}
	_ = columnarClient.Warmup(extra.ConnStr, extra.Username, extra.Password, extra)
	bulkResultColumnar := columnarClient.ReadBulk(extra.ConnStr, extra.Username, extra.Password, keyValues, extra)
	bulkResultDB := database.ReadBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues, extra)
	for _, x := range keyValues {
		if bulkResultDB.GetError(x.Key) != nil && bulkResultColumnar.GetError(x.Key) != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		} else if bulkResultDB.GetError(x.Key) == nil && bulkResultColumnar.GetError(x.Key) == nil {
			ok, _ := gen.Template.Compare(bulkResultDB.Value(x.Key), bulkResultColumnar.Value(x.Key))
			if !ok {
				result.IncrementFailure(initTime, x.Key, errors.New("Template Compare Failed "+databaseInfo.DBType), false, bulkResultDB.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			} else {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			}
		} else {
			errStr := ""
			if bulkResultDB.GetError(x.Key) != nil {
				errStr = "  Document in Columnar but error in the Database"
			} else {
				errStr = "  Document in Database but not in Columnar"
			}
			result.IncrementFailure(initTime, x.Key, errors.New("Mismatched data:  "+databaseInfo.DBType+": "+x.Key+errStr), false, bulkResultDB.GetExtra(x.Key),
				x.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
		}
	}
}

func createS3Bucket(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValue external_storage.KeyValue
	key := seed
	docId := gen.BuildKey(key)
	keyValue = external_storage.KeyValue{
		Key:    docId,
		Doc:    extra.Bucket,
		Offset: 0,
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	folderResult := extStorage.CreateBucket(keyValue, extra)

	if folderResult.GetError() != nil {
		if db.CheckAllowedInsertError(folderResult.GetError()) && rerun {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
		} else {
			result.IncrementFailure(initTime, keyValue.Key, folderResult.GetError(), false, folderResult.GetExtra(),
				keyValue.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: keyValue.Offset}
		}
	} else {
		state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
	}
}

func deleteS3Bucket(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValue external_storage.KeyValue
	key := seed
	docId := gen.BuildKey(key)
	keyValue = external_storage.KeyValue{
		Key:    docId,
		Doc:    nil,
		Offset: 0,
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	folderResult := extStorage.DeleteBucket(keyValue, extra)

	if folderResult.GetError() != nil {
		if db.CheckAllowedInsertError(folderResult.GetError()) && rerun {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
		} else {
			result.IncrementFailure(initTime, keyValue.Key, folderResult.GetError(), false, folderResult.GetExtra(),
				keyValue.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: keyValue.Offset}
		}
	} else {
		state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
	}
}

func insertFolder(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValue external_storage.KeyValue
	key := seed
	docId := gen.BuildKey(key)
	keyValue = external_storage.KeyValue{
		Key:    docId,
		Doc:    extra.FolderPath,
		Offset: 0,
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	folderResult := extStorage.CreateFolder(keyValue, extra)

	if folderResult.GetError() != nil {
		if db.CheckAllowedInsertError(folderResult.GetError()) && rerun {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
		} else {
			result.IncrementFailure(initTime, keyValue.Key, folderResult.GetError(), false, folderResult.GetExtra(),
				keyValue.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: keyValue.Offset}
		}
	} else {
		state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
	}
}

func deleteFolder(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValue external_storage.KeyValue
	key := seed
	docId := gen.BuildKey(key)
	keyValue = external_storage.KeyValue{
		Key:    docId,
		Doc:    nil,
		Offset: 0,
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	folderResult := extStorage.DeleteFolder(keyValue, extra)

	if folderResult.GetError() != nil {
		if db.CheckAllowedInsertError(folderResult.GetError()) && rerun {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
		} else {
			result.IncrementFailure(initTime, keyValue.Key, folderResult.GetError(), false, folderResult.GetExtra(),
				keyValue.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: keyValue.Offset}
		}
	} else {
		state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: keyValue.Offset}
	}
}

func insertFiles(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValues []external_storage.KeyValue
	var docsArray []interface{}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := gen.Template.GenerateDocument(fake, docId, operationConfig.DocSize)
		keyValues = append(keyValues, external_storage.KeyValue{
			Key:    docId,
			Doc:    nil,
			Offset: offset,
		})
		docsArray = append(docsArray, doc)
	}

	// Handling the Different File Formats
	templateName := strings.ToLower(operationConfig.TemplateName)
	var fileToUpload []byte
	var errDocToFileFormat error

	switch strings.ToLower(extra.FileFormat) {
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
			// TODO update the template with parquet tags
			schema := parquet.SchemaOf(new(template.Person))
			writer = parquet.NewGenericWriter[interface{}](bufferParquetFile, schema)
		} else if strings.ToLower(templateName) == "product" {
			// TODO update the template with parquet tags
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
		panic("invalid file format or file format not supported")
	}

	// Emptying the created array of docs
	docsArray = nil

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := extStorage.CreateFiles(&fileToUpload, keyValues, extra)

	// Emptying created file
	fileToUpload = nil

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			if db.CheckAllowedInsertError(bulkResult.GetError(x.Key)) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			} else {
				result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}

	}
	keyValues = nil
}

func deleteFiles(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValues []external_storage.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, external_storage.KeyValue{
			Key: docId,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := extStorage.DeleteFiles(keyValues, extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			if db.CheckAllowedDeletetError(bulkResult.GetError(x.Key)) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			} else {
				result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}
	}
}

func insertFilesInFolders(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	insertFiles(start, end, seed, operationConfig, rerun, gen, state, result, databaseInfo, extra, wg)
}

func updateFilesInFolder(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	insertFiles(start, end, seed, operationConfig, rerun, gen, state, result, databaseInfo, extra, wg)
}

func deleteFilesInFolder(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo DatabaseInformation, extra external_storage.ExternalStorageExtras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	extStorage, extStorageErr := external_storage.ConfigExternalStorage(databaseInfo.DBType)
	if extStorageErr != nil {
		result.FailWholeBulkOperation(start, end, extStorageErr, state, gen, seed)
		return
	}

	var keyValues []external_storage.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, external_storage.KeyValue{
			Key: docId,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := extStorage.DeleteFilesInFolder(keyValues, extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			if db.CheckAllowedDeletetError(bulkResult.GetError(x.Key)) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			} else {
				result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}
	}
}
