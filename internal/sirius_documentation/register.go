package sirius_documentation

import (
	"github.com/AryaanB9/sirius_aryaan/internal/task_result"
	"github.com/AryaanB9/sirius_aryaan/internal/tasks/data_loading"
	"github.com/AryaanB9/sirius_aryaan/internal/tasks/util_sirius"
)

type TaskRegister struct {
	HttpMethod string
	Config     interface{}
}

type Register struct {
}

func (r *Register) RegisteredTasks() map[string]TaskRegister {
	return map[string]TaskRegister{
		"/result":          {"POST", &util_sirius.TaskResult{}},
		"/clear_data":      {"POST", &util_sirius.ClearTask{}},
		"/create":          {"POST", &data_loading.GenericLoadingTask{}},
		"/delete":          {"POST", &data_loading.GenericLoadingTask{}},
		"/upsert":          {"POST", &data_loading.GenericLoadingTask{}},
		"/touch":           {"POST", &data_loading.GenericLoadingTask{}},
		"/read":            {"POST", &data_loading.GenericLoadingTask{}},
		"/bulk-create":     {"POST", &data_loading.GenericLoadingTask{}},
		"/bulk-delete":     {"POST", &data_loading.GenericLoadingTask{}},
		"/bulk-upsert":     {"POST", &data_loading.GenericLoadingTask{}},
		"/bulk-touch":      {"POST", &data_loading.GenericLoadingTask{}},
		"/bulk-read":       {"POST", &data_loading.GenericLoadingTask{}},
		"/sub-doc-insert":  {"POST", &data_loading.GenericLoadingTask{}},
		"/sub-doc-upsert":  {"POST", &data_loading.GenericLoadingTask{}},
		"/sub-doc-delete":  {"POST", &data_loading.GenericLoadingTask{}},
		"/sub-doc-read":    {"POST", &data_loading.GenericLoadingTask{}},
		"/sub-doc-replace": {"POST", &data_loading.GenericLoadingTask{}},
		//"/validate":    {"POST", &bulk_loading.ValidateTask{}},
		//"/single-create":          {"POST", &key_based_loading_cb.SingleInsertTask{}},
		//"/single-delete":          {"POST", &key_based_loading_cb.SingleDeleteTask{}},
		//"/single-upsert":          {"POST", &key_based_loading_cb.SingleUpsertTask{}},
		//"/single-read":            {"POST", &key_based_loading_cb.SingleReadTask{}},
		//"/single-touch":           {"POST", &key_based_loading_cb.SingleTouchTask{}},
		//"/single-replace":         {"POST", &key_based_loading_cb.SingleReplaceTask{}},
		//"/run-template-query":     {"POST", &bulk_query_cb.QueryTask{}},
		//"/retry-exceptions":       {"POST", &bulk_loading.RetryExceptions{}},
		//"/single-sub-doc-insert":  {"POST", &key_based_loading_cb.SingleSubDocInsert{}},
		//"/single-sub-doc-upsert":  {"POST", &key_based_loading_cb.SingleSubDocUpsert{}},
		//"/single-sub-doc-replace": {"POST", &key_based_loading_cb.SingleSubDocReplace{}},
		//"/single-sub-doc-delete":  {"POST", &key_based_loading_cb.SingleSubDocDelete{}},
		//"/single-sub-doc-read":    {"POST", &key_based_loading_cb.SingleSubDocRead{}},
		//"/single-doc-validate":    {"POST", &key_based_loading_cb.SingleValidate{}},
		"/warmup-bucket": {"POST", &util_sirius.BucketWarmUpTask{}},
	}
}

func (r *Register) HelperStruct() map[string]any {
	return map[string]any{
		"operationConfig": &data_loading.OperationConfig{},
		"bulkError":       &task_result.FailedDocument{},
		"retriedError":    &task_result.FailedDocument{},
		"exceptions":      &data_loading.Exceptions{},
		"sdkTimings":      &task_result.SDKTiming{},
		"singleResult":    &task_result.SingleOperationResult{},
	}

}
