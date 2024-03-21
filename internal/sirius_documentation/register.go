package sirius_documentation

import (
	"github.com/AryaanB9/sirius_aryaan/internal/task_result"
	"github.com/AryaanB9/sirius_aryaan/internal/tasks"
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
		"/create":          {"POST", &tasks.GenericLoadingTask{}},
		"/delete":          {"POST", &tasks.GenericLoadingTask{}},
		"/upsert":          {"POST", &tasks.GenericLoadingTask{}},
		"/touch":           {"POST", &tasks.GenericLoadingTask{}},
		"/read":            {"POST", &tasks.GenericLoadingTask{}},
		"/bulk-create":     {"POST", &tasks.GenericLoadingTask{}},
		"/bulk-delete":     {"POST", &tasks.GenericLoadingTask{}},
		"/bulk-upsert":     {"POST", &tasks.GenericLoadingTask{}},
		"/bulk-touch":      {"POST", &tasks.GenericLoadingTask{}},
		"/bulk-read":       {"POST", &tasks.GenericLoadingTask{}},
		"/sub-doc-insert":  {"POST", &tasks.GenericLoadingTask{}},
		"/sub-doc-upsert":  {"POST", &tasks.GenericLoadingTask{}},
		"/sub-doc-delete":  {"POST", &tasks.GenericLoadingTask{}},
		"/sub-doc-read":    {"POST", &tasks.GenericLoadingTask{}},
		"/sub-doc-replace": {"POST", &tasks.GenericLoadingTask{}},
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
		"/warmup-bucket": {"POST", &tasks.BucketWarmUpTask{}},
	}
}

func (r *Register) HelperStruct() map[string]any {
	return map[string]any{
		"operationConfig": &tasks.OperationConfig{},
		"bulkError":       &task_result.FailedDocument{},
		"retriedError":    &task_result.FailedDocument{},
		"exceptions":      &tasks.Exceptions{},
		"sdkTimings":      &task_result.SDKTiming{},
		"singleResult":    &task_result.SingleOperationResult{},
	}

}
