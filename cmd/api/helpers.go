package main

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/AryaanB9/sirius_aryaan/internal/meta_data"
	"github.com/AryaanB9/sirius_aryaan/internal/server_requests"
	"github.com/AryaanB9/sirius_aryaan/internal/sirius_documentation"
	"github.com/AryaanB9/sirius_aryaan/internal/task_result"
	"github.com/AryaanB9/sirius_aryaan/internal/task_state"
	"github.com/AryaanB9/sirius_aryaan/internal/tasks"
	"github.com/AryaanB9/sirius_aryaan/internal/tasks/util_sirius"
	"github.com/AryaanB9/sirius_aryaan/internal/template"
)

type jsonResponse struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (app *Config) readJSON(w http.ResponseWriter, r *http.Request, data interface{}) error {
	maxBytes := 10485760
	r.Body = http.MaxBytesReader(w, r.Body, int64(maxBytes))

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(data); err != nil {
		return err
	}

	return nil
}

func (app *Config) writeJSON(w http.ResponseWriter, status int, data any, headers ...http.Header) error {
	out, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if len(headers) > 0 {
		for key, value := range headers[0] {
			w.Header()[key] = value
		}
	}
	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(status)

	_, err = w.Write(out)
	if err != nil {
		return err
	}

	return nil
}

func (app *Config) errorJSON(w http.ResponseWriter, err error, status ...int) error {
	statusCode := http.StatusBadRequest
	if len(status) > 0 {
		statusCode = status[0]
	}

	var payload jsonResponse
	payload.Error = true
	payload.Message = err.Error()
	return app.writeJSON(w, statusCode, payload)
}

func checkIdentifierToken(identifierToken string) error {
	if identifierToken == "" {
		return errors.New("invalid Identifier Token")
	}
	return nil
}

func registerInterfaces() {
	gob.Register(&[]interface{}{})
	gob.Register(&map[string]interface{}{})
	gob.Register(&map[string]any{})
	gob.Register(&tasks.Request{})
	gob.Register(&meta_data.MetaData{})
	gob.Register(&template.Person{})
	gob.Register(&template.Product{})
	gob.Register(&template.Hotel{})
	gob.Register(&template.Small{})
	gob.Register(&server_requests.ServerRequests{})
	gob.Register(&tasks.GenericLoadingTask{})
	gob.Register(&util_sirius.TaskResult{})
	//gob.Register(&bulk_loading.ValidateTask{})
	gob.Register(&task_result.TaskResult{})
	gob.Register(&task_state.TaskState{})
	//gob.Register(&bulk_query_cb.QueryTask{})
	gob.Register(&meta_data.MetaData{})
	gob.Register(&meta_data.CollectionMetaData{})
	gob.Register(&tasks.RetryExceptions{})
	gob.Register(&tasks.BucketWarmUpTask{})

	r := sirius_documentation.Register{}
	for _, taskReg := range r.RegisteredTasks() {
		gob.Register(taskReg.Config)
	}

	for _, helper := range r.HelperStruct() {
		gob.Register(helper)
	}
}
