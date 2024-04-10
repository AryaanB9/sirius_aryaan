package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/couchbaselabs/sirius/internal/tasks"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
)

// routes returns a http Handler which supports multiple http request.
func (app *Config) routes() http.Handler {

	mux := chi.NewRouter()

	// who is allowed to use
	mux.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"https://*", "http://*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	mux.Use(middleware.Heartbeat("/ping"))

	mux.Get("/check-online", app.testServer)
	mux.Post("/result", app.taskResult)
	mux.Post("/clear_data", app.clearRequestFromServer)
	mux.Post("/warmup-bucket", app.WarmUpBucket)

	mux.Post("/validate", app.validateTask)
	mux.Post("/retry-exceptions", app.RetryExceptionTask)

	mux.Post("/create", app.insertTask)
	mux.Post("/bulk-create", app.bulkInsertTask)
	mux.Post("/read", app.readTask)
	mux.Post("/bulk-read", app.bulkReadTask)
	mux.Post("/upsert", app.upsertTask)
	mux.Post("/bulk-upsert", app.bulkUpsertTask)
	mux.Post("/delete", app.deleteTask)
	mux.Post("/bulk-delete", app.bulkDeleteTask)
	mux.Post("/touch", app.touchTask)
	mux.Post("/bulk-touch", app.bulkTouchTask)

	mux.Post("/sub-doc-insert", app.SubDocInsertTask)
	mux.Post("/sub-doc-upsert", app.SubDocUpsertTask)
	mux.Post("/sub-doc-delete", app.SubDocDeleteTask)
	mux.Post("/sub-doc-read", app.SubDocReadTask)
	mux.Post("/sub-doc-replace", app.SubDocReplaceTask)

	mux.Post("/list-database", app.listDBTask)
	mux.Post("/create-database", app.createDBTask)
	mux.Post("/delete-database", app.deleteDBTask)
	mux.Post("/count", app.CountTask)

	mux.Post("/validate-columnar", app.validateColumnarTask)

	// Endpoints for External/Blob Storage use case
	mux.Post("/create-bucket", app.createBucketTask)
	mux.Post("/delete-bucket", app.deleteBucketTask)
	mux.Post("/create-folder", app.folderInsertTask)
	mux.Post("/delete-folder", app.folderDeleteTask)
	mux.Post("/create-file", app.fileInsertTask)
	mux.Post("/update-file", app.fileUpdateTask)
	mux.Post("/delete-file", app.fileDeleteTask)
	mux.Post("/create-files-in-folders", app.insertFilesInFoldersTask)
	mux.Post("/update-files-in-folder", app.updateFilesInFolderTask)
	mux.Post("/delete-files-in-folder", app.deleteFilesInFolderTask)
	mux.Post("/get-info", app.getInfoTask)

	return mux
}

func getFileName() string {
	cw, err := os.Getwd()
	if err != nil {
		log.Fatalf(err.Error())
	}
	return filepath.Join(cw, tasks.RequestPath, "sirius_logs")
}
