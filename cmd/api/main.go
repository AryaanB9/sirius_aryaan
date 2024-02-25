package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/barkha06/sirius/internal/server_requests"
	"github.com/barkha06/sirius/internal/sirius_documentation"
	"github.com/barkha06/sirius/internal/tasks_manager"
)

const webPort = "4000"
const TaskQueueSize = 100

type Config struct {
	taskManager    *tasks_manager.TaskManager
	serverRequests *server_requests.ServerRequests
}

type DebugServer struct {
	*http.Server
}

// NewDebugServer provides new debug http server
func NewDebugServer(address string) *DebugServer {
	return &DebugServer{
		&http.Server{
			Addr:    address,
			Handler: http.DefaultServeMux,
		},
	}
}

func main() {

	go func() {
		debugServer := NewDebugServer(fmt.Sprintf("%s:%d", "0.0.0.0", 6060))
		log.Println("Starting Sirius profiling service at 6060")
		log.Fatal(debugServer.ListenAndServe())
	}()
	// Generate Sirius Documentation
	go sirius_documentation.Generate()

	// registers structures which will store and load from disk
	registerInterfaces()

	// setting up the log file.
	//gocb.SetLogger(gocb.VerboseStdioLogger())
	logFile, err := os.OpenFile(getFileName(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	// app will contain entities which are required at top most level of sirius.
	app := Config{
		taskManager:    tasks_manager.NewTasKManager(TaskQueueSize),
		serverRequests: server_requests.NewServerRequests(),
	}

	log.Printf("Starting Document Loading Service at port %s\n", webPort)
	srv := http.Server{
		Addr:    fmt.Sprintf(":%s", webPort),
		Handler: app.routes(),
	}
	// start the server
	err = srv.ListenAndServe()
	if err != nil {
		app.taskManager.StopTaskManager()
		log.Println(err)
		os.Exit(-1)
	}
}
