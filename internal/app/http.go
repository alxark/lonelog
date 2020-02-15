package app

import (
	"encoding/json"
	"net/http"
	"log"
	"github.com/gorilla/mux"
)

type HttpService struct {
	logger    log.Logger
	Pipelines []*Pipeline
}

func NewHttp(logger log.Logger, pipelines []*Pipeline) (hs *HttpService, err error) {
	hs = &HttpService{}
	hs.logger = logger
	hs.Pipelines = pipelines

	return hs, err
}

//Start serve & listen port
func (hs HttpService) Serve(listen string) {
	//hs.Info("Listen on:  " + hs.listen)
	router := mux.NewRouter()
	//router.HandleFunc("/", indexPageg
	router.HandleFunc("/status", hs.getStatus).Methods("GET") //curl -X GET "http://localhost:10200/regions"

	log.Fatal(http.ListenAndServe(listen, router))
}
func indexPage(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode("Privet")
}

// Log messages
func (hs HttpService) log(r *http.Request, msg interface{}) {
	hs.logger.Printf("HTTP [%s] %s", r.RemoteAddr, msg)
}

type StatusReply struct {
	Pipelines []PipelineStatus
}

// check service health
func (hs HttpService) getStatus(w http.ResponseWriter, r *http.Request) {
	status := StatusReply{}

	var pipelineStatuses []PipelineStatus
	for i := range hs.Pipelines {
		status := hs.Pipelines[i].GetStatus()

		pipelineStatuses = append(pipelineStatuses, status)
	}
	status.Pipelines = pipelineStatuses

	hs.renderOk(w, status)
}

func (hs *HttpService) renderOk(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(data)
}

func renderError(w http.ResponseWriter, msg string) {
	//Log("warn", who, "Render error: "+msg)
	json.NewEncoder(w).Encode(`{"err":"Internal server error. ` + msg + `"}`)
}
