package main

import (
	"flag"
	"fmt"
	"github.com/alxark/lonelog/internal/app"
	"log"
	"os"
	"runtime"
)

const (
	defaultHttpPort = 8080
)

func main() {
	confPath := flag.String("config", "/etc/lonelog.conf", "path to configuration file")
	version := flag.Bool("version", false, "check version and exit")

	// debugMode := flag.Bool("debug", false, "enable additional logging")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

	if *version {
		fmt.Print("LoneLog Daemon\nCompiler: " + runtime.Compiler + "\nOS: " + runtime.GOOS + "\n")
		return
	}

	logger.Println("Starting new application instance")

	cfg, err := app.ReadConfig(*confPath)
	if err != nil {
		logger.Fatal("Configuration parsing error. Got: " + err.Error())
	}

	var pipelines []*app.Pipeline
	pipeline, err := app.NewPipeline(*cfg, *logger)
	if err != nil {
		logger.Fatal(err)
	}
	pipelines = append(pipelines, &pipeline)

	httpApi, err := app.NewHttp(*logger, pipelines)

	for i := range pipelines {
		logger.Printf("Starting pipeline #%d", i)
		go pipelines[i].Run()
	}

	var port int
	if cfg.Global.HttpPort > 0 {
		port = cfg.Global.HttpPort
	} else {
		port = defaultHttpPort
	}
	listen := fmt.Sprintf(":%d", port)
	logger.Print("Starting HTTP API on " + listen)
	httpApi.Serve(listen)
}
