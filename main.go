package main

import (
	"github.com/alxark/lonelog/app"
	"log"
	"os"
	"flag"
)

func main() {
	confPath := flag.String("config", "/etc/lonelog.conf", "path to configuration file")
	// debugMode := flag.Bool("debug", false, "enable additional logging")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
	logger.Println("Starting new application instance")

	cfg, err := app.ReadConfig(*confPath)
	if err != nil {
		logger.Fatal("Configuration parsing error. Got: " + err.Error())
	}

	pipeline, err := app.NewPipeline(*cfg, *logger)
	if err != nil {
		logger.Fatal(err)
	}

	pipeline.Run()
}
