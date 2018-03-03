package inputs

import (
	syslog "gopkg.in/mcuadros/go-syslog.v2"
	"log"
	"strconv"
	"errors"
	"github.com/alxark/lonelog/structs"
	"fmt"
	"time"
	"math"
)

type Syslog struct {
	structs.Input
	log  log.Logger
	Ip   string
	Port int
}

const BENCHMARK_LIMIT = 10000

func NewSyslog(options map[string]string, logger log.Logger) (s *Syslog, err error) {
	s = &Syslog{}
	if _, ok := options["ip"]; !ok {
		logger.Printf("No listen IP specified, using default: 0.0.0.0")
		s.Ip = "0.0.0.0"
	} else {
		s.Ip = options["ip"]
	}

	if _, ok := options["port"]; !ok {
		logger.Printf("No listen port specified, using default: 514")
		s.Port = 514
	} else {
		s.Port, err = strconv.Atoi(options["port"])
		if err != nil {
			return s, errors.New("Incorrect port number. Got: " + options["port"])
		}
	}
	s.log = logger
	s.log.Printf("Configured syslog on %s:%d", s.Ip, s.Port)

	return
}

/**
 * Accept messages and send them to channel
 */
func (s *Syslog) AcceptTo(output chan structs.Message) (err error) {
	s.log.Printf("Starting syslog acceptor on %s:%d", s.Ip, s.Port)

	channel := make(syslog.LogPartsChannel, 8192)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	server.SetFormat(syslog.RFC3164)
	server.SetHandler(handler)
	server.ListenUDP(fmt.Sprintf("%s:%d", s.Ip, s.Port))
	server.Boot()

	i := 0
	benchmarkStart := time.Now().Unix()

	for logItem := range channel {
		i += 1
		msg, err := s.reformatMessage(logItem)
		if err != nil {
			s.log.Printf("Failed to reformat message: " + err.Error())
			continue
		}

		if i >= BENCHMARK_LIMIT {
			benchmarkNow := time.Now().Unix()

			rps := math.Floor(float64(BENCHMARK_LIMIT) / float64(benchmarkNow - benchmarkStart))
			s.log.Printf("Processed %d, processing speed: %f RPS", BENCHMARK_LIMIT, rps)

			benchmarkStart = benchmarkNow
			i = 0
		}

		output <- msg
	}

	return
}

/**
 * Convert message to our internal presentation
 */
func (s *Syslog) reformatMessage(logItem map[string]interface{}) (msg structs.Message, err error) {
	msg.Hostname = logItem["hostname"].(string)
	msg.Content = logItem["content"].(string)

	ts := logItem["timestamp"].(time.Time)

	msg.AcceptTime = ts
	msg.Payload = make(map[string]string)

	payload := msg.Payload
	payload["content"] = msg.Content
	payload["hostname"] = msg.Hostname
	msg.Payload = payload

	return msg, nil
}
