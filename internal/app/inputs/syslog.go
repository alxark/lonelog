package inputs

import (
	"errors"
	"fmt"
	"github.com/alxark/lonelog/internal/structs"
	"gopkg.in/mcuadros/go-syslog.v2"
	"log"
	"strconv"
	"time"
)

type Syslog struct {
	BasicInput

	log       log.Logger
	Ip        string
	QueueSize int
	Port      int
}

const (
	syslogStatDumpInterval = 1000
	syslogDefaultQueueSize = 8192
	syslogDefaultPort      = 514
)

func NewSyslog(options map[string]string, logger log.Logger) (s *Syslog, err error) {
	s = &Syslog{}
	if _, ok := options["ip"]; !ok {
		logger.Printf("No listen IP specified, using default: 0.0.0.0")
		s.Ip = "0.0.0.0"
	} else {
		s.Ip = options["ip"]
	}

	if queueSize, ok := options["queue"]; ok {
		queueSizeReal, err := strconv.Atoi(queueSize)
		if err != nil {
			return nil, errors.New("incorrect syslog queue size: " + queueSize)
		}
		s.QueueSize = queueSizeReal
	} else {
		s.QueueSize = syslogDefaultQueueSize
	}

	if _, ok := options["port"]; !ok {
		logger.Printf("No listen port specified, using default: 514")
		s.Port = syslogDefaultPort
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

func (s *Syslog) IsMultiThread() bool {
	return false
}

/**
 * Accept messages and send them to channel
 */
func (s *Syslog) AcceptTo(output chan structs.Message, counter chan int) (err error) {
	s.log.Printf("Starting syslog acceptor on %s:%d, queue size: %d", s.Ip, s.Port, s.QueueSize)

	channel := make(syslog.LogPartsChannel, s.QueueSize)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	server.SetFormat(syslog.RFC3164)
	server.SetHandler(handler)
	server.ListenUDP(fmt.Sprintf("%s:%d", s.Ip, s.Port))
	server.Boot()

	i := 0

	for logItem := range channel {
		i += 1
		msg, err := s.reformatMessage(logItem)
		if err != nil {
			s.log.Printf("Failed to reformat message: " + err.Error())
			continue
		}

		if i >= syslogStatDumpInterval {
			counter <- i
			i = 0
		}

		_ = s.WriteMessage(output, msg)
	}

	return
}

/**
 * Convert message to our internal presentation
 */
func (s *Syslog) reformatMessage(logItem map[string]interface{}) (msg structs.Message, err error) {
	msg.Hostname = logItem["hostname"].(string)
	msg.Content = ""

	ts := logItem["timestamp"].(time.Time)

	msg.AcceptTime = ts
	msg.Payload = make(map[string]string)

	payload := msg.Payload
	payload["content"] = logItem["content"].(string)
	payload["hostname"] = msg.Hostname
	msg.Payload = payload

	return msg, nil
}
