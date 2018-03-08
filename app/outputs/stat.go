package outputs

import (
	"log"
	"github.com/alxark/lonelog/structs"
	"time"
	"strconv"
)

const DEFAULT_PERIOD = 10

type StatOutput struct {
	log log.Logger
	Period int64
}

func NewStatOutput(options map[string]string, logger log.Logger) (s *StatOutput, err error) {
	logger.Printf("Initializing stat output")

	s = &StatOutput{}
	s.log = logger

	if _, ok := options["period"]; ok {
		period, err := strconv.Atoi(options["period"])
		if err != nil {
			s.log.Printf("Failed to setup period. Incorrect value: %s, using default: %d", options["period"], DEFAULT_PERIOD)
		}

		s.Period = int64(period)
	} else {
		s.Period = DEFAULT_PERIOD
	}

	return s, nil
}

func (s *StatOutput) ReadFrom(input chan structs.Message, runtimeOptions map[string]string) (err error) {
	s.log.Printf("Started stat output. Period: %d", s.Period)

	start := time.Now().Unix()
	counter := 0

	for range input {
		counter += 1

		if start < time.Now().Unix() - s.Period {
			time_diff := time.Now().Unix() - start

			rps := float32(counter) / float32(time_diff)
			s.log.Printf("Current speed %f RPS, processed: %d", rps, counter)

			start = time.Now().Unix()
			counter = 0
		}
	}

	return
}