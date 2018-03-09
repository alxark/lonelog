package outputs

import (
	"log"
	"encoding/json"
	"github.com/alxark/lonelog/structs"
	"strings"
)

type StdoutOutput struct {
	Log log.Logger
}

func NewStdoutOutput(options map[string]string, logger log.Logger) (s *StdoutOutput, err error) {
	logger.Printf("Initializing stdout output")

	s = &StdoutOutput{}
	s.Log = logger
	return s, nil
}

func (s *StdoutOutput) ReadFrom(input chan structs.Message, runtimeOptions map[string]string, counter chan int) (err error) {
	for msg := range input {
		marshaled, err := json.Marshal(msg)
		if err != nil {
			s.Log.Printf("Failed to marshall message. Got: " + err.Error())
			continue
		}

		s.Log.Print(string(marshaled))
		s.Log.Print(strings.Repeat("=", 50))
	}

	return
}