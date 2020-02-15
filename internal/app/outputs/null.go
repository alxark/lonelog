package outputs

import (
	"log"
	"github.com/alxark/lonelog/internal/structs"
)

type NullOutput struct {
	BasicOutput

	Log log.Logger
}

func NewNullOutput(options map[string]string, logger log.Logger) (s *NullOutput, err error) {
	logger.Printf("Initializing stdout output")

	s = &NullOutput{}
	s.Log = logger
	return s, nil
}

func (s *NullOutput) ReadFrom(input chan structs.Message, runtimeOptions map[string]string, counter chan int) (err error) {
	for range input {

	}

	return
}
