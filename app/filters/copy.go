package filters

import (
	"log"
	"github.com/alxark/lonelog/structs"
)

type CopyFilter struct {
	BasicFilter

	Mapping map[string]string

	log log.Logger
}

func NewCopyFilter(options map[string]string, logger log.Logger) (s *CopyFilter, err error) {
	s = &CopyFilter{}

	s.Mapping = options
	s.log = logger

	return s, nil
}

/**
 * Split content field by delimiter
 */
func (s *CopyFilter) Proceed(input chan structs.Message, output chan structs.Message) (err error) {
	s.log.Printf("Set filter started. Total items: %d", len(s.Mapping))

	for msg := range input {
		payload := msg.Payload

		for fromField, toField := range s.Mapping {
			if _, ok := payload[fromField]; !ok {
				continue
			}

			payload[toField] = payload[fromField]
		}
		msg.Payload = payload

		output <- msg
	}

	s.log.Printf("Channel processing finished. Exiting")

	return
}
