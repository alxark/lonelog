package filters

import (
	"context"
	"github.com/alxark/lonelog/internal/structs"
	"log"
)

type SetFilter struct {
	BasicFilter

	Updates map[string]string

	log log.Logger
}

func NewSetFilter(options map[string]string, logger log.Logger) (s *SetFilter, err error) {
	s = &SetFilter{}

	s.Updates = options
	s.log = logger

	return s, nil
}

/**
 * Split content field by delimiter
 */
func (s *SetFilter) Proceed(ctx context.Context, input chan structs.Message, output chan structs.Message) (err error) {
	s.log.Printf("Set filter started. Total items: %d", len(s.Updates))

	for msg := range input {
		payload := msg.Payload

		for key, value := range s.Updates {
			payload[key] = value
		}
		msg.Payload = payload

		output <- msg
	}

	s.log.Printf("Channel processing finished. Exiting")

	return
}
