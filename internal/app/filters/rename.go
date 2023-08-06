package filters

import (
	"context"
	"github.com/alxark/lonelog/internal/structs"
	"log"
)

type RenameFilter struct {
	BasicFilter

	RenameMap map[string]string
	log       log.Logger
}

func NewRenameFilter(options map[string]string, logger log.Logger) (f *RenameFilter, err error) {
	f = &RenameFilter{}
	f.RenameMap = options
	f.log = logger

	return f, nil
}

/**
 * Split content field by delimiter
 */
func (f *RenameFilter) Proceed(ctx context.Context, input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Printf("Rename filter activated. Total renames: %d", len(f.RenameMap))

	for msg := range input {
		payload := msg.Payload

		for from, to := range f.RenameMap {
			if value, ok := payload[from]; ok {
				payload[to] = value
				delete(payload, from)
			}
		}
		msg.Payload = payload
		output <- msg
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
