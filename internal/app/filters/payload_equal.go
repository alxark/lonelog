package filters

import (
	"context"
	"errors"
	"github.com/alxark/lonelog/internal/structs"
	"log"
)

type PayloadEqualFilter struct {
	BasicFilter

	Options map[string]string

	log log.Logger
}

func NewPayloadEqualFilter(options map[string]string, logger log.Logger) (f *PayloadEqualFilter, err error) {
	f = &PayloadEqualFilter{}

	f.Options = options

	if len(options) == 0 {
		return nil, errors.New("no options passed")
	}

	f.log = logger

	return f, nil
}

func (f *PayloadEqualFilter) Proceed(ctx context.Context, input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Printf("started payload_equal filter. Total items: %d", len(f.Options))

	for ctx.Err() == nil {
		msg, _ := f.ReadMessage(input)

		for key, value := range f.Options {
			if _, ok := msg.Payload[key]; !ok {
				continue
			}

			if value != msg.Payload[key] {
				continue
			}

			f.log.Printf("processing message: %s", msg.Payload["ipv4"])

			f.WriteMessage(output, msg)
		}
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
