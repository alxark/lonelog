package filters

import (
	"context"
	"errors"
	"github.com/alxark/lonelog/internal/structs"
	"log"
)

type PayloadAssertFilter struct {
	BasicFilter

	FieldOptions map[string]string

	log log.Logger
}

func NewPayloadAssertFilter(options map[string]string, logger log.Logger) (f *PayloadAssertFilter, err error) {
	f = &PayloadAssertFilter{}
	for key, value := range options {
		switch value {
		case "required":
		case "absent":
			continue
		default:
			return nil, errors.New("incorrect field status for " + key + " => " + value)
		}
	}

	f.log = logger
	f.FieldOptions = options

	return f, nil
}

func (f *PayloadAssertFilter) Proceed(ctx context.Context, input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Printf("Payload fields assert. Total rules: %d", len(f.FieldOptions))

messageLoop:
	for ctx.Err() == nil {
		msg, _ := f.ReadMessage(input)

		for fieldName, status := range f.FieldOptions {
			switch status {
			case "required":
				if _, ok := msg.Payload[fieldName]; !ok {
					if f.Debug {
						f.log.Printf("FILTERED: %s", msg.Payload["content"])
					}

					continue messageLoop
				}
				break
			case "absent":
				if _, ok := msg.Payload[fieldName]; ok {
					continue messageLoop
				}
				break
			}
		}

		_ = f.WriteMessage(output, msg)
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
