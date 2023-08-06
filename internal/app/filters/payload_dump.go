package filters

import (
	"context"
	"encoding/json"
	"github.com/alxark/lonelog/internal/structs"
	"log"
)

type PayloadDumpFilter struct {
	BasicFilter

	log log.Logger
}

func NewPayloadDumpFilter(options map[string]string, logger log.Logger) (f *PayloadDumpFilter, err error) {
	f = &PayloadDumpFilter{}

	f.log = logger

	return f, nil
}

func (f *PayloadDumpFilter) Proceed(ctx context.Context, input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Printf("started payload_dump filter")

	for ctx.Err() == nil {
		msg, _ := f.ReadMessage(input)

		jsonData, err := json.MarshalIndent(msg.Payload, "", "    ")
		if err != nil {
			continue
		}

		f.log.Print(string(jsonData))

		f.WriteMessage(output, msg)
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
