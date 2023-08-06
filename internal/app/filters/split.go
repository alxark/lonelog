package filters

import (
	"context"
	"errors"
	"github.com/alxark/lonelog/internal/structs"
	"log"
	"strconv"
	"strings"
)

type SplitFilter struct {
	BasicFilter

	Delimiter string
	Prefix    string
	log       log.Logger
}

func NewSplitFilter(options map[string]string, logger log.Logger) (f *SplitFilter, err error) {
	f = &SplitFilter{}

	if _, ok := options["delimiter"]; !ok {
		logger.Print(options)
		return f, errors.New("no delimiter specified")
	}

	if _, ok := options["prefix"]; !ok {
		return f, errors.New("no prefix specified")
	}

	if _, ok := options["field"]; !ok {
		f.Field = "content"
	} else {
		f.Field = options["field"]
	}

	f.Delimiter = options["delimiter"]
	f.Prefix = options["prefix"]
	f.log = logger

	return f, nil
}

/**
 * Split content field by delimiter
 */
func (f *SplitFilter) Proceed(ctx context.Context, input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Printf("Split filter activated. Delimiter: %s, Prefix: %s, Field: %s", f.Delimiter, f.Prefix, f.Field)

	for ctx.Err() == nil {
		msg, _ := f.ReadMessage(input)

		if fieldData, ok := msg.Payload[f.Field]; ok {
			splitData := strings.Split(fieldData, f.Delimiter)

			payload := msg.Payload
			for i, v := range splitData {
				keyName := f.Prefix + strconv.Itoa(i)
				payload[keyName] = v
			}

			msg.Payload = payload
		}

		f.WriteMessage(output, msg)
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
