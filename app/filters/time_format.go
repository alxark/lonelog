package filters

import (
	"log"
	"github.com/alxark/lonelog/structs"
	"errors"
	"time"
)

type TimeFormatFilter struct {
	BasicFilter

	SourceFormat string
	TargetFormat string
	OnError      string

	log log.Logger
}

func NewTimeFormatFilter(options map[string]string, logger log.Logger) (t *TimeFormatFilter, err error) {
	t = &TimeFormatFilter{}

	if sourceFormat, ok := options["source_format"]; ok {
		t.SourceFormat = sourceFormat
	} else {
		return nil, errors.New("no source format")
	}

	if targetFormat, ok := options["target_format"]; ok {
		t.TargetFormat = targetFormat
	} else {
		return nil, errors.New("no target format")
	}

	t.log = logger
	t.OnError = "current_time"

	return t, nil
}

/**
 * Split content field by delimiter
 */
func (t *TimeFormatFilter) Proceed(input chan structs.Message, output chan structs.Message) (err error) {
	t.log.Printf("Updating time format on %s, %s => %s", t.Field, t.SourceFormat, t.TargetFormat)

	for msg := range input {
		if _, ok := msg.Payload[t.Field]; !ok {
			output <- msg
			continue
		}

		date, err := time.Parse(t.SourceFormat, msg.Payload[t.Field])
		if err != nil {
			if t.OnError == "current_time" {
				date = time.Now()
			}
		}

		payload := msg.Payload
		payload[t.Field] = date.Format(t.TargetFormat)
		msg.Payload = payload

		output <- msg
	}

	t.log.Printf("Channel processing finished. Exiting")

	return
}
