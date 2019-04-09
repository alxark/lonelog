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
	TargetField string
	Timezone     *time.Location

	OnError string

	log log.Logger
}

func NewTimeFormatFilter(options map[string]string, logger log.Logger) (t *TimeFormatFilter, err error) {
	t = &TimeFormatFilter{}
	t.log = logger

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

	if targetField, ok := options["target_field"]; ok {
		t.TargetField = targetField
	} else {
		t.TargetField = t.Field
	}

	if timezone, ok := options["timezone"]; ok {
		location, err := time.LoadLocation(timezone)
		if err != nil {
			return nil, err
		}

		t.Timezone = location
	} else {
		t.Timezone, _ = time.LoadLocation("")
	}

	t.OnError = "current_time"

	return t, nil
}

/**
 * Split content field by delimiter
 */
func (t *TimeFormatFilter) Proceed(input chan structs.Message, output chan structs.Message) (err error) {
	if t.TargetField == "" {
		t.TargetField = t.Field
	}
	
	t.log.Printf("Converting %s (%s) to %s (%s)", t.SourceFormat, t.Field, t.TargetFormat, t.TargetField)

	for msg := range input {
		if _, ok := msg.Payload[t.Field]; !ok {
			output <- msg
			continue
		}

		date, err := time.Parse(t.SourceFormat, msg.Payload[t.Field])
		if err != nil {
			if t.Debug {
				t.log.Print("Incorrect datetime: " + msg.Payload[t.Field] + ", error: " + err.Error())
			}

			if t.OnError == "current_time" {
				date = time.Now()
			}
		}

		date = date.In(t.Timezone)

		payload := msg.Payload
		payload[t.TargetField] = date.Format(t.TargetFormat)
		if t.Debug {
			t.log.Printf(t.GetName() + ": Converted time %s (%s) => %s (%s)", msg.Payload[t.Field], t.Field, payload[t.TargetField], t.TargetField)
		}

		msg.Payload = payload

		output <- msg
	}

	t.log.Printf("Channel processing finished. Exiting")

	return
}
