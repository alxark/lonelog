package filters

import (
	"log"
	"errors"
	"github.com/alxark/lonelog/structs"
	"strings"
)

type SubstrContainsFilter struct {
	BasicFilter

	Action      string
	TargetField string
	TargetValue string
	Substring   string

	log log.Logger
}

func NewSubstrContainsFilter(options map[string]string, logger log.Logger) (f *SubstrContainsFilter, err error) {
	f = &SubstrContainsFilter{}

	if _, ok := options["substring"]; !ok {
		return f, errors.New("no substring specified")
	}

	f.Substring = options["substring"]

	if action, ok := options["action"]; ok {
		if action != "set" {
			return nil, errors.New("unknown action: " + f.Action)
		}

		f.Action = action
	} else {
		return nil, errors.New("no action passed")
	}

	if fieldName, ok := options["target_field"]; ok {
		f.TargetField = fieldName
	} else {
		return nil, errors.New("no target field")
	}

	if fieldValue, ok := options["target_value"]; ok && fieldValue != "" {
		f.TargetValue = fieldValue
	} else {
		return nil, errors.New("no target value")
	}

	f.log = logger

	return f, nil
}

/**
 * Split content field by delimiter
 */
func (f *SubstrContainsFilter) Proceed(input chan structs.Message, output chan structs.Message) (err error) {

	for msg := range input {
		// skip records without target field
		if _, ok := msg.Payload[f.Field]; !ok {
			output <- msg
			continue
		}

		if strings.Contains(msg.Payload[f.Field], f.Substring) {
			payload := msg.Payload
			payload[f.TargetField] = f.TargetValue

			msg.Payload = payload
		}

		output <- msg
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
