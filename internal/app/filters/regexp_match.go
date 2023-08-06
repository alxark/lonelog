package filters

import (
	"context"
	"errors"
	"github.com/alxark/lonelog/internal/structs"
	"log"
	"regexp"
	"strings"
)

type RegexpMatchFilter struct {
	BasicFilter

	Expression  regexp.Regexp
	Action      string
	TargetField string
	TargetValue string
	log         log.Logger
}

func NewRegexpMatchFilter(options map[string]string, logger log.Logger) (f *RegexpMatchFilter, err error) {
	f = &RegexpMatchFilter{}
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

	if expression, ok := options["expression"]; ok {
		f.Expression = *regexp.MustCompile(strings.Trim(expression, " \n\t\r"))
	} else {
		return nil, errors.New("no expression")
	}
	f.log = logger

	return f, nil
}

/**
 * Split content field by delimiter
 */
func (f *RegexpMatchFilter) Proceed(ctx context.Context, input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Printf("Regexp match filter activated. RegExp: %s, will %s to %s => %s",
		f.Expression.String(), f.Action, f.TargetField, f.TargetValue)

	for msg := range input {
		// skip records without target field
		if _, ok := msg.Payload[f.Field]; !ok {
			output <- msg
			continue
		}

		if f.Expression.MatchString(msg.Payload[f.Field]) {
			payload := msg.Payload
			payload[f.TargetField] = f.TargetValue

			msg.Payload = payload
		}

		output <- msg
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
