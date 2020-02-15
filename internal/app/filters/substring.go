package filters

import (
	"errors"
	"github.com/alxark/lonelog/internal/structs"
	"log"
	"strconv"
)

type SubstringFilter struct {
	BasicFilter

	Start  int
	Length int

	log log.Logger
}

func NewSubstringFilter(options map[string]string, logger log.Logger) (f *SubstringFilter, err error) {
	f = &SubstringFilter{}

	if startValue, ok := options["start"]; ok {
		start, err := strconv.Atoi(startValue)
		if err != nil {
			f.log.Print("Unable to parse start value for filter, got: " + startValue + ", should be int")
			return nil, err
		}

		f.Start = start
	} else {
		f.Start = 0
	}

	if lengthValue, ok := options["length"]; ok {
		length, err := strconv.Atoi(lengthValue)
		if err != nil {
			return nil, errors.New("unable to parse length option, expected string, got: " + lengthValue)
		}

		f.Length = length
	} else {
		f.log.Print("No length options for filter")
		return nil, errors.New("no length in filter options")
	}

	f.log = logger

	return f, nil
}

/**
 * Split content field by delimiter
 */
func (f *SubstringFilter) Proceed(input chan structs.Message, output chan structs.Message) (err error) {
	end := f.Start + f.Length

	for msg := range input {
		// skip records without target field
		if _, ok := msg.Payload[f.Field]; !ok {
			output <- msg
			continue
		}

		payload := msg.Payload
		payload[f.Field] = payload[f.Field][f.Start:end]
		msg.Payload = payload

		output <- msg
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
