package filters

import (
	"log"
	"github.com/alxark/lonelog/internal/structs"
	"regexp"
	"strings"
)

type RegexpRemoveFilter struct {
	BasicFilter

	Expressions map[string]regexp.Regexp
	log         log.Logger
}

func NewRegexpRemoveFilter(options map[string]string, logger log.Logger) (f *RegexpRemoveFilter, err error) {
	f = &RegexpRemoveFilter{}
	f.Expressions = make(map[string]regexp.Regexp, len(options))

	f.log = logger
	for k, expression := range options {
		f.log.Printf("Compiling regexp %s => %s", k, expression)
		f.Expressions[k] = *regexp.MustCompile(strings.Trim(expression, " \n\t\r"))
	}

	return f, nil
}

/**
 * Split content field by delimiter
 */
func (f *RegexpRemoveFilter) Proceed(input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Printf("Regexp remove filter activated. Total regexp: %d, target field: %s", len(f.Expressions), f.Field)

messageLoop:
	for msg := range input {
		// skip records without target field
		if _, ok := msg.Payload[f.Field]; !ok {
			output <- msg
			continue
		}

		for _, expression := range f.Expressions {
			match := expression.FindStringSubmatch(msg.Payload[f.Field])
			if match != nil {
				// f.log.Printf("removing %s, expr: %s", msg.Payload[f.Field], expression.String())
				continue messageLoop
			}
		}

		output <- msg
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
