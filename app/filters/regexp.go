package filters

import (
	"log"
	"github.com/alxark/lonelog/structs"
	"regexp"
	"strings"
)

type RegexpFilter struct {
	BasicFilter

	Expressions map[string]regexp.Regexp
	log         log.Logger
}

func NewRegexpFilter(options map[string]string, logger log.Logger) (f *RegexpFilter, err error) {
	f = &RegexpFilter{}
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
func (f *RegexpFilter) Proceed(input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Printf("Regexp filter activated. Total regexp: %d, target field: %s", len(f.Expressions), f.Field)

	for msg := range input {
		// skip records without target field
		if _, ok := msg.Payload[f.Field]; !ok {
			output <- msg
			continue
		}

		// f.log.Print(msg.Payload[f.Field])
		for _, expression := range f.Expressions {
			// f.log.Printf(expression.String())

			match := expression.FindStringSubmatch(msg.Payload[f.Field])
			if match == nil {
				continue
			}

			payload := msg.Payload
			for i, name := range expression.SubexpNames() {
				if name == "" {
					continue
				}

				payload[name] = match[i]
				// f.log.Printf("'%s'\t %d -> %s\n", name, i, match[i])
			}

			msg.Payload = payload
		}

		output <- msg
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
