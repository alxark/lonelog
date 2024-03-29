package filters

import (
	"context"
	"errors"
	"github.com/alxark/lonelog/internal/structs"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"math"
	"regexp"
	"strings"
	"sync"
)

type RegexpFilter struct {
	BasicFilter

	RegexpList map[string]string

	log log.Logger
}

var regexpOnce = sync.Once{}
var regexpMetrics = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "ll",
	Subsystem: "filters",
	Name:      "regexp_matches",
}, []string{"filter", "rule"})

// this coef is used to reduce number of matches so in fact we will
const REDUCECOEF = 0.99

/**
 * Regexp with match counts, used to re arrange
 */
type CountableRegexp struct {
	Expression regexp.Regexp
	Name       string
	Matches    int
	Fails      int
}

func (r *CountableRegexp) Match() {
	r.Matches += 1
}

func (r *CountableRegexp) Fail() {
	r.Fails += 1
}

func (r *CountableRegexp) Reduce() {
	r.Matches = int(math.Floor(float64(r.Matches) * REDUCECOEF))
}

func NewRegexpFilter(options map[string]string, logger log.Logger) (f *RegexpFilter, err error) {
	f = &RegexpFilter{}

	f.log = logger
	for k, expression := range options {
		f.log.Printf("Compiling regexp %s => %s", k, expression)
		_, err := regexp.Compile(strings.Trim(expression, " \n\t\r"))
		if err != nil {
			return nil, errors.New("regexp compilation failed:" + err.Error())
		}
	}

	f.RegexpList = options

	return f, nil
}

func (f *RegexpFilter) Init() error {
	regexpOnce.Do(func() {
		prometheus.MustRegister(regexpMetrics)
	})

	return f.BasicFilter.Init()
}

/**
 * Split content field by delimiter
 */
func (f *RegexpFilter) Proceed(ctx context.Context, input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Printf("Regexp filter activated. Total regexp: %d, target field: %s. Service interval: %d",
		len(f.RegexpList), f.Field, f.ServiceInterval)

	var expList []CountableRegexp
	for regexpName, regexpString := range f.RegexpList {
		r := *regexp.MustCompile(strings.Trim(regexpString, " \n\t\r"))

		item := CountableRegexp{Expression: r, Name: regexpName}
		f.log.Printf("Compiled regexp: %s", r.String())
		expList = append(expList, item)
	}

	j := 0
	sortPos := 0
	processed := 0
	for ctx.Err() == nil {
		msg, _ := f.ReadMessage(input)

		j += 1
		processed += 1

		// skip records without target field
		if _, ok := msg.Payload[f.Field]; !ok {
			_ = f.WriteMessage(output, msg)
			continue
		}

	iterateExpressions:
		for i, e := range expList {
			match := e.Expression.FindStringSubmatch(msg.Payload[f.Field])

			if match == nil {
				expList[i].Fail()
				continue
			}

			payload := msg.Payload
			for i, name := range e.Expression.SubexpNames() {
				if name == "" {
					continue
				}

				payload[name] = match[i]
			}

			msg.Payload = payload
			expList[i].Match()

			break iterateExpressions
		}

		_ = f.WriteMessage(output, msg)

		if j == f.ServiceInterval {
			j = 0
			if f.Debug {
				f.log.Printf("Starting regexp re-arrange process, total processed: %d", processed)
			}

			for i, e := range expList {
				if f.Debug {
					f.log.Printf("OK: %d, FAIL: %d => %s", e.Matches, e.Fails, e.Expression.String())
				}
				expList[i].Reduce()

				regexpMetrics.WithLabelValues(f.Name, e.Name).Set(float64(e.Matches))
			}

			if expList[sortPos].Matches < expList[sortPos+1].Matches {
				expList[sortPos], expList[sortPos+1] = expList[sortPos+1], expList[sortPos]
			}

			sortPos += 1
			if sortPos == len(expList)-1 {
				sortPos = 0
			}
		}
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
