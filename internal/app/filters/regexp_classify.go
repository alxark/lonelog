package filters

import (
	"log"
	"github.com/alxark/lonelog/internal/structs"
	"github.com/hashicorp/hcl"
	"regexp"
	"io/ioutil"
	"errors"
	"bytes"
	"sort"
	"strings"
	"time"
)

/**
 * How this module is working:
 *
 * 1. Check if we already have this value classified, if we have it - just return from our cache
 * 2. We don't have this value, then we will run ALL classify rules against this value and calculate classify result
 * 3. Save data to cache and upgrade record
 */

type RegexpClassifyConfig struct {
	Rules map[string]RegexpClassifyRuleRaw `hcl:"rule"`
}

type RegexpClassifyRuleRaw struct {
	Expression string
	Backref    bool
	Fields     map[string]string
}

type RegexpClassifyRule struct {
	Expression regexp.Regexp
	Backref    bool
	Fields     map[string]string
}

/**
 * Classification result, will be stored to cache
 */
type RegexpClassifyResult struct {
	Counter        int
	LastActivation time.Time
	Fields         map[string]string
}

func (r *RegexpClassifyResult) UpdateActivation() {
	r.LastActivation = time.Now()
	r.Counter += 1
}

func (r *RegexpClassifyResult) IsOld() bool {
	if r.LastActivation.Unix() < time.Now().Unix()-60 {
		return true
	} else {
		return false
	}
}

type RegexpClassifyFilter struct {
	BasicFilter

	Expression regexp.Regexp
	Rules      []RegexpClassifyRule
	log        log.Logger
}

func NewRegexpClassifyFilter(options map[string]string, logger log.Logger) (f *RegexpClassifyFilter, err error) {
	f = &RegexpClassifyFilter{}

	var classesPath string
	var ok bool

	if classesPath, ok = options["rules"]; !ok {
		return nil, errors.New("no regexp_classify rules available")
	}

	b, err := ioutil.ReadFile(classesPath)

	if err != nil {
		return nil, errors.New("failed to read regexp_classify rules " + classesPath)
	}

	conf := &RegexpClassifyConfig{}
	if err := hcl.Decode(conf, bytes.NewBuffer(b).String()); err != nil {
		return nil, err
	}

	logger.Printf("Loaded classification rules. Total rules: %d", len(conf.Rules))

	f.log = logger

	var rules []RegexpClassifyRule
	var rulesKeys []string

	for k, _ := range conf.Rules {
		rulesKeys = append(rulesKeys, k)
	}
	sort.Strings(rulesKeys)

	for _, key := range rulesKeys {
		rule := RegexpClassifyRule{}

		f.log.Printf("Compiling rule %s", conf.Rules[key].Expression)
		rule.Expression = *regexp.MustCompile(strings.Trim(conf.Rules[key].Expression, " \n\t\r"))
		rule.Fields = conf.Rules[key].Fields
		rule.Backref = conf.Rules[key].Backref
		rules = append(rules, rule)
	}

	f.Rules = rules
	f.log.Printf("Regexp classify initialized. Total rules: %d", len(f.Rules))

	return f, nil
}

/**
 * Split content field by delimiter
 */
func (f *RegexpClassifyFilter) Proceed(input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Print("RegexpClassify thread started")

	classifyCache := make(map[string]*RegexpClassifyResult)

	i := 0
	for msg := range input {
		i += 1

		if _, ok := msg.Payload[f.Field]; !ok {
			output <- msg
			continue
		}

		fieldValue := msg.Payload[f.Field]

		payload := msg.Payload

		if result, ok := classifyCache[fieldValue]; ok {
			if f.Debug {
				f.log.Printf("found in classifyCache for %s, total cache size: %d, cache hit: %d", fieldValue, len(classifyCache), result.Counter)
			}

			if len(result.Fields) > 0 {
				for key, value := range result.Fields {
					payload[key] = value
				}

				msg.Payload = payload
			}

			classifyCache[fieldValue].UpdateActivation()

			output <- msg
			continue
		}

		classifyResult := &RegexpClassifyResult{}
		fields := make(map[string]string)

		classifyResult.Fields = make(map[string]string)

		if f.Debug {
			f.log.Printf("not found data for %s", fieldValue)
		}

		for _, v := range f.Rules {
			if v.Expression.MatchString(fieldValue) {
				for matchField, matchValue := range v.Fields {
					fields[matchField] = matchValue
				}
			}
		}

		classifyResult.Fields = fields
		classifyResult.UpdateActivation()
		classifyCache[fieldValue] = classifyResult

		if len(fields) > 0 {
			for k, v := range fields {
				payload[k] = v
			}

			msg.Payload = payload
		}

		output <- msg

		if i >= f.ServiceInterval {
			i = 0

			if f.Debug {
				f.log.Printf("Running service procedures, total cache size: %d", len(classifyCache))
			}

			var toRemove []string
			for k, v := range classifyCache {
				if v.IsOld() {
					toRemove = append(toRemove, k)
					if f.Debug {
						f.log.Printf("cache %s, LA: %s, hits: %d", k, v.LastActivation, v.Counter)
					}
				}
			}

			if f.Debug {
				f.log.Printf("Total items for removal: %d", len(toRemove))
			}

			for _, keyName := range toRemove {
				delete(classifyCache, keyName)
			}

			time.Sleep(10 * time.Second)
		}

	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}
