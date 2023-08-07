package outputs

import (
	"regexp"
	"strings"
)

type BasicOutput struct {
	Debug bool
	Name  string
}

func (bo *BasicOutput) PrepareStringVariable(template string, variables map[string]string) (res string) {
	reg := `\$\{(?P<variable>[A-Z\_]+)\}`
	r := regexp.MustCompile(reg)

	res = template
	match := r.FindAllStringSubmatch(template, -1)
	for _, pair := range match {
		keyPlace := pair[0]
		keyName := pair[1]

		if value, ok := variables[keyName]; ok {
			res = strings.Replace(res, keyPlace, value, -1)
		}
	}

	return res
}

func (bo *BasicOutput) Init() error {
	return nil
}

func (bo *BasicOutput) SetName(name string) {
	bo.Name = name
}

func (bo *BasicOutput) GetName() string {
	return bo.Name
}

func (bo *BasicOutput) SetDebug(debug bool) {
	bo.Debug = debug
}
