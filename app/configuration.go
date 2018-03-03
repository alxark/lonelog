package app

import (
	"io/ioutil"
	"github.com/hashicorp/hcl"
	"bytes"
	"errors"
)

type Configuration struct {
	Input  []InputPlugin
	Filter []FilterPlugin
	Output []OutputPlugin
}

func ReadConfig(filePath string) (*Configuration, error) {
	b, err := ioutil.ReadFile(filePath)

	if err != nil {
		return nil, errors.New("Failed to read configuration file " + filePath)
	}

	conf := &Configuration{}

	if err := hcl.Decode(conf, bytes.NewBuffer(b).String()); err != nil {
		return nil, err
	}

	return conf, nil
}
