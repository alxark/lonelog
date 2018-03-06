package app

import (
	"io/ioutil"
	"github.com/hashicorp/hcl"
	"bytes"
	"errors"
)

type GlobalConfiguration struct {
	OutputSplay  int `hcl:"output_splay"`
	StatInterval int `hcl:"stat_interval"`
}

type InConfiguration struct {
	Queue int
	Input []InputPlugin
}
type OutConfiguration struct {
	Queue  int
	Output []OutputPlugin
}
type Configuration struct {
	Global GlobalConfiguration
	In     InConfiguration
	Out    OutConfiguration
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
