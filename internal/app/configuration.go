package app

import (
	hcl "github.com/hashicorp/hcl/v2/hclsimple"
)

type GlobalConfiguration struct {
	OutputSplay  int `hcl:"output_splay"`
	StatInterval int `hcl:"stat_interval"`
	HttpPort     int `hcl:"http_port"`
}

type InConfiguration struct {
	Queue int           `hcl:"queue,optional"`
	Input []InputPlugin `hcl:"input,block"`
}

type OutConfiguration struct {
	Queue  int            `hcl:"queue,optional"`
	Output []OutputPlugin `hcl:"output,block"`
}

type Configuration struct {
	Global GlobalConfiguration `hcl:"global,block"`
	In     InConfiguration     `hcl:"in,block"`
	Out    OutConfiguration    `hcl:"out,block"`
	Filter []FilterPlugin      `hcl:"filter,block"`
}

func ReadConfig(filePath string) (*Configuration, error) {

	conf := &Configuration{}

	if err := hcl.DecodeFile(filePath, nil, conf); err != nil {
		return nil, err
	}

	return conf, nil
}
