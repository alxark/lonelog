package app

import (
	"log"
	"time"
	"github.com/alxark/lonelog/app/inputs"
	"github.com/alxark/lonelog/app/outputs"
	"github.com/alxark/lonelog/structs"
	"github.com/alxark/lonelog/app/filters"
	"errors"
)

const CHAIN_SIZE = 8192

type Pipeline struct {
	log          log.Logger
	Inputs       []structs.Input
	Outputs      []structs.Output
	Filters      []structs.Filter
	InputStream  chan structs.Message
	OutputStream chan structs.Message
}

func NewPipeline(configuration Configuration, logger log.Logger) (p Pipeline, err error) {
	p.log = logger
	p.log.Println("Initializing new pipeline")

	err = p.setupInputs(configuration.Input)
	if err != nil {
		return
	}

	err = p.setupFilters(configuration.Filter)
	if err != nil {
		return
	}

	err = p.setupOutput(configuration.Output)
	if err != nil {
		return
	}

	return
}

func (p *Pipeline) setupInputs(inputsList []InputPlugin) (err error) {
	p.log.Printf("Total inputs found: %d", len(inputsList))

	p.InputStream = make(chan structs.Message, CHAIN_SIZE)

	for _, v := range inputsList {
		switch(v.Plugin) {
		case "syslog":
			syslogPlugin, err := inputs.NewSyslog(v.Options, p.log)

			if err != nil {
				return err
			}

			p.Inputs = append(p.Inputs, syslogPlugin)
		}
	}

	return
}

func (p *Pipeline) setupFilters(filtersList []FilterPlugin) (err error) {
	p.log.Printf("Total filters available: %d", len(filtersList))

	for _, v := range filtersList {
		var filterPlugin structs.Filter

		switch v.Plugin {
		case "split":
			filterPlugin, err = filters.NewSplitFilter(v.Options, p.log)
			break
		case "rename":
			filterPlugin, err = filters.NewRenameFilter(v.Options, p.log)
			break
		case "regexp":
			filterPlugin, err = filters.NewRegexpFilter(v.Options, p.log)
			break
		case "regexp_remove":
			filterPlugin, err = filters.NewRegexpRemoveFilter(v.Options, p.log)
			break
		case "regexp_match":
			filterPlugin, err = filters.NewRegexpMatchFilter(v.Options, p.log)
			break
		case "payload_assert":
			filterPlugin, err = filters.NewPayloadAssertFilter(v.Options, p.log)
			break
		case "geoip":
			filterPlugin, err = filters.NewGeoipFilter(v.Options, p.log)
			break
		case "set":
			filterPlugin, err = filters.NewSetFilter(v.Options, p.log)
			break
		case "time_format":
			filterPlugin, err = filters.NewTimeFormatFilter(v.Options, p.log)
			break
		default:
			return errors.New("plugin not found: " + v.Plugin)
		}

		if err != nil {
			return err
		}

		filterPlugin.SetField(v.Field)
		p.Filters = append(p.Filters, filterPlugin)

	}

	p.log.Printf("Configured %d top level filters", len(p.Filters))

	return nil
}

/**
 * Setup output plugins
 */
func (p *Pipeline) setupOutput(outputsList []OutputPlugin) (err error) {
	p.OutputStream = make(chan structs.Message, CHAIN_SIZE)

	for _, v := range outputsList {
		var outputPlugin structs.Output

		switch v.Plugin {
		case "stdout":
			outputPlugin, err = outputs.NewStdoutOutput(v.Options, p.log)
			break
		case "stat":
			outputPlugin, err = outputs.NewStatOutput(v.Options, p.log)
			break
		case "clickhouse":
			outputPlugin, err = outputs.NewClickhouseOutput(v.Options, p.log)
			break
		default:
			return errors.New("failed to initialize output: " + v.Plugin)
		}

		if err != nil {
			return err
		}
		p.Outputs = append(p.Outputs, outputPlugin)
	}

	return nil
}

/**
 * Proceed pipeline stuff
 */
func (p *Pipeline) Run() (err error) {
	var subChains []chan structs.Message

	for i, input := range p.Inputs {
		p.log.Printf("Activating input ID#%d", i)
		go input.AcceptTo(p.InputStream)
	}

	if len(p.Filters) == 0 {
		p.log.Printf("No filters configured! Linking output and input plugins directly")

		p.OutputStream = p.InputStream
	} else {
		// we have some filters available, let's link them one by one

		if len(p.Filters) > 1 {
			subChainsNum := len(p.Filters) - 1

			for i := 0; i < subChainsNum; i += 1 {
				p.log.Printf("Creating sub-chain for #%d", i)
				chain := make(chan structs.Message, CHAIN_SIZE)
				subChains = append(subChains, chain)
			}
		}

		for i, filter := range p.Filters {
			p.log.Printf("Activating filter #%d", i)

			if i == 0 && len(p.Filters) == 1 {
				p.log.Print("Single filter mode activated")
				go filter.Proceed(p.InputStream, p.OutputStream)
			} else if i == 0 && len(p.Filters) > 1 {
				p.log.Printf("First filter to chain pipeline activated")
				go filter.Proceed(p.InputStream, subChains[i])
			} else if i > 0 && i == len(p.Filters)-1 {
				p.log.Printf("Last filter in chain, #%d", i)
				go filter.Proceed(subChains[i-1], p.OutputStream)
			} else if i > 0 && i != len(p.Filters)-1 {
				p.log.Printf("Middle filter in chain, #%d", i)
				go filter.Proceed(subChains[i-1], subChains[i])
			}
		}
	}

	for i, output := range p.Outputs {
		p.log.Printf("Activating output ID#%d", i)
		go output.ReadFrom(p.OutputStream)
	}

	for {
		time.Sleep(5 * time.Second)
		p.log.Printf("Input queue: %d, Output queue: %d", len(p.InputStream), len(p.OutputStream))
		for i, channel := range subChains {
			p.log.Printf("Sub-channel #%d size is %d", i, len(channel))
		}
	}
}
