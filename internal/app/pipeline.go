package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/alxark/lonelog/internal/app/filters"
	"github.com/alxark/lonelog/internal/app/inputs"
	"github.com/alxark/lonelog/internal/app/outputs"
	"github.com/alxark/lonelog/internal/structs"
	"log"
	"strconv"
	"time"
)

const (
	defaultChannelSize = 8192

	// how oftern plugins should do their internal service procedures (stat calculation,
	// data optimization and other such stuff)
	defaultServiceInterval = 65535

	// default offset between output service start. splay is used to separate
	// launch of different threads and prevent too many write operations in one moment
	defaultOutputSplay = 5

	// how often to collect service statistics about inner queues
	defaultStatInterval = 30
)

type Pipeline struct {
	log          log.Logger
	Inputs       []structs.Input
	Outputs      []structs.Output
	Filters      []structs.Filter
	InputStream  chan structs.Message
	OutputStream chan structs.Message

	// list of filter channels
	SubChains          []chan structs.Message
	SubChainsNames     []string
	ThreadsCount       []int
	OutputThreadsCount []int

	OutputSplay  int
	StatInterval int
	Bench        *Benchmark

	Status PipelineStatus
}

func NewPipeline(configuration Configuration, logger log.Logger) (p Pipeline, err error) {
	p.log = logger
	p.log.Println("Initializing new pipeline")
	p.Bench, _ = NewBenchmark(p.log)

	inputStreamSize := defaultChannelSize
	if configuration.In.Queue > 0 {
		inputStreamSize = configuration.In.Queue
	}
	p.log.Printf("Initialized input queue size: %d", inputStreamSize)
	p.InputStream = make(chan structs.Message, inputStreamSize)

	err = p.setupInputs(configuration.In.Input)
	if err != nil {
		return
	}

	err = p.setupFilters(configuration.Filter)
	if err != nil {
		return
	}

	outputStreamSize := defaultChannelSize
	if configuration.Out.Queue > 0 {
		outputStreamSize = configuration.Out.Queue
	}
	p.log.Printf("Initialized output queue size: %d", outputStreamSize)
	p.OutputStream = make(chan structs.Message, outputStreamSize)

	err = p.setupOutput(configuration.Out.Output)
	if err != nil {
		return
	}

	if configuration.Global.StatInterval > 0 {
		p.StatInterval = configuration.Global.StatInterval
	} else {
		p.StatInterval = defaultStatInterval
	}

	if configuration.Global.OutputSplay > 0 {
		p.OutputSplay = configuration.Global.OutputSplay
	} else {
		p.OutputSplay = defaultOutputSplay
	}

	return
}

func (p *Pipeline) setupInputs(inputsList []InputPlugin) (err error) {
	p.log.Printf("Total inputs found: %d", len(inputsList))

	for _, v := range inputsList {
		p.log.Printf("processing input %s", v.Name)

		var inputPlugin structs.Input
		switch v.Plugin {
		case "syslog":
			inputPlugin, err = inputs.NewSyslog(v.Options.Data, p.log)
			break
		case "redis":
			inputPlugin, err = inputs.NewRedisInput(v.Options.Data, p.log)
			break
		}

		if err != nil {
			return err
		}

		inputPlugin.SetName(v.Name)

		if inputPlugin.Init() != nil {
			return errors.New("failed to initialize input plugin: " + err.Error())
		}

		if v.Threads > 1 {
			if inputPlugin.IsMultiThread() {
				for i := 0; i < v.Threads; i += 1 {
					p.Inputs = append(p.Inputs, inputPlugin)
				}
			} else {
				p.log.Printf("multi-threaded mode is not support for %s", v.Plugin)
				p.Inputs = append(p.Inputs, inputPlugin)
			}
		} else {
			p.Inputs = append(p.Inputs, inputPlugin)
		}
	}

	return
}

func (p *Pipeline) setupFilters(filtersList []FilterPlugin) (err error) {
	p.log.Printf("Total filters available: %d", len(filtersList))

	for i, v := range filtersList {
		var filterPlugin structs.Filter

		switch v.Plugin {
		case "split":
			filterPlugin, err = filters.NewSplitFilter(v.Options.Data, p.log)
			break
		case "substring":
			filterPlugin, err = filters.NewSubstringFilter(v.Options.Data, p.log)
			break
		case "rename":
			filterPlugin, err = filters.NewRenameFilter(v.Options.Data, p.log)
			break
		case "regexp":
			filterPlugin, err = filters.NewRegexpFilter(v.Options.Data, p.log)
			break
		case "regexp_remove":
			filterPlugin, err = filters.NewRegexpRemoveFilter(v.Options.Data, p.log)
			break
		case "regexp_classify":
			filterPlugin, err = filters.NewRegexpClassifyFilter(v.Options.Data, p.log)
			break
		case "regexp_match":
			filterPlugin, err = filters.NewRegexpMatchFilter(v.Options.Data, p.log)
			break
		case "payload_assert":
			filterPlugin, err = filters.NewPayloadAssertFilter(v.Options.Data, p.log)
			break
		case "payload_equal":
			filterPlugin, err = filters.NewPayloadEqualFilter(v.Options.Data, p.log)
			break
		case "payload_dump":
			filterPlugin, err = filters.NewPayloadDumpFilter(v.Options.Data, p.log)
			break
		case "geoip":
			filterPlugin, err = filters.NewGeoipFilter(v.Options.Data, p.log)
			break
		case "set":
			filterPlugin, err = filters.NewSetFilter(v.Options.Data, p.log)
			break
		case "time_format":
			filterPlugin, err = filters.NewTimeFormatFilter(v.Options.Data, p.log)
			break
		case "substr_contains":
			filterPlugin, err = filters.NewSubstrContainsFilter(v.Options.Data, p.log)
			break
		case "copy":
			filterPlugin, err = filters.NewCopyFilter(v.Options.Data, p.log)
			break
		case "tcp_tee":
			filterPlugin, err = filters.NewTcpTeeFilter(v.Options.Data, p.log)
			break
		case "web_rpc":
			filterPlugin, err = filters.NewWebRpcFilter(v.Options.Data, p.log)
			break
		default:
			return errors.New(fmt.Sprintf("plugin #%d not found: %s", i, v.Plugin))
		}

		if err != nil {
			return err
		}

		if v.Name == "" {
			v.Name = fmt.Sprintf("Filter #%d", i)
		}
		filterPlugin.SetName(v.Name)
		filterPlugin.SetField(v.Field)

		if v.ServiceInterval == 0 {
			v.ServiceInterval = defaultServiceInterval
		}

		if v.Debug {
			filterPlugin.SetDebug(true)
			p.log.Printf("Activated debug mode for %s", v.Name)
		}

		filterPlugin.SetServiceInterval(v.ServiceInterval)
		p.Filters = append(p.Filters, filterPlugin)

		queueSize := defaultChannelSize
		if v.Queue > 0 {
			queueSize = v.Queue
		}

		p.log.Printf("Creating sub-chain for #%d, size: %d", i, queueSize)
		chain := make(chan structs.Message, queueSize)
		p.SubChains = append(p.SubChains, chain)
		p.SubChainsNames = append(p.SubChainsNames, v.Name)

		if v.Threads == 0 {
			v.Threads = 1
		}
		p.ThreadsCount = append(p.ThreadsCount, v.Threads)
	}

	p.log.Printf("Configured %d top level filters", len(p.Filters))

	return nil
}

/**
 * Setup output plugins
 */
func (p *Pipeline) setupOutput(outputsList []OutputPlugin) (err error) {
	for _, v := range outputsList {
		var outputPlugin structs.Output

		switch v.Plugin {
		case "stdout":
			outputPlugin, err = outputs.NewStdoutOutput(v.Options.Data, p.log)
			break
		case "stat":
			outputPlugin, err = outputs.NewStatOutput(v.Options.Data, p.log)
			break
		case "clickhouse":
			outputPlugin, err = outputs.NewClickhouseOutput(v.Options.Data, p.log)
			break
		case "redis":
			outputPlugin, err = outputs.NewRedisOutput(v.Options.Data, p.log)
			break
		case "null":
			outputPlugin, err = outputs.NewNullOutput(v.Options.Data, p.log)
			break
		default:
			return errors.New("failed to initialize output: " + v.Plugin)
		}

		if err != nil {
			return err
		}
		threadsCount := 1
		if v.Threads > 0 {
			threadsCount = v.Threads
		}

		if v.Debug {
			outputPlugin.SetDebug(true)
			p.log.Printf("Activated debug mode for output plugin")
		}

		p.Outputs = append(p.Outputs, outputPlugin)
		p.OutputThreadsCount = append(p.OutputThreadsCount, threadsCount)
	}

	p.log.Printf("Output initialization finished")

	return nil
}

/**
 * Proceed pipeline stuff
 */
func (p *Pipeline) Run() (err error) {
	ctx := context.Background()

	p.log.Printf("Starting pipeline processing")

	for i, input := range p.Inputs {
		p.log.Printf("Activating input ID#%d", i)
		go input.AcceptTo(p.InputStream, p.Bench.NewChannel("input"))
	}

	if len(p.Filters) == 0 {
		p.log.Printf("No filters configured! Linking output and input plugins directly")

		p.OutputStream = p.InputStream
	} else {
		// we have some filters available, let's link them one by one

		for i, filter := range p.Filters {
			p.log.Printf("Activating filter #%d", i)

			if err := filter.Init(); err != nil {
				p.log.Fatal("failed to initialize filter: " + err.Error())
				return err
			}

			for thread := 0; thread < p.ThreadsCount[i]; thread += 1 {
				p.log.Printf("Activating thread %d", thread)

				if i == 0 && len(p.Filters) == 1 {
					p.log.Print("Single filter mode activated")
					go filter.Proceed(ctx, p.InputStream, p.OutputStream)
				} else if i == 0 && len(p.Filters) > 1 {
					p.log.Printf("First filter to chain pipeline activated")
					go filter.Proceed(ctx, p.InputStream, p.SubChains[i])
				} else if i > 0 && i == len(p.Filters)-1 {
					p.log.Printf("Last filter in chain, #%d", i)
					go filter.Proceed(ctx, p.SubChains[i-1], p.OutputStream)
				} else if i > 0 && i != len(p.Filters)-1 {
					p.log.Printf("Middle filter in chain, #%d", i)
					go filter.Proceed(ctx, p.SubChains[i-1], p.SubChains[i])
				}
			}
		}
	}

	for i, output := range p.Outputs {
		for j := 0; j < p.OutputThreadsCount[i]; j += 1 {
			options := make(map[string]string)

			p.log.Printf("Activating output ID#%d, thread %d", i, j)

			options["THREAD"] = strconv.Itoa(j)

			go output.ReadFrom(p.OutputStream, options, p.Bench.NewChannel("output"))
			p.log.Printf("Waiting for %d seconds before next activation", p.OutputSplay)
			time.Sleep(time.Duration(p.OutputSplay) * time.Second)
		}
	}
	go p.Bench.Process()

	p.log.Printf("Starting stat check, duration: %d", p.StatInterval)

	for {
		time.Sleep(time.Duration(p.StatInterval) * time.Second)
		p.Status = p.GetStatus()
	}
}

func (p *Pipeline) GetStatus() PipelineStatus {
	currentStatus := PipelineStatus{}
	currentStatus.In = PluginStatus{
		Name:      "input",
		Size:      len(p.InputStream),
		Benchmark: p.Bench.GetPluginBenchmark("input"),
	}

	currentStatus.Out = PluginStatus{
		Name:      "output",
		Size:      len(p.OutputStream),
		Benchmark: p.Bench.GetPluginBenchmark("output"),
	}

	var filterStatuses []PluginStatus

	for i, channel := range p.SubChains {
		filterStatuses = append(filterStatuses, PluginStatus{Name: "filter-" + p.SubChainsNames[i], Size: len(channel)})
	}

	currentStatus.Filters = filterStatuses
	return currentStatus
}
