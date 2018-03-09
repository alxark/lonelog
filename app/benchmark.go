package app

import (
	"log"
	"sync"
	"time"
	"math"
)

const (
	defaultBenchmarkChannelSize      = 8192
	defaultBenchmarkCounterThreshold = 100000
)

type Benchmark struct {
	log         log.Logger
	Channels    map[string]chan int
	Counters    map[string]BenchmarkCounter
	UpdateMutex sync.Mutex

	isRunning bool
}

type BenchmarkCounter struct {
	Name              string
	Processed         int
	RPS               float64
	LastRpsActivation time.Time
	LastRpsValue      int
	NextRpsValue      int
}

func NewBenchmark(logger log.Logger) (bm *Benchmark, err error) {
	bm = &Benchmark{}
	bm.log = logger
	bm.Channels = make(map[string]chan int)
	bm.Counters = make(map[string]BenchmarkCounter)
	bm.isRunning = false
	bm.UpdateMutex = sync.Mutex{}

	return bm, err
}

func (bm *Benchmark) NewChannel(pluginName string) (chan int) {
	if channel, ok := bm.Channels[pluginName]; ok {
		bm.log.Printf("Re-used channel for %s", pluginName)
		return channel
	}

	if bm.isRunning {
		bm.log.Fatal("trying to require new channel for %s while processing is already active", pluginName)
	}

	channel := make(chan int, defaultBenchmarkChannelSize)
	bm.Channels[pluginName] = channel
	bm.Counters[pluginName] = BenchmarkCounter{
		LastRpsActivation: time.Now(),
		NextRpsValue:      defaultBenchmarkCounterThreshold,
	}

	bm.log.Printf("Initialized new benchmark channel %s", pluginName)

	return channel
}

/**
 * Read data from benchmark channels and calculate stat
 */
func (bm *Benchmark) Process() {
	bm.isRunning = true

	bm.log.Printf("Benchmark processing started")
	var currentCounter BenchmarkCounter
	// used to calculate value difference between processed values
	var valueDiff float64
	// time diff between last RPS calculation and current time
	var timeDiff float64
	var now time.Time

	for {

		for channelName, channel := range bm.Channels {
			unprocessed := len(channel)
			if unprocessed == 0 {
				continue
			}

			currentCounter = bm.Counters[channelName]
			for i := 0; i < unprocessed; i += 1 {
				counter := <-channel
				currentCounter.Processed += counter
			}

			// we need to calculate RPS now
			if currentCounter.Processed >= currentCounter.NextRpsValue {
				now = time.Now()
				timeDiff = float64(now.Unix() - currentCounter.LastRpsActivation.Unix())
				valueDiff = float64(currentCounter.Processed - currentCounter.LastRpsValue)

				currentCounter.RPS = math.Floor(valueDiff / timeDiff)
				currentCounter.NextRpsValue = currentCounter.Processed + defaultBenchmarkCounterThreshold
				currentCounter.LastRpsActivation = now
			}

			bm.UpdateMutex.Lock()
			bm.Counters[channelName] = currentCounter
			bm.UpdateMutex.Unlock()
		}
	}
}

/**
 * Get current benchmark status
 */
func (bm *Benchmark) GetPluginBenchmark(pluginName string) (stat BenchmarkCounter) {
	if _, ok := bm.Counters[pluginName]; !ok {
		return BenchmarkCounter{}
	}

	bm.UpdateMutex.Lock()
	defer bm.UpdateMutex.Unlock()
	return bm.Counters[pluginName]
}
