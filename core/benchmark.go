package core

import (
	"time"
	"log"
)

type Benchmark struct {
	log log.Logger
	BenchmarkStart time.Time
	BenchmarkIeration int
}

func NewBenchmark(logger log.Logger, period int) (be *Benchmark, err error) {
	be = &Benchmark{}
	be.log = logger
	be.BenchmarkStart = time.Now()

	return be, err
}

func (be *Benchmark) Increment() {
	be.BenchmarkIeration += 1
}