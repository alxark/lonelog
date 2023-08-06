package filters

import (
	"github.com/alxark/lonelog/internal/structs"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

type BasicFilter struct {
	Field           string
	Name            string
	Debug           bool
	ServiceInterval int
}

var inputMetrics = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "ll",
	Subsystem: "filters",
	Name:      "input",
	Help:      "Total number of input messages",
}, []string{"filter"})

var outputMetrics = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "ll",
	Subsystem: "filters",
	Name:      "output",
	Help:      "Total number of output messages",
}, []string{"filter"})

var filterRegisterOnce = sync.Once{}

func (bf *BasicFilter) SetDebug(debug bool) {
	bf.Debug = debug
}

func (bf *BasicFilter) IsThreadSafe() bool {
	return true
}

// Init - filter initialization
func (bf *BasicFilter) Init() error {
	filterRegisterOnce.Do(func() {
		prometheus.MustRegister(inputMetrics)
		prometheus.MustRegister(outputMetrics)
	})

	return nil
}

func (bf *BasicFilter) SetName(name string) (err error) {
	bf.Name = name
	return
}

func (bf *BasicFilter) GetName() string {
	return bf.Name
}

func (bf *BasicFilter) SetServiceInterval(interval int) {
	bf.ServiceInterval = interval
}

func (bf *BasicFilter) SetField(fieldName string) (err error) {
	if fieldName == "" {
		fieldName = "content"
	}

	bf.Field = fieldName
	return
}

func (bf *BasicFilter) ReadMessage(input chan structs.Message) (msg structs.Message, ok bool) {
	msg, ok = <-input

	inputMetrics.WithLabelValues(bf.GetName()).Inc()

	return
}

func (bf *BasicFilter) WriteMessage(output chan structs.Message, msg structs.Message) error {
	output <- msg

	outputMetrics.WithLabelValues(bf.GetName()).Inc()

	return nil
}
