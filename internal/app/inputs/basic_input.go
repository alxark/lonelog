package inputs

import (
	"github.com/alxark/lonelog/internal/structs"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

type BasicInput struct {
	Name string
}

var generatedMetrics = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "ll",
	Subsystem: "input",
	Name:      "generated",
	Help:      "Total number of output messages",
}, []string{"input"})

var generatedRegisterOnce = sync.Once{}

func (bi *BasicInput) IsMultiThread() bool {
	return true
}

func (bi *BasicInput) Init() error {
	generatedRegisterOnce.Do(func() {
		prometheus.MustRegister(generatedMetrics)
	})

	return nil
}

func (bi *BasicInput) IsActive(value string) bool {
	return value == "1" || value == "on" || value == "yes" || value == "true"
}

func (bi *BasicInput) SetName(name string) (err error) {
	bi.Name = name
	return
}

func (bi *BasicInput) GetName() string {
	return bi.Name
}

func (bi *BasicInput) WriteMessage(output chan structs.Message, msg structs.Message) error {
	output <- msg

	generatedMetrics.WithLabelValues(bi.GetName()).Inc()

	return nil
}
