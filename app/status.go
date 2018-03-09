package app

type PluginStatus struct {
	Name string
	Size int
	Benchmark BenchmarkCounter
}

type PipelineStatus struct {
	In      PluginStatus
	Filters []PluginStatus
	Out     PluginStatus
}
