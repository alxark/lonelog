package app

type InputPlugin struct {
	Name    string
	Plugin  string
	Options map[string]string
}

type FilterPlugin struct {
	Name    string
	Threads int
	Plugin  string
	Field   string
	Queue   int
	Options map[string]string
}

type OutputPlugin struct {
	Plugin  string
	Options map[string]string
}
