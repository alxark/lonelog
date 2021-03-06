package app

type InputPlugin struct {
	Name    string
	Plugin  string
	Threads int
	Options map[string]string
}

type FilterPlugin struct {
	Name            string
	Threads         int
	ServiceInterval int                 `hcl:"service_interval"`
	Plugin          string
	Field           string
	Queue           int
	Debug           bool
	Options         map[string]string
	Args            []map[string]string `hcl:"arg"`
}

type OutputPlugin struct {
	Plugin  string
	Threads int
	Debug   bool
	Options map[string]string
}
