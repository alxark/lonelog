package app

type InputPlugin struct {
	Name    string `hcl:",label"`
	Plugin  string `hcl:"plugin"`
	Threads int    `hcl:"threads,optional"`
	Options struct {
		Data map[string]string `hcl:",remain"`
	} `hcl:"options,block"`
}

type FilterPlugin struct {
	Name            string `hcl:",label"`
	Threads         int    `hcl:"threads,optional"`
	ServiceInterval int    `hcl:"service_interval,optional"`
	Plugin          string `hcl:"plugin"`
	Field           string `hcl:"field,optional"`
	Queue           int    `hcl:"queue,optional"`
	Debug           bool   `hcl:"debug,optional"`
	Options         struct {
		Data map[string]string `hcl:",remain"`
	} `hcl:"options,block"`
	Args []map[string]string `hcl:"arg,optional"`
}

type OutputPlugin struct {
	Name    string `hcl:",label"`
	Plugin  string `hcl:"plugin"`
	Threads int    `hcl:"threads,optional"`
	Debug   bool   `hcl:"debug,optional"`
	Options struct {
		Data map[string]string `hcl:",remain"`
	} `hcl:"options,block"`
}
