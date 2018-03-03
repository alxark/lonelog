package app

type InputPlugin struct {
	Name    string
	Plugin  string
	Options map[string]string
}

type FilterPlugin struct {
	Name    string
	Plugin  string
	Field   string
	Options map[string]string
}

type OutputPlugin struct {
	Plugin  string
	Options map[string]string
}
