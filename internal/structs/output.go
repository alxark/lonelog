package structs

type Output interface {
	ReadFrom(chan Message, map[string]string, chan int) error

	// SetDebug - enable debugging
	SetDebug(bool)

	// Init - initialize filter, good place to put metrics initialization
	Init() error

	// SetName - set output name
	SetName(string)

	// GetName - get output name
	GetName() string
}
