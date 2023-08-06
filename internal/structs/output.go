package structs

type Output interface {
	ReadFrom(chan Message, map[string]string, chan int) error

	// SetDebug - enable debugging
	SetDebug(bool)

	// Init - initialize filter, good place to put metrics initialization
	Init() error
}
