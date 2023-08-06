package structs

type Input interface {
	AcceptTo(chan Message, chan int) error
	IsMultiThread() bool
	Init() error
	SetName(string) error
	GetName() string
}
