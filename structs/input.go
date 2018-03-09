package structs

type Input interface {
	AcceptTo(chan Message, chan int) error
	IsMultiThread() bool
}
