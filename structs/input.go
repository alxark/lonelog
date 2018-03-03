package structs

type Input interface {
	AcceptTo(chan Message) error
}
