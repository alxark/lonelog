package structs

type Output interface {
	ReadFrom(chan Message) error
}
