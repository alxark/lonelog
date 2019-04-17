package structs

type Output interface {
	ReadFrom(chan Message, map[string]string, chan int) error
	SetDebug(bool)
}