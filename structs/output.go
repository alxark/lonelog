package structs

type Output interface {
	ReadFrom(chan Message, map[string]string) error
}
