package structs

type Filter interface {
	Proceed(input chan Message, output chan Message) error
	SetField(fieldName string) error
	SetName(filterName string) error
	GetName() string
	SetServiceInterval(int)
	SetDebug(bool)
}
