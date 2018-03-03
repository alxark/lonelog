package structs

type Filter interface {
	Proceed(input chan Message, output chan Message) error
	SetField(fieldName string) error
}
