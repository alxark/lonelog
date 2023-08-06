package structs

import "context"

type Filter interface {
	Proceed(ctx context.Context, input chan Message, output chan Message) error
	SetField(fieldName string) error
	SetName(filterName string) error
	GetName() string
	SetServiceInterval(int)
	SetDebug(bool)
	Init() error
}
