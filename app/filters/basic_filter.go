package filters

type BasicFilter struct {
	Field string
	Debug bool
}

/**
 * Change debug mode
 */
func (bf *BasicFilter) SetDebug(debug bool) (err error) {
	bf.Debug = debug
	return
}

func (bf *BasicFilter) SetField(fieldName string) (err error) {
	if fieldName == "" {
		fieldName = "content"
	}

	bf.Field = fieldName
	return
}
