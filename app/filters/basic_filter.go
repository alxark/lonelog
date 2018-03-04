package filters

type BasicFilter struct {
	Field           string
	Name            string
	Debug           bool
	ServiceInterval int
}

/**
 * Change debug mode
 */
func (bf *BasicFilter) SetDebug(debug bool) {
	bf.Debug = debug
}

/**
 * Set filter base name
 */
func (bf *BasicFilter) SetName(name string) (err error) {
	bf.Name = name
	return
}

func (bf *BasicFilter) GetName() string {
	return bf.Name
}

func (bf *BasicFilter) SetServiceInterval(interval int) {
	bf.ServiceInterval = interval
}

func (bf *BasicFilter) SetField(fieldName string) (err error) {
	if fieldName == "" {
		fieldName = "content"
	}

	bf.Field = fieldName
	return
}
