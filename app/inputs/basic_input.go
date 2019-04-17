package inputs

type BasicInput struct {
}

func (bi *BasicInput) IsMultiThread() bool {
	return true
}

func (bi *BasicInput) IsActive(value string) bool {
	return value == "1" || value == "on" || value == "yes" || value == "true"
}
