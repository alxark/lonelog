package inputs


type BasicInput struct {
}

func (bi *BasicInput) IsMultiThread() bool {
	return true
}