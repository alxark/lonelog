package structs

import "time"

type Message struct {
	AcceptTime time.Time
	Hostname   string
	Tags       []string
	Content    string
	Payload    map[string]string
}
