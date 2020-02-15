package filters

import (
	"log"
	"github.com/alxark/lonelog/internal/structs"
	"net"
	"encoding/json"
	"strconv"
	"errors"
)

const tcpTeeDumpStreamSize = 1024

type TcpTeeFilter struct {
	BasicFilter

	DumpStream chan structs.Message
	Port       int
	log        log.Logger
}

func NewTcpTeeFilter(options map[string]string, logger log.Logger) (f *TcpTeeFilter, err error) {
	f = &TcpTeeFilter{}
	if _, ok := options["port"]; !ok {
		return f, errors.New("no port specified")
	}

	f.Port, err = strconv.Atoi(options["port"])
	if err != nil {
		return f, errors.New("failed to setup TCP-TEE port, got: " + err.Error())
	}

	if f.Port < 0 || f.Port > 65535 {
		return f, errors.New("incorrect port, not in 1 - 65535 range: " + options["port"])
	}

	f.log = logger
	f.DumpStream = make(chan structs.Message, tcpTeeDumpStreamSize)

	return f, nil
}

func (f *TcpTeeFilter) IsThreadSafe() bool {
	return false
}

/**
 * Split content field by delimiter
 */
func (f *TcpTeeFilter) Proceed(input chan structs.Message, output chan structs.Message) (err error) {
	f.log.Print("TCP-TEE filter activated.")
	go f.NewListener()

	for msg := range input {
		output <- msg

		if len(f.DumpStream) < tcpTeeDumpStreamSize-1 {
			f.DumpStream <- msg
		}
	}

	f.log.Printf("Channel processing finished. Exiting")

	return
}

//
func (f *TcpTeeFilter) NewListener() {
	listenInterface := "0.0.0.0:" + strconv.Itoa(f.Port)

	l, err := net.Listen("tcp", listenInterface)
	if err != nil {
		f.log.Fatalf("Failed to start listener. Got: %s", err.Error())
	}

	defer l.Close()

	f.log.Printf("Started new TCP-TEE on %s", listenInterface)

	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			f.log.Printf("Error accepting: ", err.Error())
			continue
		}

		f.log.Printf("New TCP-TEE connection started: " + conn.RemoteAddr().String())

		// Handle connections in a new goroutine.
		go f.ProceedHandler(conn)
	}
}

func (f *TcpTeeFilter) ProceedHandler(conn net.Conn) {
	for msg := range f.DumpStream {
		info, err := json.Marshal(msg)
		if err != nil {
			f.log.Printf("failed to encode to JSON: %s", err.Error())
			continue
		}

		_, err = conn.Write(info)
		if err != nil {
			f.log.Printf("Connection to %s closed", conn.RemoteAddr().String())
			conn.Close()
			return
		}
		conn.Write([]byte("\n"))
	}
}
