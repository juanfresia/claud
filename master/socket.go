package master

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
)

type Socket struct {
	ip          string
	toConnbox   chan<- Event
	fromConnbox chan Event

	fromSocket chan Event

	enc *gob.Encoder
	dec *gob.Decoder
}

func newSocket(conn net.Conn) *Socket {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	sock := &Socket{}
	sock.enc = enc
	sock.dec = dec

	sock.fromSocket = make(chan Event, 10)

	return sock
}

func (s *Socket) launch() {
	go s.receiveLoop()
	go s.eventLoop()
}

func (s *Socket) send(event Event) {
	// Send message thorugh socket
	fmt.Print("Socket: sending some message\n")

	err := s.enc.Encode(event)
	if err != nil {
		fmt.Printf("Socket: error sending message: %s\n", err)
		// Log something here
	}
}

func (s *Socket) receiveLoop() {
	var event Event
	var err error
	for {
		// Receive
		fmt.Print("Socket: waiting for a message\n")
		err = s.dec.Decode(&event)
		fmt.Print("Socket: received something\n")

		if err != nil {
			if err == io.EOF {
				// Exit
				break
			}
			// TODO: log error and continue
		} else {
			fmt.Print("Socket: received something - sending to eventLoop\n")
			s.fromSocket <- event
		}
	}
}

func (s *Socket) eventLoop() {
	for {
		select {
		// Received a message from connbox
		case event := <-s.fromConnbox:
			fmt.Print("Socket: new event from connbox\n")
			s.send(event)
		case event := <-s.fromSocket:
			fmt.Print("Socket: new event from socket\n")
			s.toConnbox <- event
		}
	}
}
