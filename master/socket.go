package master

import (
	"encoding/gob"
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

	return sock
}

func (s *Socket) launch() {
	go s.receiveLoop()
	go s.eventLoop()
}

func (s *Socket) send(event Event) {
	// Send message thorugh socket
	err := s.enc.Encode(event)
	if err != nil {
		// Log something here
	}
}

func (s *Socket) receiveLoop() {
	var event Event
	var err error
	for {
		// Receive
		err = s.dec.Decode(&event)
		if err != nil {
			if err == io.EOF {
				// Exit
				break
			}
			// TODO: log error and continue
		} else {
			s.fromSocket <- event
		}
	}
}

func (s *Socket) eventLoop() {
	for {
		select {
		// Received a message from connbox
		case event := <-s.fromConnbox:
			s.send(event)
		case event := <-s.fromSocket:
			s.toConnbox <- event
		}
	}
}
