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

	sock.fromSocket = make(chan Event, 10)

	return sock
}

func (s *Socket) launch() {
	go s.receiveLoop()
	go s.eventLoop()
}

func (s *Socket) send(event Event) {
	err := s.enc.Encode(event)
	if err != nil {
		masterLog.Error("Error on Socket sending message: " + err.Error())
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
			masterLog.Error("Error on Socket receiving message: " + err.Error())
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
