package connbox

import (
	"encoding/gob"
	"github.com/juanfresia/claud/logger"
	"io"
	"net"
)

type Socket struct {
	ip          string
	toConnbox   chan<- Message
	fromConnbox chan Message

	fromSocket chan Message

	enc *gob.Encoder
	dec *gob.Decoder
}

func newSocket(conn net.Conn) *Socket {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	sock := &Socket{}
	sock.enc = enc
	sock.dec = dec

	sock.fromSocket = make(chan Message, 10)

	return sock
}

func (s *Socket) launch() {
	go s.receiveLoop()
	go s.eventLoop()
}

func (s *Socket) send(event Message) {
	err := s.enc.Encode(event)
	if err != nil {
		//logger.Logger.Error("Error on Socket sending message: " + err.Error())
	}
}

func (s *Socket) receiveLoop() {
	var event Message
	var err error
	for {
		// Receive
		err = s.dec.Decode(&event)

		if err != nil {
			if err == io.EOF {
				// Exit
				break
			}
			logger.Logger.Error("Error on Socket receiving message: " + err.Error())
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
