package connbox

import (
	"encoding/gob"
	"github.com/juanfresia/claud/logger"
	"io"
	"net"
)


type Payload struct {
	Payload interface{}
}

type Socket struct {
	ip          net.Addr
	toConnbox   chan<- Message
	fromConnbox chan Message

	close       chan bool
	closeSignal chan net.Addr

	fromSocket chan Payload

	enc *gob.Encoder
	dec *gob.Decoder
}

func newSocket(conn net.Conn) *Socket {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	sock := &Socket{}
	sock.enc = enc
	sock.dec = dec

	sock.ip = conn.RemoteAddr()
	sock.fromSocket = make(chan Payload, 10)
	sock.close = make(chan bool, 1)

	return sock
}

func (s *Socket) launch() {
	go s.receiveLoop()
	go s.eventLoop()
}

func (s *Socket) send(event Payload) {
	err := s.enc.Encode(event)
	if err != nil {
		logger.Logger.Error("Error on Socket sending Payload: " + err.Error())
		s.close <- true
	}
}

func (s *Socket) receiveLoop() {
	var event Payload
	var err error
	for {
		// Receive
		err = s.dec.Decode(&event)

		if err != nil {
			if err == io.EOF {
				// Exit
				s.close <- true
				break
			}
			logger.Logger.Error("Error on Socket receiving Payload: " + err.Error())
		} else {
			s.fromSocket <- event
		}
	}
	logger.Logger.Info("The reading routing of the socket is now closed")
}

func (s *Socket) eventLoop() {
	for {
		select {
		// Received a Payload from connbox
		case event := <-s.fromConnbox:
			s.send(Payload{event.Msg})
		case event := <-s.fromSocket:
			s.toConnbox <- Message{s.ip, event.Payload}
		case <-s.close:
			// Send close to connbox
			logger.Logger.Info("A socket has been closed")
			s.closeSignal <- s.ip
			return
		}
	}
}
