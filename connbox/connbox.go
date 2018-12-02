package connbox

import (
	"encoding/gob"
	"fmt"
	"github.com/juanfresia/claud/logger"
	"net"
	"sync"
	"time"
)

type Message struct {
	Msg interface{}
}

const (
	connDialTimeout = time.Second
)

type Connbox struct {
	toNode   chan<- interface{} // write only channel
	fromNode <-chan interface{} // read only channel

	connections     []*Socket
	connectionsLock *sync.Mutex
	fromSocket      chan Message

	close chan<- bool
	ready chan bool
}

func Register(value interface{}) {
	gob.Register(value)
}

func NewConnBox(fromNode, toNode chan interface{}) *Connbox {
	cb := &Connbox{}
	cb.toNode = toNode
	cb.fromNode = fromNode

	cb.connections = make([]*Socket, 0)
	cb.connectionsLock = &sync.Mutex{}

	cb.close = make(chan bool, 1)
	cb.fromSocket = make(chan Message)

	return cb
}

func (cb *Connbox) StartPassive(leaderPort string) error {
	logger.Logger.Info("Starting Connbox in passive mode on this node")
	// Listen on all interfaces
	ln, err := net.Listen("tcp", ":"+leaderPort)
	if err != nil {
		logger.Logger.Error("Leader couldn't socket listen: " + err.Error())
		return err
	}

	go cb.eventLoop()

	// Accept connections on port
	for {
		fmt.Print("Awaiting connections from other nodes\n")
		conn, err := ln.Accept()
		if err != nil {
			logger.Logger.Error("Leader couldn't socket accept: " + err.Error())
			continue
		}
		logger.Logger.Info("New connection with a follower stablished")
		go cb.handleNewSocket(conn)
	}
}

func (cb *Connbox) handleNewSocket(conn net.Conn) {
	s := newSocket(conn)
	cb.addSocket(s)
	s.launch()
}

func (cb *Connbox) addSocket(s *Socket) {
	cb.connectionsLock.Lock()
	s.toConnbox = cb.fromSocket
	s.fromConnbox = make(chan Message, 10)
	cb.connections = append(cb.connections, s)
	cb.connectionsLock.Unlock()
}

func (cb *Connbox) StartActive(addr string) error {
	logger.Logger.Info("Starting Connbox in active mode on this node")

	conn, err := net.DialTimeout("tcp", addr, connDialTimeout)
	if err != nil {
		logger.Logger.Error("Couldn't connect to leader socket: " + err.Error())
		return err
	}
	fmt.Print("Node made contact with the leader\n")

	s := newSocket(conn)
	cb.addSocket(s)
	s.launch()

	go cb.eventLoop()
	return nil
}

func (cb *Connbox) eventLoop() {
	for {
		select {
		// Received a message from a peer
		case event := <-cb.fromSocket:
			cb.toNode <- event.Msg
		case event := <-cb.fromNode:
			for _, c := range cb.connections {
				c.fromConnbox <- Message{event}
			}
		}
	}
}
