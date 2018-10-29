package master

import (
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	schedulerPort   = "2002"
	connDialTimeout = time.Second
)

type EventType int

const (
	SCH_ACK EventType = iota
	SCH_RDY
	SCH_RES
	SCH_JOB
	SCH_JOB_END
)

type Event struct {
	Type    EventType
	Payload interface{}
}

type Connbox struct {
	toMaster   chan<- Event // write only channel
	fromMaster <-chan Event // read only channel

	connections     []*Socket
	connectionsLock *sync.Mutex
	fromSocket      chan Event

	close chan<- bool
	ready chan bool
}

func newConnBox(fromMaster, toMaster chan Event) *Connbox {
	cb := &Connbox{}
	cb.toMaster = toMaster
	cb.fromMaster = fromMaster

	cb.connections = make([]*Socket, 0)
	cb.connectionsLock = &sync.Mutex{}

	cb.close = make(chan bool, 1)
	cb.fromSocket = make(chan Event)

	return cb
}

func (cb *Connbox) startPassive() error {
	masterLog.Info("Starting Connbox in passive mode on this master")
	// Listen on all interfaces
	ln, err := net.Listen("tcp", ":"+schedulerPort)
	if err != nil {
		masterLog.Error("Leader couldn't socket listen: " + err.Error())
		return err
	}

	go cb.eventLoop()

	// Accept connections on port
	for {
		fmt.Print("Awaiting connections from other masters\n")
		conn, err := ln.Accept()
		if err != nil {
			masterLog.Error("Leader couldn't socket accept: " + err.Error())
			continue
		}
		masterLog.Info("New connection with a follower stablished")
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
	s.fromConnbox = make(chan Event, 10)
	cb.connections = append(cb.connections, s)
	cb.connectionsLock.Unlock()
}

func (cb *Connbox) startActive(addr string) error {
	masterLog.Info("Starting Connbox in active mode on this master")

	conn, err := net.DialTimeout("tcp", addr, connDialTimeout)
	if err != nil {
		masterLog.Error("Couldn't connect to leader socket: " + err.Error())
		return err
	}
	fmt.Print("Master made contact with the leader\n")

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
			cb.toMaster <- event
		case event := <-cb.fromMaster:
			for _, c := range cb.connections {
				c.fromConnbox <- event
			}
		}
	}
}
