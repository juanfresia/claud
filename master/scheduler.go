package master

import (
	"bufio"
	"fmt"
	"net"
	"time"
	//"github.com/satori/go.uuid"
)

// ----------------------- Data type definitions ----------------------

const (
	schedulerPort   = "2002"
	connDialTimeout = time.Second
)

type scheduler struct {
	masterAddresses map[string]string
}

// ----------------------------- Functions ----------------------------

func newScheduler() scheduler {
	sch := &scheduler{}
	sch.masterAddresses = make(map[string]string)

	return *sch
}

func (sch *scheduler) leaderSchHandler(conn net.Conn) {
	for {
		// Will listen for message to process ending in newline (\n)
		message, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			masterLog.Error("Couldn't read from socket: " + err.Error())
			conn.Close()
			return
		}
		fmt.Printf("Message received from follower: %s", string(message))
		// Reply the hello back
		fmt.Fprintf(conn, "Hello there follower\n")
	}
}

func (sch *scheduler) leaderScheduler() {
	// Listen on all interfaces
	ln, err := net.Listen("tcp", ":"+schedulerPort)
	if err != nil {
		masterLog.Error("Couldn't socket listen: " + err.Error())
		return
	}
	// Accept connections on port
	for {
		conn, err := ln.Accept()
		if err != nil {
			masterLog.Error("Couldn't socket accept: " + err.Error())
			return
		}
		go sch.leaderSchHandler(conn)
	}
}

func (sch *scheduler) followerScheduler(leaderSchAddr string) {
	conn, err := net.DialTimeout("tcp", leaderSchAddr, connDialTimeout)
	if err != nil {
		masterLog.Error("Couldn't connect to leader socket: " + err.Error())
		return
	}
	for {
		// Send hello to leader scheduler
		fmt.Fprintf(conn, "Hello leader\n")
		// Listen for reply
		message, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			masterLog.Error("Couldn't read from leader socket: " + err.Error())
			conn.Close()
			return
		}
		fmt.Print("Message from leader scheduler: " + message)
		time.Sleep(time.Second * 8)
	}
}

func (sch *scheduler) openConnections(leaderIP string, imLeader bool) {
	if imLeader {
		go sch.leaderScheduler()
	} else {
		go sch.followerScheduler(leaderIP + ":" + schedulerPort)
	}
}
