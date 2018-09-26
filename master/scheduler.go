package master

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/satori/go.uuid"
)

// ----------------------- Data type definitions ----------------------

const (
	schedulerPort   = "2002"
	connDialTimeout = time.Second
)

type Scheduler struct {
	uuid            uuid.UUID
	masterAddresses map[string]string

	schedLog *log.Logger
}

// ----------------------------- Functions ----------------------------

func newScheduler(uuid uuid.UUID, kernelLog *log.Logger) Scheduler {
	sch := &Scheduler{uuid: uuid, schedLog: kernelLog}
	sch.masterAddresses = make(map[string]string)

	return *sch
}

func (sch *Scheduler) leaderSchHandler(conn net.Conn) {
	for {
		// Will listen for message to process ending in newline (\n)
		message, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			sch.schedLog.Println("ERROR: Couldn't read from socket: ", err.Error())
			conn.Close()
			return
		}
		fmt.Printf("Message received from follower: %s", string(message))
		// Reply the hello back
		fmt.Fprintf(conn, "Hello there follower\n")
	}
}

func (sch *Scheduler) leaderScheduler() {
	// Listen on all interfaces
	ln, err := net.Listen("tcp", ":"+schedulerPort)
	if err != nil {
		sch.schedLog.Println("ERROR: Couldn't socket listen: ", err.Error())
		return
	}
	// Accept connections on port
	for {
		conn, err := ln.Accept()
		if err != nil {
			sch.schedLog.Println("ERROR: Couldn't socket accept: ", err.Error())
			return
		}
		go sch.leaderSchHandler(conn)
	}
}

func (sch *Scheduler) followerScheduler(leaderSchAddr string) {
	conn, err := net.DialTimeout("tcp", leaderSchAddr, connDialTimeout)
	if err != nil {
		sch.schedLog.Println("ERROR: Couldn't connect to leader socket: ", err.Error())
		return
	}
	for {
		// Send hello to leader Scheduler
		fmt.Fprintf(conn, "Hello leader\n")
		// Listen for reply
		message, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			sch.schedLog.Println("ERROR: Couldn't read from leader socket: ", err.Error())
			conn.Close()
			return
		}
		fmt.Print("Message from leader scheduler: " + message)
		time.Sleep(time.Second * 8)
	}
}

func (sch *Scheduler) OpenConnections(leaderData *MasterData) {
	if leaderData.uuid == sch.uuid.String() {
		go sch.leaderScheduler()
	} else {
		leaderSchAddr := leaderData.addr.String()
		leaderSchAddr = leaderSchAddr[:strings.Index(leaderSchAddr, ":")+1] + schedulerPort
		go sch.followerScheduler(leaderSchAddr)
	}
}
