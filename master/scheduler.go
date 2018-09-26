package master

import (
	"encoding/gob"
	"fmt"
	"net"
	"time"

	linuxproc "github.com/c9s/goprocinfo/linux"
)

// ----------------------- Data type definitions ----------------------

const (
	schedulerPort   = "2002"
	connDialTimeout = time.Second
)

type schMsg struct {
	MemFree  uint64
	MemTotal uint64
}

type scheduler struct {
	masterAddresses map[string]string
}

// ----------------------------- Functions ----------------------------

func newScheduler() scheduler {
	sch := &scheduler{}
	sch.masterAddresses = make(map[string]string)

	return *sch
}

func (sch *scheduler) readMemory() *schMsg {
	meminfo, err := linuxproc.ReadMemInfo("/proc/meminfo")
	if err != nil {
		masterLog.Error("Couldn't read memory info: " + err.Error())
	}
	msg := &schMsg{MemFree: meminfo.MemFree, MemTotal: meminfo.MemTotal}
	return msg
}

func (sch *scheduler) leaderSchHandler(conn net.Conn) {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)
	for {
		// Will listen for message to process ending in newline (\n)
		var mf schMsg
		err := dec.Decode(&mf)
		if err != nil {
			masterLog.Error("Couldn't read from socket: " + err.Error())
			return
		}
		fmt.Printf("Follower memory is: %v KB free (of %v KB)", mf.MemFree, mf.MemTotal)
		// Reply the hello back
		msg := "ACK\n"
		err = enc.Encode(&msg)
		if err != nil {
			masterLog.Error("Couldn't write to socket: " + err.Error())
			return
		}
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
		enc := gob.NewEncoder(conn)
		dec := gob.NewDecoder(conn)
		mf := sch.readMemory()
		// Send hello to leader scheduler
		err = enc.Encode(mf)
		if err != nil {
			masterLog.Error("Couldn't write to leader socket: " + err.Error())
			return
		}
		// Listen for reply
		var msg string
		err = dec.Decode(&msg)
		if err != nil {
			masterLog.Error("Couldn't read from leader socket: " + err.Error())
			return
		}
		fmt.Print("Message from leader scheduler: " + msg)
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
