package master

import (
	"encoding/gob"
	"fmt"
	"github.com/satori/go.uuid"
	"net"
	"sync/atomic"
	"time"
)

// ----------------------- Data type definitions ----------------------

const (
	schedulerPort   = "2002"
	connDialTimeout = time.Second
)

type masterResourcesData struct {
	MasterUuid uuid.UUID
	MemFree    uint64
	MemTotal   uint64
}

type scheduler struct {
	mastersAmount   int32
	memTotal        uint64
	masterResources map[string]masterResourcesData
}

// ----------------------------- Functions ----------------------------

func newScheduler(mem uint64) scheduler {
	sch := &scheduler{memTotal: mem}
	sch.masterResources = make(map[string]masterResourcesData)
	// Initialize own resources data
	ownResourcesData := masterResourcesData{myUuid, mem, mem}
	sch.masterResources[myUuid.String()] = ownResourcesData

	return *sch
}

func (sch *scheduler) leaderSchHandler(conn net.Conn) {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)
	for {
		// Will listen for message to process ending in newline (\n)
		var mf masterResourcesData
		err := dec.Decode(&mf)
		if err != nil {
			masterLog.Error("Couldn't read from socket: " + err.Error())
			atomic.AddInt32(&sch.mastersAmount, -1)
			return
		}
		sch.masterResources[mf.MasterUuid.String()] = mf
		// Reply the memory data back
		err = enc.Encode(&sch.masterResources)
		if err != nil {
			masterLog.Error("Couldn't write to socket: " + err.Error())
			atomic.AddInt32(&sch.mastersAmount, -1)
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
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)
	for {
		mf := &masterResourcesData{MasterUuid: myUuid, MemFree: sch.memTotal, MemTotal: sch.memTotal}
		// Send memory to leader scheduler
		err = enc.Encode(mf)
		if err != nil {
			masterLog.Error("Couldn't write to leader socket: " + err.Error())
			return
		}
		// Listen for reply
		err = dec.Decode(&sch.masterResources)
		if err != nil {
			masterLog.Error("Couldn't read from leader socket: " + err.Error())
			return
		}
		fmt.Print("Received data from leader scheduler\n")

		time.Sleep(time.Second * 18)
	}
}

func (sch *scheduler) openConnections(leaderIP string, imLeader bool, mastersAmount int32) {
	// TODO: Close all active connections via channels
	atomic.StoreInt32(&sch.mastersAmount, mastersAmount)
	if imLeader {
		go sch.leaderScheduler()
	} else {
		go sch.followerScheduler(leaderIP + ":" + schedulerPort)
	}
}

func (sch *scheduler) getMastersResources() map[string]masterResourcesData {
	return sch.masterResources
}
