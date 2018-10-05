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

type schAction int

const (
	SCH_ACK schAction = iota
	SCH_RDY
	SCH_RES
	SCH_JOB
)

type msgScheduler struct {
	Action  schAction
	Payload interface{}
}

type scheduler struct {
	mastersAmount   int32
	memTotal        uint64
	masterResources map[string]masterResourcesData
	// TODO: Add a "jobs table" (job id vs master uuid)
	leaderCh   []chan msgScheduler
	followerCh chan msgScheduler
}

// ----------------------------- Functions ----------------------------

func newScheduler(mem uint64) scheduler {
	sch := &scheduler{memTotal: mem}
	sch.masterResources = make(map[string]masterResourcesData)
	// Initialize own resources data
	ownResourcesData := masterResourcesData{myUuid, mem, mem}
	sch.masterResources[myUuid.String()] = ownResourcesData

	gob.Register(map[string]masterResourcesData{})
	return *sch
}

func (sch *scheduler) leaderSchHandler(conn net.Conn, connId int32) {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	// First receive resources from the follower
	var mf masterResourcesData
	err := dec.Decode(&mf)
	if err != nil {
		masterLog.Error("Leader couldn't read from socket: " + err.Error())
		atomic.AddInt32(&sch.mastersAmount, -1)
		return
	}
	sch.masterResources[mf.MasterUuid.String()] = mf

	for {
		msg := <-sch.leaderCh[connId]
		switch msg.Action {
		case SCH_RDY:
			// Send resources updates to followers
			fmt.Print("Have to send resources updates to followers\n")
			// Send all master resources to followers
			msgWithMemory := msgScheduler{Action: SCH_RES, Payload: sch.masterResources}
			err = enc.Encode(&msgWithMemory)
			if err != nil {
				masterLog.Error("Leader couldn't write to socket: " + err.Error())
				atomic.AddInt32(&sch.mastersAmount, -1)
				return
			}
		case SCH_JOB:
			// TODO: Forward the msgScheduler to the follower
		default:
			// Should never happen
			masterLog.Error("Received wrong sch action!")
		}
	}

}

func (sch *scheduler) leaderScheduler() {
	sch.leaderCh = make([]chan msgScheduler, atomic.LoadInt32(&sch.mastersAmount))
	var mastersConnected int32 = 0
	// Listen on all interfaces
	ln, err := net.Listen("tcp", ":"+schedulerPort)
	if err != nil {
		masterLog.Error("Leader couldn't socket listen: " + err.Error())
		return
	}
	// Accept connections on port
	for {
		conn, err := ln.Accept()
		if err != nil {
			masterLog.Error("Leader couldn't socket accept: " + err.Error())
			atomic.AddInt32(&sch.mastersAmount, -1)
			continue
		}
		sch.leaderCh[mastersConnected] = make(chan msgScheduler, 10)
		go sch.leaderSchHandler(conn, mastersConnected)
		mastersConnected += 1
		if (mastersConnected + 1) == atomic.LoadInt32(&sch.mastersAmount) {
			masterLog.Info(fmt.Sprint(mastersConnected) + " followers made contact with me")
			// Send ready to all connections
			for i := 0; int32(i) < mastersConnected; i++ {
				sch.leaderCh[i] <- msgScheduler{Action: SCH_RDY}
			}
			return
		}
	}
}

func (sch *scheduler) followerScheduler(leaderSchAddr string) {
	sch.followerCh = make(chan msgScheduler, 10)
	conn, err := net.DialTimeout("tcp", leaderSchAddr, connDialTimeout)
	if err != nil {
		masterLog.Error("Couldn't connect to leader socket: " + err.Error())
		return
	}
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	// First send resources to the leader
	mf := &masterResourcesData{MasterUuid: myUuid, MemFree: sch.memTotal, MemTotal: sch.memTotal}
	err = enc.Encode(mf)
	if err != nil {
		masterLog.Error("Couldn't write to leader socket: " + err.Error())
		return
	}

	for {
		// Listen forever the leaders comands
		leaderMsg := msgScheduler{}
		err = dec.Decode(&leaderMsg)
		if err != nil {
			masterLog.Error("Couldn't read from leader socket: " + err.Error())
			return
		}
		switch leaderMsg.Action {
		case SCH_RES:
			// Update resources with the info from leader
			resources, ok := leaderMsg.Payload.(map[string]masterResourcesData)
			if !ok {
				masterLog.Error("Received something strange as master resources")
				continue
			}
			for uuid, data := range resources {
				sch.masterResources[uuid] = data
			}
			fmt.Print("Master resources updated with info from leader\n")
		case SCH_JOB:
			// TODO: If the job needs to be launched on my machine (check uuids),
			// then REALLY launch the job. Update the masterResources table and
			// the "jobs table".
		default:
			// Should never happen
			masterLog.Error("Received wrong sch action!")
		}

	}
}

func (sch *scheduler) openConnections(leaderIP string, imLeader bool, mastersAmount int32) {
	// TODO: Close all active connections via the channels
	atomic.StoreInt32(&sch.mastersAmount, mastersAmount)
	if imLeader {
		go sch.leaderScheduler()
	} else {
		go sch.followerScheduler(leaderIP + ":" + schedulerPort)
	}
}

// TODO: Select a node with enough resources for launching a job, make a
// msgScheduler with Action: SCH_JOB and forward it to all connections via
// the sch.leaderCh
func (sch *scheduler) launchJob() {

}

func (sch *scheduler) getMastersResources() map[string]masterResourcesData {
	return sch.masterResources
}
