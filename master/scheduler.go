package master

import (
	"encoding/gob"
	"fmt"
	"github.com/satori/go.uuid"
	"net"
	"os/exec"
	"strconv"
	"sync/atomic"
)

// ----------------------- Data type definitions ----------------------

type schAction int

const (
	SCH_ACK schAction = iota
	SCH_RDY
	SCH_RES
	SCH_JOB
	SCH_JOB_END
)

type msgScheduler struct {
	Action  schAction
	Payload interface{}
}

type scheduler struct {
	mastersAmount   *int32
	memTotal        uint64
	masterResources map[string]masterResourcesData
	// TODO: Erase the finished jobs from the table after a while?
	jobsTable  map[string]jobData
	leaderCh   []chan msgScheduler
	followerCh chan msgScheduler
}

// ----------------------------- Functions ----------------------------

func newScheduler(mem uint64) scheduler {
	sch := &scheduler{memTotal: mem}
	sch.masterResources = make(map[string]masterResourcesData)
	sch.jobsTable = make(map[string]jobData)
	sch.leaderCh = make([]chan msgScheduler, maxMasterAmount)
	for i := 0; i < maxMasterAmount; i++ {
		sch.leaderCh[i] = make(chan msgScheduler, 10)
	}
	sch.mastersAmount = new(int32)
	// Initialize own resources data
	ownResourcesData := masterResourcesData{myUuid, mem, mem}
	sch.masterResources[myUuid.String()] = ownResourcesData

	gob.Register(map[string]masterResourcesData{})
	gob.Register(jobData{})
	return *sch
}

func (sch *scheduler) openConnections(leaderIP string, imLeader bool, mastersAmount int32) {
	// TODO: Close all active connections via the channels?
	atomic.StoreInt32(sch.mastersAmount, mastersAmount)
	if imLeader {
		go sch.leaderScheduler()
	} else {
		go sch.followerScheduler(leaderIP + ":" + schedulerPort)
	}
}

func (sch *scheduler) spawnJob(job *jobData, imLeader bool) {
	masterLog.Info("Launching job on this node: " + job.JobName)
	jobFullName := job.JobName + "-" + job.JobId
	cmd := "sudo docker run --rm -m " + strconv.FormatUint(job.MemUsage, 10) + "m --name " + jobFullName + " " + job.ImageName
	fmt.Printf("%v\n", cmd)
	jobCmd := exec.Command("/bin/bash", "-c", cmd)
	err := jobCmd.Run()
	if err != nil {
		masterLog.Error("Couldn't successfully execute " + job.JobName + ":" + err.Error())
		job.JobStatus = JOB_FAILED
	} else {
		job.JobStatus = JOB_FINISHED
	}

	// If this was the leader, tell the followers the job has finished
	if imLeader {
		masterLog.Info("Job finished on the leader!!")
		for i := 0; int32(i) < (atomic.LoadInt32(sch.mastersAmount) - 1); i++ {
			sch.leaderCh[i] <- msgScheduler{Action: SCH_JOB_END, Payload: *job}
		}
		assignedData := sch.masterResources[job.AsignedMaster]
		assignedData.MemFree += job.MemUsage
		sch.masterResources[job.AsignedMaster] = assignedData
		sch.jobsTable[job.JobId] = *job
	}

}

// stopJob attempts to stop the container running the job
func (sch *scheduler) stopJob(jobID string) string {
	job := sch.jobsTable[jobID]
	jobFullName := job.JobName + "-" + job.JobId
	masterLog.Info("Stopping job on this node: " + jobFullName)

	cmd := "sudo docker stop " + jobFullName
	fmt.Printf("%v\n", cmd)
	jobCmd := exec.Command("/bin/bash", "-c", cmd)
	err := jobCmd.Run()
	if err != nil {
		masterLog.Error("Couldn't successfully stop job " + jobFullName + ":" + err.Error())
		return ""
	}
	return jobID
}

// launchJob selects a node with enough resources for launching the job,
// and forwards a msgScheduler with Action: SCH_JOB to all connections via
// the sch.leaderCh. It returns the JobId of the created job.
func (sch *scheduler) launchJob(jobName string, memUsage uint64, imageName string) string {
	assignedMaster := ""
	for uuid, data := range sch.masterResources {
		if data.MemFree >= memUsage {
			assignedMaster = uuid
			break
		}
	}
	if assignedMaster == "" {
		return "Error"
	}

	masterLog.Info("Ordering " + assignedMaster + " to launch a new job")
	job := jobData{JobName: jobName, MemUsage: memUsage, AsignedMaster: assignedMaster}
	job.ImageName = imageName
	job.JobId = uuid.NewV4().String()
	job.JobStatus = JOB_RUNNING
	for i := 0; int32(i) < (atomic.LoadInt32(sch.mastersAmount) - 1); i++ {
		sch.leaderCh[i] <- msgScheduler{Action: SCH_JOB, Payload: job}
	}

	assignedData := sch.masterResources[assignedMaster]
	assignedData.MemFree -= memUsage
	sch.masterResources[assignedMaster] = assignedData
	sch.jobsTable[job.JobId] = job
	if assignedMaster == myUuid.String() {
		go sch.spawnJob(&job, true)
	}
	return job.JobId
}

func (sch *scheduler) getMastersResources() map[string]masterResourcesData {
	return sch.masterResources
}

func (sch *scheduler) getJobsTable() map[string]jobData {
	return sch.jobsTable
}

// -------------------- Leader Scheduler Functions --------------------

func (sch *scheduler) leaderSchHandler(conn net.Conn, connId int32) {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	// First receive resources from the follower
	var mf masterResourcesData
	err := dec.Decode(&mf)
	if err != nil {
		masterLog.Error("Leader couldn't read from socket: " + err.Error())
		atomic.AddInt32(sch.mastersAmount, -1)
		return
	}
	sch.masterResources[mf.MasterUuid.String()] = mf

	go sch.leaderSchSender(enc, connId)
	go sch.leaderSchReceiver(dec, connId)
}

func (sch *scheduler) leaderSchSender(enc *gob.Encoder, connId int32) {
	for {
		msg := <-sch.leaderCh[connId]
		switch msg.Action {
		case SCH_RDY:
			// Send resources updates to followers
			fmt.Print("Sending master resources to followers\n")
			// Send all master resources to followers
			msgWithMemory := msgScheduler{Action: SCH_RES, Payload: sch.masterResources}
			err := enc.Encode(&msgWithMemory)
			if err != nil {
				masterLog.Error("Leader couldn't write to socket: " + err.Error())
				atomic.AddInt32(sch.mastersAmount, -1)
				return
			}
		case SCH_JOB_END:
			fallthrough
		case SCH_JOB:
			// Check the msgScheduler has a jobData and forward it to the follower
			_, ok := msg.Payload.(jobData)
			if !ok {
				masterLog.Error("Received something strange as job data")
				continue
			}
			err := enc.Encode(&msg)
			if err != nil {
				masterLog.Error("Leader couldn't write to socket: " + err.Error())
				atomic.AddInt32(sch.mastersAmount, -1)
				return
			}
		default:
			// Should never happen
			masterLog.Error("Received wrong sch action!")
		}
	}
}

func (sch *scheduler) leaderSchReceiver(dec *gob.Decoder, connId int32) {
	for {
		// Listen forever the follower connection
		msg := msgScheduler{}
		err := dec.Decode(&msg)
		if err != nil {
			masterLog.Error("Couldn't read from leader socket: " + err.Error())
			return
		}
		switch msg.Action {
		case SCH_JOB_END:
			job, ok := msg.Payload.(jobData)
			if !ok {
				masterLog.Error("Received something strange as job data")
				continue
			}
			// Forward the message to all connections via sch.leaderCh
			for i := 0; int32(i) < (atomic.LoadInt32(sch.mastersAmount) - 1); i++ {
				sch.leaderCh[i] <- msgScheduler{Action: SCH_JOB_END, Payload: job}
			}
			masterLog.Info("Job finished on this master with status: " + job.JobStatus.String())
			assignedData := sch.masterResources[job.AsignedMaster]
			assignedData.MemFree += job.MemUsage
			sch.masterResources[job.AsignedMaster] = assignedData
			sch.jobsTable[job.JobId] = job
		default:
			// Should never happen
			masterLog.Error("Received wrong sch action!")
		}
	}
}

func (sch *scheduler) leaderScheduler() {
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
			atomic.AddInt32(sch.mastersAmount, -1)
			continue
		}
		go sch.leaderSchHandler(conn, mastersConnected)
		mastersConnected += 1
		if (mastersConnected + 1) == atomic.LoadInt32(sch.mastersAmount) {
			masterLog.Info(fmt.Sprint(mastersConnected) + " followers made contact with me")
			// Send ready to all connections
			for i := 0; int32(i) < mastersConnected; i++ {
				sch.leaderCh[i] <- msgScheduler{Action: SCH_RDY}
			}
			return
		}
	}
}

// ------------------- Follower Scheduler Functions -------------------

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
			// If the job needs to be launched on my machine, then
			// REALLY launch the job. Update the masterResources table
			// and the jobsTable.
			job, ok := leaderMsg.Payload.(jobData)
			if !ok {
				masterLog.Error("Received something strange as job data")
				continue
			}
			masterLog.Info("Received new job data from leader!\n")
			assignedData := sch.masterResources[job.AsignedMaster]
			assignedData.MemFree -= job.MemUsage
			sch.masterResources[job.AsignedMaster] = assignedData
			sch.jobsTable[job.JobId] = job
			if job.AsignedMaster == myUuid.String() {
				go sch.followerJobSpawner(enc, job)
			}
		case SCH_JOB_END:
			job, ok := leaderMsg.Payload.(jobData)
			if !ok {
				masterLog.Error("Received something strange as job data")
				continue
			}
			assignedData := sch.masterResources[job.AsignedMaster]
			assignedData.MemFree += job.MemUsage
			sch.masterResources[job.AsignedMaster] = assignedData
			sch.jobsTable[job.JobId] = job
		default:
			// Should never happen
			masterLog.Error("Received wrong sch action!")
		}
	}
}

func (sch *scheduler) followerJobSpawner(enc *gob.Encoder, job jobData) {
	msg := msgScheduler{Action: SCH_JOB_END}
	sch.spawnJob(&job, false)
	masterLog.Info("Job finished on this follower master!")
	msg.Payload = job
	err := enc.Encode(&msg)
	if err != nil {
		masterLog.Error("Couldn't write to leader socket: " + err.Error())
	}
}
