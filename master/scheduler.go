package master

import (
	"encoding/gob"
	"fmt"
	"github.com/satori/go.uuid"
	"os/exec"
	"strconv"
)

// ----------------------- Data type definitions ----------------------

type scheduler struct {
	mastersAmount   int32
	memTotal        uint64
	masterResources map[string]masterResourcesData

	// TODO: Erase the finished jobs from the table after a while?
	jobsTable map[string]jobData

	connbox     *Connbox
	toConnbox   chan Event
	fromConnbox chan Event
}

// ----------------------------- Functions ----------------------------

func newScheduler(mem uint64) scheduler {
	sch := &scheduler{memTotal: mem}
	sch.masterResources = make(map[string]masterResourcesData)
	sch.jobsTable = make(map[string]jobData)

	// Create buffered channels (TODO: dont hardcode those 10)
	sch.toConnbox = make(chan Event, 10)
	sch.fromConnbox = make(chan Event, 10)

	cb, err := newConnBox(sch.toConnbox, sch.fromConnbox)
	if err != nil {
		// TODO: do something
	}
	sch.connbox = cb

	sch.mastersAmount = 0
	// Initialize own resources data
	ownResourcesData := masterResourcesData{myUuid, mem, mem}

	sch.masterResources[myUuid.String()] = ownResourcesData

	// This setup is global (do any registration before launching connbox)
	gob.Register(map[string]masterResourcesData{})
	gob.Register(jobData{})
	return *sch
}

// TODO: rename this?
func (sch *scheduler) start(leaderIP string, imLeader bool, mastersAmount int32) {
	// TODO: Close all active connections via the channels?
	sch.mastersAmount = mastersAmount
	if imLeader {
		go sch.connbox.startPassive()
		sch.leaderScheduler()
	} else {
		go sch.connbox.startActive(leaderIP + ":" + schedulerPort)
		sch.followerScheduler()
	}
	go sch.eventLoop(imLeader)
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

		sch.toConnbox <- Event{Type: SCH_JOB_END, Payload: *job}

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
// the sch.toConnbox. It returns the JobId of the created job.
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

	sch.toConnbox <- Event{Type: SCH_JOB, Payload: job}

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

func (sch *scheduler) leaderScheduler() error {
	// Wait until all followers are connected
	// This is the leader, which is already connected
	var mastersConnected int32 = 1
	for {
		select {
		case <-sch.fromConnbox:
			// TODO: check message type is correct (SCH_RDY)
			mastersConnected += 1
			if mastersConnected == sch.mastersAmount {
				// Send resources updates to followers
				fmt.Print("Sending master resources to followers\n")
				// Send all master resources to followers
				sch.toConnbox <- Event{Type: SCH_RES, Payload: sch.masterResources}
				return nil
			}
		}
	}
}

// ------------------- Follower Scheduler Functions -------------------

func (sch *scheduler) followerScheduler() error {
	// Send resources to leader
	// First send resources to the leader
	mf := &masterResourcesData{MasterUuid: myUuid, MemFree: sch.memTotal, MemTotal: sch.memTotal}
	event := Event{Type: SCH_RDY, Payload: mf}
	sch.toConnbox <- event

	// Wait here for ready signal
	for range sch.fromConnbox {
		// TODO: assert event type is correct
		break
	}

	return nil
}

// This function handles events from peers
func (sch *scheduler) handleFollowerEvent(e Event) {
	switch e.Type {
	case SCH_RES:
		// Update resources with the info from leader
		resources, ok := e.Payload.(map[string]masterResourcesData)
		if !ok {
			masterLog.Error("Received something strange as master resources")
			return
		}
		for uuid, data := range resources {
			sch.masterResources[uuid] = data
		}
		fmt.Print("Master resources updated with info from leader\n")
	case SCH_JOB:
		// If the job needs to be launched on my machine, then
		// REALLY launch the job. Update the masterResources table
		// and the jobsTable.
		job, ok := e.Payload.(jobData)
		if !ok {
			masterLog.Error("Received something strange as job data")
			return
		}
		masterLog.Info("Received new job data from leader!\n")
		assignedData := sch.masterResources[job.AsignedMaster]
		assignedData.MemFree -= job.MemUsage
		sch.masterResources[job.AsignedMaster] = assignedData
		sch.jobsTable[job.JobId] = job
		if job.AsignedMaster == myUuid.String() {
			go sch.followerJobSpawner(job)
		}
	case SCH_JOB_END:
		job, ok := e.Payload.(jobData)
		if !ok {
			masterLog.Error("Received something strange as job data")
			return
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

func (sch *scheduler) handelLeaderEvent(e Event) {
	switch e.Type {
	case SCH_JOB_END:
		job, ok := e.Payload.(jobData)
		if !ok {
			masterLog.Error("Received something strange as job data")
			return
		}
		sch.toConnbox <- e
		masterLog.Info("Job finished on this master with status: " + job.JobStatus.String())
		assignedData := sch.masterResources[job.AsignedMaster]
		assignedData.MemFree += job.MemUsage
		sch.masterResources[job.AsignedMaster] = assignedData
		sch.jobsTable[job.JobId] = job
	case SCH_JOB:
		// Check the msgScheduler has a jobData and forward it to the follower
		_, ok := e.Payload.(jobData)
		if !ok {
			masterLog.Error("Received something strange as job data")
			return
		}
		sch.toConnbox <- e
	default:
		// Should never happen
		masterLog.Error("Received wrong sch action!")
	}
}

func (sch *scheduler) eventLoop(imLeader bool) {
	for {
		select {
		case msg := <-sch.fromConnbox:
			if imLeader {
				sch.handelLeaderEvent(msg)
			} else {
				sch.handleFollowerEvent(msg)
			}
		}
	}
}

func (sch *scheduler) followerJobSpawner(job jobData) {
	event := Event{Type: SCH_JOB_END}
	sch.spawnJob(&job, false)
	masterLog.Info("Job finished on this follower master!")
	event.Payload = job
	sch.toConnbox <- event
}
