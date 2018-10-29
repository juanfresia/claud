package master

import (
	"encoding/gob"
	"fmt"
	"github.com/satori/go.uuid"
	"os/exec"
	"strconv"
)

// ----------------------- Data type definitions ----------------------

// masterResourcesData represents the resources of a master node
type masterResourcesData struct {
	MasterUuid uuid.UUID
	MemFree    uint64
	MemTotal   uint64
}

// jobState tracks the state of a running job.
type jobState int

const (
	JOB_RUNNING jobState = iota
	JOB_FINISHED
	JOB_FAILED
)

func (js jobState) String() string {
	strMap := [...]string{
		"RUNNING",
		"FINISHED",
		"FAILED",
	}
	return strMap[js]
}

// jobData represents all the info of a running/to run job
type jobData struct {
	JobName        string
	ImageName      string
	MemUsage       uint64
	AssignedMaster string
	JobId          string
	JobStatus      jobState
}

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

func registerEventPayloads() {
	// This setup is global (do any registration before launching connbox)
	gob.Register(map[string]masterResourcesData{})
	gob.Register(jobData{})
	gob.Register(masterResourcesData{})
}

func (sch *scheduler) updateTablesWithJob(job jobData, isLaunched bool) {
	assignedMaster := job.AssignedMaster
	assignedData := sch.masterResources[assignedMaster]
	if isLaunched {
		assignedData.MemFree -= job.MemUsage
	} else {
		assignedData.MemFree += job.MemUsage
	}
	sch.masterResources[assignedMaster] = assignedData
	sch.jobsTable[job.JobId] = job
}

func newScheduler(mem uint64) scheduler {
	sch := &scheduler{memTotal: mem}
	sch.masterResources = make(map[string]masterResourcesData)
	sch.jobsTable = make(map[string]jobData)

	// Create buffered channels (TODO: dont hardcode those 10)
	sch.toConnbox = make(chan Event, 10)
	sch.fromConnbox = make(chan Event, 10)

	sch.connbox = newConnBox(sch.toConnbox, sch.fromConnbox)

	// Initialize own resources data
	ownResourcesData := masterResourcesData{myUuid, mem, mem}
	sch.masterResources[myUuid.String()] = ownResourcesData

	registerEventPayloads()
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
		sch.updateTablesWithJob(*job, false)
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
	job := jobData{JobName: jobName, MemUsage: memUsage, AssignedMaster: assignedMaster}
	job.ImageName = imageName
	job.JobId = uuid.NewV4().String()
	job.JobStatus = JOB_RUNNING

	sch.toConnbox <- Event{Type: SCH_JOB, Payload: job}
	sch.updateTablesWithJob(job, true)

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
	fmt.Print("Entering leader scheduler\n")
	// Wait until all followers are connected
	// This is the leader, which is already connected
	var mastersConnected int32 = 1
	for {
		select {
		case e := <-sch.fromConnbox:
			fmt.Print("Received resources from one master\n")
			// TODO: check message type is correct (SCH_RDY)
			resources, ok := e.Payload.(masterResourcesData)
			if !ok {
				masterLog.Error("Received something strange as master resources")
				// TODO: really change this
				fmt.Print("Something went wrong\n")
				return nil
			}
			uuid := resources.MasterUuid.String()
			sch.masterResources[uuid] = resources
			fmt.Print("Updated master resources\n")
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
	fmt.Print("Entering follower scheduler\n")
	// Send resources to leader
	// First send resources to the leader
	mf := &masterResourcesData{MasterUuid: myUuid, MemFree: sch.memTotal, MemTotal: sch.memTotal}
	event := Event{Type: SCH_RDY, Payload: mf}
	sch.toConnbox <- event

	fmt.Print("Sent resources to leader\n")

	// Wait here for ready signal
	for e := range sch.fromConnbox {
		fmt.Print("Received leader response\n")
		// TODO: assert event type is correct
		// Update resources with the info from leader
		resources, ok := e.Payload.(map[string]masterResourcesData)
		if !ok {
			masterLog.Error("Received something strange as master resources")
			// TODO: really change this
			return nil
		}
		for uuid, data := range resources {
			sch.masterResources[uuid] = data
		}
		fmt.Print("Master resources updated with info from leader\n")
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
		sch.updateTablesWithJob(job, true)
		if job.AssignedMaster == myUuid.String() {
			go sch.followerJobSpawner(job)
		}
	case SCH_JOB_END:
		job, ok := e.Payload.(jobData)
		if !ok {
			masterLog.Error("Received something strange as job data")
			return
		}
		sch.updateTablesWithJob(job, false)
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
		sch.updateTablesWithJob(job, false)
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
