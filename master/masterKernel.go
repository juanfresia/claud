package master

import (
	"encoding/gob"
	"fmt"
	"github.com/satori/go.uuid"
	"os/exec"
	"strconv"
	"time"
)

const (
	maxMasterAmount = 100
	leaderPort      = "2002"
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

// masterKernel is the module that kedules and launches jobs
// on the different masters.
type masterKernel struct {
	mt          tracker
	newLeaderCh chan string

	mastersAmount   int32
	memTotal        uint64
	masterResources map[string]masterResourcesData

	// TODO: Erase the finished jobs from the table after a while?
	jobsTable map[string]jobData

	connbox     *connbox
	toConnbox   chan Event
	fromConnbox chan Event
}

// -------------------------------------------------------------------
//                               Functions
// -------------------------------------------------------------------

// newMasterKernel creates a new masterKernel together with
// its tracker and connbox.
func newMasterKernel(mem uint64) masterKernel {
	StartupMasterLog("./master-" + myUuid.String()[:8] + ".log")

	k := &masterKernel{memTotal: mem}
	k.newLeaderCh = make(chan string, 10)
	k.mt = newtracker(k.newLeaderCh)

	k.masterResources = make(map[string]masterResourcesData)
	k.jobsTable = make(map[string]jobData)

	// Create buffered channels with ConnBox
	k.toConnbox = make(chan Event, 10)
	k.fromConnbox = make(chan Event, 10)
	k.connbox = newConnBox(k.toConnbox, k.fromConnbox)

	// Initialize own resources data
	ownResourcesData := masterResourcesData{myUuid, mem, mem}
	k.masterResources[myUuid.String()] = ownResourcesData

	registerEventPayloads()

	go k.eventLoop()

	fmt.Println("Master Kernel is up!")
	return *k
}

func registerEventPayloads() {
	// This setup is global (do any registration before launching connbox)
	gob.Register(map[string]masterResourcesData{})
	gob.Register(jobData{})
	gob.Register(masterResourcesData{})
}

func (k *masterKernel) updateTablesWithJob(job jobData, isLaunched bool) {
	assignedMaster := job.AssignedMaster
	assignedData := k.masterResources[assignedMaster]
	if isLaunched {
		assignedData.MemFree -= job.MemUsage
	} else {
		assignedData.MemFree += job.MemUsage
	}
	k.masterResources[assignedMaster] = assignedData
	k.jobsTable[job.JobId] = job
}

func (k *masterKernel) connectWithFollowers() error {
	masterLog.Info("Starting connection as leader master")
	// Wait until all followers are connected
	// This is the leader, which is already connected
	var mastersConnected int32 = 1
	for {
		select {
		case e := <-k.fromConnbox:
			masterLog.Info("Received resources from one master")
			// TODO: check message type is correct (EV_RES_F)
			resources, ok := e.Payload.(masterResourcesData)
			if !ok {
				masterLog.Error("Received something strange as master resources")
				// TODO: really change this
				fmt.Print("Something went wrong\n")
				return nil
			}
			uuid := resources.MasterUuid.String()
			k.masterResources[uuid] = resources
			masterLog.Info("Updated master resources")
			mastersConnected += 1
			if mastersConnected == k.mastersAmount {
				// Send resources updates to followers
				masterLog.Info("Sending master resources to followers\n")
				fmt.Print("Sending master resources to followers")
				// Send all master resources to followers
				k.toConnbox <- Event{Type: EV_RES_L, Payload: k.masterResources}
				return nil
			}
		}
	}
}

func (k *masterKernel) connectWithLeader() error {
	masterLog.Info("Starting connection as follower master")
	// Send resources to leader
	// First send resources to the leader
	mf := &masterResourcesData{MasterUuid: myUuid, MemFree: k.memTotal, MemTotal: k.memTotal}
	event := Event{Type: EV_RES_F, Payload: mf}
	k.toConnbox <- event

	masterLog.Info("Sent resources to leader")

	// Wait here for ready signal
	for e := range k.fromConnbox {
		masterLog.Info("Received leader response")
		// TODO: assert event type is correct
		// Update resources with the info from leader
		resources, ok := e.Payload.(map[string]masterResourcesData)
		if !ok {
			masterLog.Error("Received something strange as master resources")
			// TODO: really change this
			return nil
		}
		for uuid, data := range resources {
			k.masterResources[uuid] = data
		}
		masterLog.Info("Master resources updated with info from leader")
		fmt.Print("Master resources updated with info from leader\n")
		break
	}

	return nil
}

// restartConnBox reopens all connections on the connbox module.
// Should be called every time a new leader is chosen.
func (k *masterKernel) restartConnBox(leaderIp string) {
	imLeader := k.mt.imLeader()
	k.mastersAmount = int32(len(k.getMasters()))
	if imLeader {
		go k.connbox.startPassive(leaderPort)
		k.connectWithFollowers()
	} else {
		// Tricky
		time.Sleep(learningTmr)
		go k.connbox.startActive(leaderIp + ":" + leaderPort)
		k.connectWithLeader()
	}
}

func (k *masterKernel) followerJobSpawner(job jobData) {
	k.spawnJob(&job, false)
	masterLog.Info("Job finished on this follower master!")
	event := Event{Type: EV_JOBEND_F}
	event.Payload = job
	k.toConnbox <- event
}

func (k *masterKernel) spawnJob(job *jobData, imLeader bool) {
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

		k.toConnbox <- Event{Type: EV_JOBEND_L, Payload: *job}
		k.updateTablesWithJob(*job, false)
	}
}

// ------------------ Functions for the masterServer -----------------

// getMasters returns a slice containing the UUID of all the
// alive master nodes.
func (k *masterKernel) getMasters() []string {
	return k.mt.getMasters()
}

// getLeaderState returns a string representing the leader state
// of this master (leader, not leader, or anarchy).
func (k *masterKernel) getLeaderState() string {
	return k.mt.getLeaderState()
}

// getLeaderId retrieves the leader UUID (or a "no leader" message
// if no leader has been chosen yet).
func (k *masterKernel) getLeaderId() string {
	return k.mt.getLeaderId()
}

func (k *masterKernel) getMastersResources() map[string]masterResourcesData {
	return k.masterResources
}

// launchJob selects a node with enough resources for launching the job,
// and forwards an Event with Type: EV_JOB_L to all connections via
// the k.toConnbox. It returns the JobId of the created job.
func (k *masterKernel) launchJob(jobName string, memUsage uint64, imageName string) string {
	assignedMaster := ""
	for uuid, data := range k.masterResources {
		if data.MemFree >= memUsage {
			assignedMaster = uuid
			break
		}
	}
	if assignedMaster == "" {
		return "Error, not enough resources on claud"
	}

	masterLog.Info("Ordering " + assignedMaster + " to launch a new job")
	job := jobData{JobName: jobName, MemUsage: memUsage, AssignedMaster: assignedMaster}
	job.ImageName = imageName
	job.JobId = uuid.NewV4().String()
	job.JobStatus = JOB_RUNNING

	k.toConnbox <- Event{Type: EV_JOB_L, Payload: job}
	k.updateTablesWithJob(job, true)

	if assignedMaster == myUuid.String() {
		go k.spawnJob(&job, true)
	}
	return job.JobId
}

// stopJob attempts to stop a job, deleting the container
func (k *masterKernel) stopJob(jobID string) string {
	job := k.jobsTable[jobID]
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

// getJobsTable retrieves all jobs data
func (k *masterKernel) getJobsList() map[string]jobData {
	return k.jobsTable
}

// -------------------------- Main eventLoop -------------------------

// eventLoop for the masterKernel to handle all other modules events.
func (k *masterKernel) eventLoop() {
	for {
		select {
		case newLeader := <-k.newLeaderCh:
			k.restartConnBox(newLeader)

		case msg := <-k.fromConnbox:
			if k.mt.imLeader() {
				k.handleLeaderEvent(msg)
			} else {
				k.handleFollowerEvent(msg)
			}
		}
	}
}

func (k *masterKernel) handleFollowerEvent(e Event) {
	switch e.Type {
	case EV_RES_L:
		// Update resources with the info from leader
		resources, ok := e.Payload.(map[string]masterResourcesData)
		if !ok {
			masterLog.Error("Received something strange as master resources")
			return
		}
		for uuid, data := range resources {
			k.masterResources[uuid] = data
		}
		fmt.Print("Master resources updated with info from leader\n")
	case EV_JOB_L:
		// If the job needs to be launched on my machine, then
		// REALLY launch the job. Update the masterResources table
		// and the jobsTable.
		job, ok := e.Payload.(jobData)
		if !ok {
			masterLog.Error("Received something strange as job data")
			return
		}
		masterLog.Info("Received new job data from leader!\n")
		k.updateTablesWithJob(job, true)
		if job.AssignedMaster == myUuid.String() {
			go k.followerJobSpawner(job)
		}
	case EV_JOBEND_L:
		job, ok := e.Payload.(jobData)
		if !ok {
			masterLog.Error("Received something strange as job data")
			return
		}
		k.updateTablesWithJob(job, false)
	default:
		// Should never happen
		masterLog.Error("Received wrong Event type!")
	}
}

func (k *masterKernel) handleLeaderEvent(e Event) {
	switch e.Type {
	case EV_JOBEND_F:
		job, ok := e.Payload.(jobData)
		if !ok {
			masterLog.Error("Received something strange as job data")
			return
		}
		e.Type = EV_JOBEND_L
		k.toConnbox <- e
		masterLog.Info("Job finished on this master with status: " + job.JobStatus.String())
		k.updateTablesWithJob(job, false)
	case EV_JOB_L:
		// Check the msgScheduler has a jobData and forward it to the follower
		_, ok := e.Payload.(jobData)
		if !ok {
			masterLog.Error("Received something strange as job data")
			return
		}
		k.toConnbox <- e
	default:
		// Should never happen
		masterLog.Error("Received wrong Event type!")
	}
}
