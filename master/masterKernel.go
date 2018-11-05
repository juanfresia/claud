package master

import (
	"encoding/gob"
	"errors"
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

// masterKernel is the module that schedules and launches jobs on
// the different masters. It uses the tracker to know whether or
// not it should behave as the leader of all masters.
type masterKernel struct {
	mt          tracker
	newLeaderCh chan string

	mastersAmount   int32
	memTotal        uint64
	clusterSize     uint
	masterResources map[string]masterResourcesData

	// TODO: Erase the finished jobs from the table after a while?
	jobsTable map[string]jobData

	connbox     *connbox
	toConnbox   chan Event
	fromConnbox chan Event
}

// -------------------------------------------------------------------
//                          General Functions
// -------------------------------------------------------------------

// newMasterKernel creates a new masterKernel together with
// its tracker and connbox.
func newMasterKernel(mem uint64, mastersTotal uint) masterKernel {
	StartupMasterLog("./master-" + myUuid.String()[:8] + ".log")

	k := &masterKernel{memTotal: mem}
	k.newLeaderCh = make(chan string, 10)
	k.clusterSize = mastersTotal/2 + 1
	k.mt = newtracker(k.newLeaderCh, k.clusterSize)

	k.masterResources = make(map[string]masterResourcesData)
	k.jobsTable = make(map[string]jobData)

	// Create buffered channels with ConnBox
	k.toConnbox = make(chan Event, 10)
	k.fromConnbox = make(chan Event, 10)
	k.connbox = newConnBox(k.toConnbox, k.fromConnbox)

	// Initialize own resources data
	k.masterResources[myUuid.String()] = masterResourcesData{myUuid, mem, mem}

	registerEventPayloads()

	go k.eventLoop()
	fmt.Println("Master Kernel is up!")
	return *k
}

// registerEventPayloads registers on the gob encoder/decoder the
// structs that will be used as payload for an Event.
func registerEventPayloads() {
	gob.Register(map[string]masterResourcesData{})
	gob.Register(jobData{})
	gob.Register(masterResourcesData{})
	gob.Register(KeepAliveMessage{})
}

// connectWithFollowers makes the leader wait for all followers
// to make contact sending their resources. The function gathers
// all the followers resources on the masterResources table and
// forwards all that info to all followers. Must be called after
// opening the connbox in passive mode.
func (k *masterKernel) connectWithFollower(e Event) error {
	// Wait until all followers are connected
	masterLog.Info("Received resources from one master")

	resources, ok := e.Payload.(masterResourcesData)
	if !ok {
		masterLog.Error("Received something strange as master resources")
		return errors.New("Received something strange as master resources")
	}

	k.masterResources[resources.MasterUuid.String()] = resources
	masterLog.Info("Updated master resources")
	// Send all master resources to followers
	masterLog.Info("Sending master resources to followers\n")
	fmt.Print("Sending master resources to followers")
	k.toConnbox <- Event{Type: EV_RES_L, Payload: k.masterResources}
	return nil
}

// connectWithLeader makes the follower send its resources to the
// leader, and wait until a response with the resources of all
// followers is received. Must be called after opening the
// connbox in passive mode.
func (k *masterKernel) connectWithLeader() error {
	masterLog.Info("Starting connection as follower master")
	// First send resources to the leader
	mf := &masterResourcesData{MasterUuid: myUuid, MemFree: k.memTotal, MemTotal: k.memTotal}
	event := Event{Type: EV_RES_F, Payload: mf}
	k.toConnbox <- event

	masterLog.Info("Sent resources to leader")
	return nil
}

// restartConnBox reopens all connections on the connbox module.
// Should be called every time a new leader is chosen.
func (k *masterKernel) restartConnBox(leaderIp string) {
	// TODO: Close current connbox connections via the channel
	k.mastersAmount = int32(len(k.getMasters()))
	if k.mt.imLeader() {
		go k.connbox.startPassive(leaderPort)
	} else {
		// Tricky
		time.Sleep(learningTmr)
		go k.connbox.startActive(leaderIp + ":" + leaderPort)
		k.connectWithLeader()
	}
}

// -------------------------------------------------------------------
//                       Job specific Functions
// -------------------------------------------------------------------

// updateTablesWithJob updates the masterResources table and the
// jobsTable with the info on the received jobData. Use the extra
// argument isLaunched to indicate whether the job is being
// launched (true) or stopped (false).
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

// spawnJob launches a new job on this node using the given
// jobData. After the job is finished, it forwards an event to
// the connbox and updates the tables.
func (k *masterKernel) spawnJob(job *jobData) {
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
	// Tell the other masters the job has finished and update the tables
	if k.mt.imLeader() {
		// If this was the leader, tell the followers the job has finished
		masterLog.Info("Job finished on the leader!!")
		k.toConnbox <- Event{Type: EV_JOBEND_L, Payload: *job}
	} else {
		// If this was a follower, tell the leader the job has finished
		masterLog.Info("Job finished on this follower master!")
		k.toConnbox <- Event{Type: EV_JOBEND_F, Payload: *job}
	}
	k.updateTablesWithJob(*job, false)
}

// --------------------- Functions for the server --------------------

// TODO: Review the functions down here after refactor is completed

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
		go k.spawnJob(&job)
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
				k.handleEventOnLeader(msg)
			} else {
				k.handleEventOnFollower(msg)
			}
		}
	}
}

// handleEventOnFollower handles all Events forwarded by the connbox
// in a master follower.
func (k *masterKernel) handleEventOnFollower(e Event) {
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
		// REALLY launch the job.
		job, ok := e.Payload.(jobData)
		if !ok {
			masterLog.Error("Received something strange as job data")
			return
		}
		masterLog.Info("Received new job data from leader!\n")
		k.updateTablesWithJob(job, true)
		if job.AssignedMaster == myUuid.String() {
			go k.spawnJob(&job)
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

//handleEventOnLeader handles all Events forwarded by the connbox
// in the master leader.
func (k *masterKernel) handleEventOnLeader(e Event) {
	switch e.Type {
	case EV_RES_F:
		k.connectWithFollower(e)
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
