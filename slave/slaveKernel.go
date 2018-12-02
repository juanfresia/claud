package slave

import (
	"errors"
	"fmt"
	"github.com/juanfresia/claud/connbox"
	"github.com/juanfresia/claud/logger"
	"github.com/satori/go.uuid"
	"os/exec"
	"strconv"
	"time"
)

const (
	maxMasterAmount = 100
	leaderPort      = "2002"
)

type EventType int

const (
	EV_NULL EventType = iota
	EV_ACK
	EV_RES_F
	EV_RES_L
	EV_JOB_L
	EV_JOB_FF
	EV_JOBEND_L
	EV_JOBEND_F
	EV_JOBEND_FF
)

type Event struct {
	Type    EventType
	Payload interface{}
}

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

type connectionMessage struct {
	MasterResources map[string]masterResourcesData
	JobsTable       map[string]jobData
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

	connbox     *connbox.Connbox
	toConnbox   chan interface{}
	fromConnbox chan interface{}
}

// -------------------------------------------------------------------
//                          General Functions
// -------------------------------------------------------------------

// newMasterKernel creates a new masterKernel together with
// its tracker and connbox.
func newMasterKernel(mem uint64, mastersTotal uint) masterKernel {
	logger.StartLog("./master-" + myUuid.String()[:8] + ".log")
	registerEventPayloads()

	k := &masterKernel{memTotal: mem}
	k.newLeaderCh = make(chan string, 10)
	k.clusterSize = mastersTotal/2 + 1
	fmt.Printf("CLUSTER SIZE: %v\n", k.clusterSize)
	k.mt = newtracker(k.newLeaderCh, k.clusterSize)

	k.masterResources = make(map[string]masterResourcesData)
	k.jobsTable = make(map[string]jobData)

	// Create buffered channels with ConnBox
	k.toConnbox = make(chan interface{}, 10)
	k.fromConnbox = make(chan interface{}, 10)
	k.connbox = connbox.NewConnBox(k.toConnbox, k.fromConnbox)

	// Initialize own resources data
	k.masterResources[myUuid.String()] = masterResourcesData{myUuid, mem, mem}

	go k.eventLoop()
	fmt.Println("Master Kernel is up!")
	return *k
}

// registerEventPayloads registers on the gob encoder/decoder the
// structs that will be used as payload for an Event.
func registerEventPayloads() {
	connbox.Register(map[string]masterResourcesData{})
	connbox.Register(jobData{})
	connbox.Register(masterResourcesData{})
	connbox.Register(KeepAliveMessage{})
	connbox.Register(connectionMessage{})
	connbox.Register(Event{})
}

// connectWithFollowers makes the leader wait for all followers
// to make contact sending their resources. The function gathers
// all the followers resources on the masterResources table and
// forwards all that info to all followers. Must be called after
// opening the connbox in passive mode.
func (k *masterKernel) connectWithFollower(e Event) error {
	// Wait until all followers are connected
	logger.Logger.Info("Received resources from one master")

	msg, ok := e.Payload.(connectionMessage)
	if !ok {
		logger.Logger.Error("Received something strange as master resources")
		return errors.New("Received something strange as master resources")
	}

	for masterUuid, data := range msg.MasterResources {
		k.masterResources[masterUuid] = data
	}
	for jobId, data := range msg.JobsTable {
		k.jobsTable[jobId] = data
	}

	logger.Logger.Info("Updated master resources")
	// Send all master resources and jobs to followers
	logger.Logger.Info("Sending master resources to followers")
	fmt.Println("Sending master resources to followers")
	k.toConnbox <- Event{Type: EV_RES_L, Payload: &connectionMessage{k.masterResources, k.jobsTable}}
	return nil
}

// connectWithLeader makes the follower send its resources to the
// leader, and wait until a response with the resources of all
// followers is received. Must be called after opening the
// connbox in passive mode.
func (k *masterKernel) connectWithLeader() error {
	logger.Logger.Info("Starting connection as follower master")
	// First send own resources and jobs to the leader
	myResources := make(map[string]masterResourcesData)
	myResources[myUuid.String()] = k.masterResources[myUuid.String()]
	myJobs := make(map[string]jobData)
	for jobId, data := range k.jobsTable {
		if data.AssignedMaster == myUuid.String() {
			myJobs[jobId] = data
		}
	}
	msg := &connectionMessage{myResources, myJobs}
	event := Event{Type: EV_RES_F, Payload: msg}
	k.toConnbox <- event

	logger.Logger.Info("Sent resources to leader")
	return nil
}

// restartConnBox reopens all connections on the connbox module.
// Should be called every time a new leader is chosen.
func (k *masterKernel) restartConnBox(leaderIp string) {
	// TODO: Close current connbox connections via the channel
	k.mastersAmount = int32(len(k.getMasters()))
	if k.mt.imLeader() {
		go k.connbox.StartPassive(leaderPort)
	} else {
		// Tricky
		time.Sleep(learningTmr)
		go k.connbox.StartActive(leaderIp + ":" + leaderPort)
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
	logger.Logger.Info("Launching job on this node: " + job.JobName)
	jobFullName := job.JobName + "-" + job.JobId
	cmd := "sudo docker run --rm -m " + strconv.FormatUint(job.MemUsage, 10) + "m --name " + jobFullName + " " + job.ImageName
	fmt.Printf("%v\n", cmd)
	jobCmd := exec.Command("/bin/bash", "-c", cmd)
	err := jobCmd.Run()
	if err != nil {
		logger.Logger.Error("Couldn't successfully execute " + job.JobName + ":" + err.Error())
		job.JobStatus = JOB_FAILED
	} else {
		job.JobStatus = JOB_FINISHED
	}
	// Tell the other masters the job has finished and update the tables
	if k.mt.imLeader() {
		// If this was the leader, tell the followers the job has finished
		logger.Logger.Info("Job finished on the leader!!")
		k.toConnbox <- Event{Type: EV_JOBEND_L, Payload: *job}
	} else {
		// If this was a follower, tell the leader the job has finished
		logger.Logger.Info("Job finished on this follower master!")
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

	logger.Logger.Info("Ordering " + assignedMaster + " to launch a new job")
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
	if job.AssignedMaster != myUuid.String() {
		// If job aint on my machine, forward the JOBEND event
		k.toConnbox <- Event{Type: EV_JOBEND_FF, Payload: job}
		return jobID
	}
	jobFullName := job.JobName + "-" + job.JobId
	logger.Logger.Info("Stopping job on this node: " + jobFullName)

	cmd := "sudo docker stop " + jobFullName
	fmt.Printf("%v\n", cmd)
	jobCmd := exec.Command("/bin/bash", "-c", cmd)
	err := jobCmd.Run()
	if err != nil {
		logger.Logger.Error("Couldn't successfully stop job " + jobFullName + ":" + err.Error())
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
				k.handleEventOnLeader(msg.(Event))
			} else {
				k.handleEventOnFollower(msg.(Event))
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
		msg, ok := e.Payload.(connectionMessage)
		if !ok {
			logger.Logger.Error("Received something strange as master resources")
			return
		}
		for masterUuid, data := range msg.MasterResources {
			k.masterResources[masterUuid] = data
		}
		for jobId, data := range msg.JobsTable {
			k.jobsTable[jobId] = data
		}
		fmt.Println("Master resources updated with info from leader")
	case EV_JOB_L:
		// If the job needs to be launched on my machine, then
		// REALLY launch the job.
		job, ok := e.Payload.(jobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		logger.Logger.Info("Received new job data from leader!")
		k.updateTablesWithJob(job, true)
		if job.AssignedMaster == myUuid.String() {
			go k.spawnJob(&job)
		}
	case EV_JOBEND_FF:
		job, ok := e.Payload.(jobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		// Only stop the job if the forwarded event was for you
		if job.AssignedMaster == myUuid.String() {
			k.stopJob(job.JobId)
		}
	case EV_JOBEND_L:
		job, ok := e.Payload.(jobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		if job.AssignedMaster != myUuid.String() {
			k.updateTablesWithJob(job, false)
		}
	default:
		// Should never happen
		logger.Logger.Error("Received wrong Event type!")
	}
}

//handleEventOnLeader handles all Events forwarded by the connbox
// in the master leader.
func (k *masterKernel) handleEventOnLeader(e Event) {
	switch e.Type {
	case EV_RES_F:
		k.connectWithFollower(e)
	case EV_JOBEND_FF:
		job, ok := e.Payload.(jobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		// Only stop the job if the forwarded event was for you
		if job.AssignedMaster == myUuid.String() {
			k.stopJob(job.JobId)
		} else {
			k.toConnbox <- e
		}
	case EV_JOBEND_F:
		job, ok := e.Payload.(jobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		e.Type = EV_JOBEND_L
		k.toConnbox <- e
		logger.Logger.Info("Job finished on a follower master with status: " + job.JobStatus.String())
		k.updateTablesWithJob(job, false)
	case EV_JOB_L:
		// Check the msgScheduler has a jobData and forward it to the follower
		_, ok := e.Payload.(jobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		k.toConnbox <- e
	default:
		// Should never happen
		logger.Logger.Error("Received wrong Event type!")
	}
}
