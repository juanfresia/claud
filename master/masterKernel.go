package master

import (
	"errors"
	"fmt"
	. "github.com/juanfresia/claud/common"
	"github.com/juanfresia/claud/connbox"
	"github.com/juanfresia/claud/logger"
	"github.com/juanfresia/claud/tracker"
	"github.com/satori/go.uuid"
	"time"
)

const (
	maxMasterAmount = 100
	leaderPort      = "2002"
)

// masterKernel is the module that schedules and launches jobs on
// the different masters. It uses the Tracker to know whether or
// not it should behave as the leader of all masters.
type masterKernel struct {
	mt          tracker.Tracker
	newLeaderCh chan string

	mastersAmount int32
	clusterSize   uint
	nodeResources map[string]NodeResourcesData

	// TODO: Erase the finished jobs from the table after a while?
	jobsTable map[string]JobData

	connbox     *connbox.Connbox
	toConnbox   chan interface{}
	fromConnbox chan interface{}
}

// -------------------------------------------------------------------
//                          General Functions
// -------------------------------------------------------------------

// newMasterKernel creates a new masterKernel together with
// its Tracker and Connbox.
func newMasterKernel(mastersTotal uint) masterKernel {
	logger.StartLog("./master-" + myUuid.String()[:8] + ".log")
	registerEventPayloads()

	k := &masterKernel{}
	k.newLeaderCh = make(chan string, 10)
	k.clusterSize = mastersTotal/2 + 1
	fmt.Printf("CLUSTER SIZE: %v\n", k.clusterSize)
	k.mt = tracker.NewTracker(k.newLeaderCh, myUuid, k.clusterSize, true)

	k.nodeResources = make(map[string]NodeResourcesData)
	k.jobsTable = make(map[string]JobData)

	// Create buffered channels with ConnBox
	k.toConnbox = make(chan interface{}, 10)
	k.fromConnbox = make(chan interface{}, 10)
	k.connbox = connbox.NewConnBox(k.toConnbox, k.fromConnbox)

	go k.eventLoop()
	fmt.Println("Master Kernel is up!")
	return *k
}

// registerEventPayloads registers on the gob encoder/decoder the
// structs that will be used as payload for an Event.
func registerEventPayloads() {
	connbox.Register(map[string]NodeResourcesData{})
	connbox.Register(JobData{})
	connbox.Register(NodeResourcesData{})
	connbox.Register(tracker.KeepAliveMessage{})
	connbox.Register(ConnectionMessage{})
	connbox.Register(Event{})
}

// connectWithFollowers makes the leader wait for all followers
// to make contact sending their resources. The function gathers
// all the followers resources on the nodeResources table and
// forwards all that info to all followers. Must be called after
// opening the connbox in passive mode.
func (k *masterKernel) connectWithFollower(e Event) error {
	// Wait until all followers are connected
	logger.Logger.Info("Received data from one node")

	msg, ok := e.Payload.(ConnectionMessage)
	if !ok {
		logger.Logger.Error("Received something strange as node data")
		return errors.New("Received something strange as node data")
	}

	for masterUuid, data := range msg.NodeResources {
		k.nodeResources[masterUuid] = data
	}
	for jobId, data := range msg.JobsTable {
		k.jobsTable[jobId] = data
	}

	logger.Logger.Info("Updated node data")
	// Send all master resources and jobs to followers
	logger.Logger.Info("Sending all data to follower nodes")
	//fmt.Println("Sending all data to follower nodes")
	k.toConnbox <- Event{Type: EV_RES_L, Payload: &ConnectionMessage{k.nodeResources, k.jobsTable}}
	return nil
}

// connectWithLeader makes the follower send its resources to the
// leader, and wait until a response with the resources of all
// followers is received. Must be called after opening the
// connbox in passive mode.
func (k *masterKernel) connectWithLeader() error {
	logger.Logger.Info("Starting connection as follower master")
	// First send own resources and jobs to the leader
	myResources := make(map[string]NodeResourcesData)
	myJobs := make(map[string]JobData)
	msg := &ConnectionMessage{myResources, myJobs}
	event := Event{Type: EV_RES_F, Payload: msg}
	k.toConnbox <- event

	logger.Logger.Info("Sent data to leader")
	return nil
}

// restartConnBox reopens all connections on the connbox module.
// Should be called every time a new leader is chosen.
func (k *masterKernel) restartConnBox(leaderIp string) {
	// TODO: Close current connbox connections via the channel
	k.mastersAmount = int32(len(k.getMasters()))
	if k.mt.ImLeader() {
		go k.connbox.StartPassive(leaderPort)
	} else {
		// Tricky
		time.Sleep(tracker.LearningTmr)
		go k.connbox.StartActive(leaderIp + ":" + leaderPort)
		k.connectWithLeader()
	}
}

// -------------------------------------------------------------------
//                       Job specific Functions
// -------------------------------------------------------------------

// updateTablesWithJob updates the nodeResources table and the
// jobsTable with the info on the received JobData. Use the extra
// argument isLaunched to indicate whether the job is being
// launched (true) or stopped (false).
func (k *masterKernel) updateTablesWithJob(job JobData, isLaunched bool) {
	assignedNode := job.AssignedNode
	assignedData := k.nodeResources[assignedNode]
	if isLaunched {
		assignedData.MemFree -= job.MemUsage
	} else {
		assignedData.MemFree += job.MemUsage
	}
	k.nodeResources[assignedNode] = assignedData
	k.jobsTable[job.JobId] = job
}

// --------------------- Functions for the server --------------------

// TODO: Review the functions down here after refactor is completed

// getMasters returns a slice containing the UUID of all the
// alive master nodes.
func (k *masterKernel) getMasters() []string {
	return k.mt.GetMasters()
}

// getNodeState returns a string representing the leader state
// of this master (leader, not leader, or anarchy).
func (k *masterKernel) getNodeState() string {
	return k.mt.GetNodeState()
}

// GetLeaderId retrieves the leader UUID (or a "no leader" message
// if no leader has been chosen yet).
func (k *masterKernel) getLeaderId() string {
	return k.mt.GetLeaderId()
}

func (k *masterKernel) getNodeResources() map[string]NodeResourcesData {
	return k.nodeResources
}

// launchJob selects a node with enough resources for launching the job,
// and forwards an Event with Type: EV_JOB_L to all connections via
// the k.toConnbox. It returns the JobId of the created job.
func (k *masterKernel) launchJob(jobName string, memUsage uint64, imageName string) string {
	// If ain't the leader, forward the job launch job
	if k.getLeaderId() != myUuid.String() {
		job := JobData{JobName: jobName, MemUsage: memUsage, ImageName: imageName}
		k.toConnbox <- Event{Type: EV_JOB_FF, Payload: job}
		return "Job forwarded to leader, see /jobs"
	}

	assignedNode := ""
	for uuid, data := range k.nodeResources {
		if data.MemFree >= memUsage {
			assignedNode = uuid
			break
		}
	}
	if assignedNode == "" {
		return "Error, not enough resources on claud"
	}

	logger.Logger.Info("Ordering " + assignedNode + " to launch a new job")
	job := JobData{JobName: jobName, MemUsage: memUsage, AssignedNode: assignedNode}
	job.ImageName = imageName
	job.JobId = uuid.NewV4().String()
	job.JobStatus = JOB_RUNNING

	k.toConnbox <- Event{Type: EV_JOB_L, Payload: job}
	k.updateTablesWithJob(job, true)

	return job.JobId
}

// stopJob attempts to stop a job, deleting the container
func (k *masterKernel) stopJob(jobID string) string {
	job := k.jobsTable[jobID]
	if job.AssignedNode == myUuid.String() {
		logger.Logger.Error("Someone ordered this master to stop a job!")
		return jobID
	}
	// If job aint on my machine, forward the JOBEND event
	k.toConnbox <- Event{Type: EV_JOBEND_FF, Payload: job}
	return jobID
}

// getJobsTable retrieves all jobs data
func (k *masterKernel) getJobsList() map[string]JobData {
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
			if k.mt.ImLeader() {
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
		msg, ok := e.Payload.(ConnectionMessage)
		if !ok {
			logger.Logger.Error("Received something strange as master data")
			return
		}
		for masterUuid, data := range msg.NodeResources {
			k.nodeResources[masterUuid] = data
		}
		for jobId, data := range msg.JobsTable {
			k.jobsTable[jobId] = data
		}
		fmt.Println("Master data updated with info from leader")
	case EV_JOB_L:
		// If the job needs to be launched on my machine, then
		// REALLY launch the job.
		job, ok := e.Payload.(JobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		logger.Logger.Info("Received new job data from leader!")
		k.updateTablesWithJob(job, true)
	case EV_JOBEND_FF:
		job, ok := e.Payload.(JobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		// Only stop the job if the forwarded event was for you
		if job.AssignedNode == myUuid.String() {
			k.stopJob(job.JobId)
		}
	case EV_JOBEND_L:
		job, ok := e.Payload.(JobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		if job.AssignedNode != myUuid.String() {
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
		job, ok := e.Payload.(JobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		// Only stop the job if the forwarded event was for you
		if job.AssignedNode == myUuid.String() {
			k.stopJob(job.JobId)
		} else {
			k.toConnbox <- e
		}
	case EV_JOBEND_F:
		job, ok := e.Payload.(JobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		e.Type = EV_JOBEND_L
		k.toConnbox <- e
		logger.Logger.Info("Job finished on a follower node with status: " + job.JobStatus.String())
		k.updateTablesWithJob(job, false)
	case EV_JOB_L:
		// Check the msgScheduler has a JobData and forward it to the follower
		_, ok := e.Payload.(JobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		k.toConnbox <- e
	case EV_JOB_FF:
		// Check the msgScheduler has a JobData and forward it to the follower
		job, ok := e.Payload.(JobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		k.launchJob(job.JobName, job.MemUsage, job.ImageName)
	default:
		// Should never happen
		logger.Logger.Error("Received wrong Event type!")
	}
}
