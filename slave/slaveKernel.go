package slave

import (
	"fmt"
	. "github.com/juanfresia/claud/common"
	"github.com/juanfresia/claud/connbox"
	"github.com/juanfresia/claud/logger"
	"github.com/juanfresia/claud/tracker"
	"os/exec"
	"strconv"
	"time"
)

const (
	maxMasterAmount = 100
	leaderPort      = "2002"
)

// slaveKernel is the module that schedules and launches jobs on
// the different masters.
type slaveKernel struct {
	mt          tracker.Tracker
	newLeaderCh chan string

	mastersAmount int32
	memTotal      uint64
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

// newSlaveKernel creates a new slaveKernel together with
// its Tracker and Connbox.
func newSlaveKernel(mem uint64, mastersTotal uint) slaveKernel {
	logger.StartLog("./slave-" + myUuid.String()[:8] + ".log")
	registerEventPayloads()

	k := &slaveKernel{memTotal: mem}
	k.newLeaderCh = make(chan string, 10)
	k.clusterSize = mastersTotal/2 + 1
	fmt.Printf("CLUSTER SIZE: %v\n", k.clusterSize)
	k.mt = tracker.NewTracker(k.newLeaderCh, myUuid, k.clusterSize, false)

	k.nodeResources = make(map[string]NodeResourcesData)
	k.jobsTable = make(map[string]JobData)

	// Create buffered channels with ConnBox
	k.toConnbox = make(chan interface{}, 10)
	k.fromConnbox = make(chan interface{}, 10)
	k.connbox = connbox.NewConnBox(k.toConnbox, k.fromConnbox)

	// Initialize own resources data
	k.nodeResources[myUuid.String()] = NodeResourcesData{myUuid, mem, mem}

	go k.eventLoop()
	fmt.Println("Slave Kernel is up!")
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

// connectWithLeader makes the follower send its resources to the
// leader, and wait until a response with the resources of all
// followers is received. Must be called after opening the
// connbox in passive mode.
func (k *slaveKernel) connectWithLeader() error {
	logger.Logger.Info("Starting connection as follower slave")
	// First send own resources and jobs to the leader
	myResources := make(map[string]NodeResourcesData)
	myResources[myUuid.String()] = k.nodeResources[myUuid.String()]
	myJobs := make(map[string]JobData)
	for jobId, data := range k.jobsTable {
		if data.AssignedNode == myUuid.String() {
			myJobs[jobId] = data
		}
	}
	msg := &ConnectionMessage{myResources, myJobs}
	event := Event{Type: EV_RES_F, Payload: msg}
	k.toConnbox <- event

	logger.Logger.Info("Sent data to leader")
	return nil
}

// restartConnBox reopens all connections on the connbox module.
// Should be called every time a new leader is chosen.
func (k *slaveKernel) restartConnBox(leaderIp string) {
	// TODO: Close current connbox connections via the channel
	k.mastersAmount = int32(len(k.getMasters()))

	time.Sleep(tracker.LearningTmr)
	go k.connbox.StartActive(leaderIp + ":" + leaderPort)
	k.connectWithLeader()
}

// -------------------------------------------------------------------
//                       Job specific Functions
// -------------------------------------------------------------------

// updateTablesWithJob updates the nodeResources table and the
// jobsTable with the info on the received JobData. Use the extra
// argument isLaunched to indicate whether the job is being
// launched (true) or stopped (false).
func (k *slaveKernel) updateTablesWithJob(job JobData, isLaunched bool) {
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

// spawnJob launches a new job on this node using the given
// JobData. After the job is finished, it forwards an event to
// the connbox and updates the tables.
func (k *slaveKernel) spawnJob(job *JobData) {
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

	logger.Logger.Info("Job finished on this follower slave!")
	k.toConnbox <- Event{Type: EV_JOBEND_F, Payload: *job}

	k.updateTablesWithJob(*job, false)
}

// --------------------- Functions for the server --------------------

// TODO: Review the functions down here after refactor is completed

// getMasters returns a slice containing the UUID of all the
// alive master nodes.
func (k *slaveKernel) getMasters() []string {
	return k.mt.GetMasters()
}

// getNodeState returns a string representing the leader state
// of this master (leader, not leader, or anarchy).
func (k *slaveKernel) getNodeState() string {
	return k.mt.GetNodeState()
}

// getLeaderId retrieves the leader UUID (or a "no leader" message
// if no leader has been chosen yet).
func (k *slaveKernel) getLeaderId() string {
	return k.mt.GetLeaderId()
}

func (k *slaveKernel) getNodeResources() map[string]NodeResourcesData {
	return k.nodeResources
}

// stopJob attempts to stop a job, deleting the container
func (k *slaveKernel) stopJob(jobID string) string {
	job := k.jobsTable[jobID]
	if job.AssignedNode != myUuid.String() {
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
func (k *slaveKernel) getJobsList() map[string]JobData {
	return k.jobsTable
}

// -------------------------- Main eventLoop -------------------------

// eventLoop for the slaveKernel to handle all other modules events.
func (k *slaveKernel) eventLoop() {
	for {
		select {
		case newLeader := <-k.newLeaderCh:
			k.restartConnBox(newLeader)

		case msg := <-k.fromConnbox:
			k.handleEventOnFollower(msg.(Event))
		}
	}
}

// handleEventOnFollower handles all Events forwarded by the connbox
// in a master follower.
func (k *slaveKernel) handleEventOnFollower(e Event) {
	switch e.Type {
	case EV_RES_L:
		// Update resources with the info from leader
		msg, ok := e.Payload.(ConnectionMessage)
		if !ok {
			logger.Logger.Error("Received something strange as node data")
			return
		}
		for masterUuid, data := range msg.NodeResources {
			k.nodeResources[masterUuid] = data
		}
		for jobId, data := range msg.JobsTable {
			k.jobsTable[jobId] = data
		}
		fmt.Println("Data updated with info from leader")
	case EV_JOB_L:
		// If the job needs to be launched on my machine, then
		// REALLY launch the job.
		job, ok := e.Payload.(JobData)
		if !ok {
			return
		}

		k.updateTablesWithJob(job, true)
		if job.AssignedNode == myUuid.String() {
			logger.Logger.Info("Received new job data from leader!")
			go k.spawnJob(&job)
		}
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
