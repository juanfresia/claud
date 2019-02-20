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

	// This maps IP:port to node uuid
	nodesByAddress map[string]string

	// TODO: Erase the finished jobs from the table after a while?
	jobsTable map[string]JobData

	connbox     *connbox.Connbox
	toConnbox   chan interface{}
	fromConnbox chan interface{}
	deadNode    chan string

	reescheduleTicker *time.Ticker
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
	k.nodesByAddress = make(map[string]string)
	k.jobsTable = make(map[string]JobData)

	// Create buffered channels with ConnBox
	k.toConnbox = make(chan interface{}, 10)
	k.fromConnbox = make(chan interface{}, 10)
	k.deadNode = make(chan string, 10)
	k.connbox = connbox.NewConnBox(k.toConnbox, k.fromConnbox, k.deadNode)

	k.reescheduleTicker = time.NewTicker(time.Minute)

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

// This need refinement to check for every combination of previos + advertised
// state
func (k *masterKernel) reconcileJobsFrom(src uuid.UUID, jobs map[string]JobData) {
	logger.Logger.Info("Reconciling jobs from: " + src.String())
	for jobId, job := range jobs {

		_, exists := k.jobsTable[jobId]
		if exists {
			logger.Logger.Info("Metadata for the job already exists")
		}

		if job.AssignedNode == src.String() {
			logger.Logger.Info("It's the owner of the job! Better trust'em")
		} else if !exists && (job.JobStatus == JOB_RUNNING) {
			job.JobStatus = JOB_PENDING
		} else if exists {
			// TODO: Here is where it gets complicated, the masters should agree upon
			// something. Currently, the first one is the one that counts.
			logger.Logger.Info("Existing job but not owner, ignoring request")
			continue
		}

		logger.Logger.Info("Updating job [" + jobId + "]")
		job.LastUpdate = time.Now()
		k.jobsTable[jobId] = job
	}
}

func (k *masterKernel) reconcileResourcesFrom(src uuid.UUID, res map[string]NodeResourcesData) {
	logger.Logger.Info("Reconciling resources from: " + src.String())
	for masterUuid, data := range res {
		k.nodeResources[masterUuid] = data
	}
}

func (k *masterKernel) updateJobData(data JobData) {
	if myUuid.String() == k.getLeaderId() {
		// Send update message
		k.toConnbox <- Event{Src: myUuid, Type: EV_JOB_UPDATE_STATE, Payload: &data}
	}
	k.jobsTable[data.JobId] = data
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

	k.reconcileResourcesFrom(e.Src, msg.NodeResources)
	k.reconcileJobsFrom(e.Src, msg.JobsTable)

	logger.Logger.Info("Updated node data")
	// Send all master resources and jobs to followers
	logger.Logger.Info("Sending all data to follower nodes")
	//fmt.Println("Sending all data to follower nodes")
	k.toConnbox <- Event{Src: myUuid, Type: EV_RES_L, Payload: &ConnectionMessage{k.nodeResources, k.jobsTable}}
	return nil
}

// handleNodeDeath is called every time the master leader
// detects a node has died via the deadNode channel. The
// leader then forwards a "node death" event to the other
// masters, and all masters poison the jobs of such node.
func (k *masterKernel) handleNodeDeath(address string) {
	logger.Logger.Info("A node has died " + address + "(" + k.nodesByAddress[address] + ")")
	nodeUuid := k.nodesByAddress[address]
	// Send the dead node UUID to all masters
	msg := Event{Src: myUuid, Type: EV_NODE_DEATH, Payload: nodeUuid}
	k.toConnbox <- msg
	k.poisonJobs(nodeUuid)
	delete(k.nodesByAddress, address)
}

// poisonJobs marks as pending all jobs related to the
// node with the given uuid.
func (k *masterKernel) poisonJobs(uuid string) {
	// TODO: send update to other masters!
	for _, job := range k.jobsTable {
		if (job.AssignedNode == uuid) && (job.JobStatus == JOB_RUNNING) {
			job.JobStatus = JOB_PENDING
			job.LastUpdate = time.Now()
			k.updateJobData(job)
		}
	}
	delete(k.nodeResources, uuid)
}

// connectWithLeader makes the follower send its resources to the
// leader, and wait until a response with the resources of all
// followers is received. Must be called after opening the
// connbox in passive mode.
func (k *masterKernel) connectWithLeader() error {
	logger.Logger.Info("Starting connection as follower master")
	// First send own resources and jobs to the leader
	myResources := k.nodeResources
	myJobs := k.jobsTable
	msg := &ConnectionMessage{myResources, myJobs}
	event := Event{Src: myUuid, Type: EV_RES_F, Payload: msg}
	k.toConnbox <- event

	logger.Logger.Info("Sent data to leader")
	return nil
}

func (k *masterKernel) becomeLeader() {
	// Upon becoming a leader, mark everything as JOB_PENDING
	for _, job := range k.jobsTable {
		if job.JobStatus == JOB_RUNNING {
			job.JobStatus = JOB_PENDING
			job.LastUpdate = time.Now()
			k.updateJobData(job)
		}
	}
}

// restartConnBox reopens all connections on the connbox module.
// Should be called every time a new leader is chosen.
func (k *masterKernel) restartConnBox(leaderIp string) {
	// TODO: Close current connbox connections via the channel
	k.mastersAmount = int32(len(k.getMasters()))
	if k.mt.ImLeader() {
		k.becomeLeader()
		go k.connbox.StartPassive(leaderPort)
	} else {
		// Tricky
		time.Sleep(tracker.LearningTmr)
		go k.connbox.StartActive(leaderIp + ":" + leaderPort)
		// TODO: warning, will block if connbox fails to start
		<-k.connbox.Ready
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
	job.LastUpdate = time.Now()
	k.updateJobData(job)
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
func (k *masterKernel) launchJob(jobName string, memUsage uint64, imageName string) (string, error) {
	// If ain't the leader, forward the job launch job
	if k.getLeaderId() != myUuid.String() {
		job := JobData{JobName: jobName, MemUsage: memUsage, ImageName: imageName, LastUpdate: time.Now()}
		k.toConnbox <- Event{Src: myUuid, Type: EV_JOB_FF, Payload: job}
		return "", nil
	}

	assignedNode := ""
	for uuid, data := range k.nodeResources {
		if data.MemFree >= memUsage {
			assignedNode = uuid
			break
		}
	}
	if assignedNode == "" {
		return "", fmt.Errorf("Error, not enough resources on claud to lauch job: %v", jobName)
	}

	logger.Logger.Info("Ordering " + assignedNode + " to launch a new job")
	job := JobData{JobName: jobName, MemUsage: memUsage, AssignedNode: assignedNode}
	job.ImageName = imageName
	job.JobId = uuid.NewV4().String()
	job.JobStatus = JOB_RUNNING

	k.toConnbox <- Event{Src: myUuid, Type: EV_JOB_L, Payload: job}
	k.updateTablesWithJob(job, true)

	return job.JobId, nil
}

func (k *masterKernel) relaunchPendingJobs() {
	if myUuid.String() != k.getLeaderId() {
		return
	}

	logger.Logger.Info("Relaunching Jobs")
	now := time.Now()
	for _, job := range k.jobsTable {

		// Skip any job that is not pending
		if job.JobStatus != JOB_PENDING {
			continue
		}

		// Skip if job has been updated recently
		// TODO: unhardcode this
		timeSinceLastUpdate := now.Sub(job.LastUpdate)
		if timeSinceLastUpdate.Seconds() < 20 {
			continue
		}

		if _, err := k.launchJob(job.JobName, job.MemUsage, job.ImageName); err != nil {
			logger.Logger.Error(err.Error())
			continue
		}

		logger.Logger.Info("Marking job " + job.JobId + " as Lost")
		job.JobStatus = JOB_LOST
		k.updateJobData(job)
		// TODO: CRITICAL: advertise this change in job to other master
		// k.toConnbox <- Event{Src: myUuid, Type: EV_JOB_L, Payload: job}
		// k.updateTablesWithJob(job, true)
	}
}

// stopJob attempts to stop a job, deleting the container
func (k *masterKernel) stopJob(jobID string) string {
	job := k.jobsTable[jobID]
	if job.AssignedNode == myUuid.String() {
		logger.Logger.Error("Someone ordered this master to stop a job!")
		return jobID
	}
	// If job aint on my machine, forward the JOBEND event
	k.toConnbox <- Event{Src: myUuid, Type: EV_JOBEND_FF, Payload: job}
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
			aux, ok := msg.(connbox.Message)
			if !ok {
				logger.Logger.Error("Error casting message received from connbox")
				continue
			}
			if k.mt.ImLeader() {
				k.handleEventOnLeader(aux)
			} else {
				k.handleEventOnFollower(aux)
			}

		case address := <-k.deadNode:
			k.handleNodeDeath(address)

		case <-k.reescheduleTicker.C:
			k.relaunchPendingJobs()
		}
	}
}

// handleEventOnFollower handles all Events forwarded by the connbox
// in a master follower.
func (k *masterKernel) handleEventOnFollower(msg connbox.Message) {
	e, ok := msg.Msg.(Event)
	if !ok {
		logger.Logger.Error("Received something that is not an Event")
	}
	// TODO: Maybe this should not be here x2
	src := msg.SrcAddr.String()
	if _, exists := k.nodesByAddress[src]; !exists {
		k.nodesByAddress[src] = e.Src.String()
	}

	//logger.Logger.Info("Message received from " + src)
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
	case EV_NODE_DEATH:
		uuid, ok := e.Payload.(string)
		if !ok {
			logger.Logger.Error("Received something strange as a dead node UUID")
			return
		}
		logger.Logger.Info("Detected the node " + uuid + " has died")
		k.poisonJobs(uuid)
	case EV_JOB_UPDATE_STATE:
		jobData, ok := e.Payload.(JobData)
		if !ok {
			logger.Logger.Error("Received something strange as job data")
			return
		}
		k.updateJobData(jobData)
	default:
		// Should never happen
		logger.Logger.Error("Received wrong Event type!")
	}
}

//handleEventOnLeader handles all Events forwarded by the connbox
// in the master leader.
func (k *masterKernel) handleEventOnLeader(msg connbox.Message) {
	e, ok := msg.Msg.(Event)
	if !ok {
		logger.Logger.Error("Received something that is not an Event")
	}

	// TODO: Maybe this should not be here
	src := msg.SrcAddr.String()
	if _, exists := k.nodesByAddress[src]; !exists {
		k.nodesByAddress[src] = e.Src.String()
	}

	logger.Logger.Info("Message received from " + src)
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
