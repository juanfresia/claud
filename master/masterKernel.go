package master

import (
	"fmt"
	"time"
	//"github.com/satori/go.uuid"
)

const (
	maxMasterAmount = 100
)

// ----------------------- Data type definitions ----------------------

// masterKernel is the core of all masters. The different master
// modules interact with it.
type masterKernel struct {
	sch         scheduler
	mt          tracker
	newLeaderCh chan string
}

// -------------------------------------------------------------------
//                               Functions
// -------------------------------------------------------------------

// newMasterKernel creates a new masterKernel together with
// its scheduler and tracker.
func newMasterKernel(mem uint64) masterKernel {
	k := &masterKernel{}

	StartupMasterLog("./kernel-" + myUuid.String()[:8] + ".log")
	k.newLeaderCh = make(chan string, 10)
	k.mt = newtracker(k.newLeaderCh)
	k.sch = newScheduler(mem)

	go k.eventLoop()

	fmt.Println("Master Kernel is up!")
	return *k
}

// restartscheduler reopens all connections on the master scheduler
// module. Should be called every time a new leader is chosen.
func (k *masterKernel) restartscheduler(leaderIp string) {
	imLeader := k.mt.imLeader()
	if !imLeader {
		// Tricky
		// TODO: change this sleep for ticker
		time.Sleep(learningTmr)
	}
	mastersAmount := int32(len(k.getMasters()))
	k.sch.start(leaderIp, imLeader, mastersAmount)
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
	return k.sch.getMastersResources()
}

// launchJob launches a job, delegating on the scheduler.launchJob()
func (k *masterKernel) launchJob(jobName string, memUsage uint64, imageName string) string {
	return k.sch.launchJob(jobName, memUsage, imageName)
}

// stopJob attempts to stop a job, deleting the container
func (k *masterKernel) stopJob(jobID string) string {
	return k.sch.stopJob(jobID)
}

// getJobsTable retrieves all jobs data
func (k *masterKernel) getJobsList() map[string]jobData {
	return k.sch.getJobsTable()
}

// -------------------------- Main eventLoop -------------------------

// eventLoop for the masterKernel to handle all other modules events.
func (k *masterKernel) eventLoop() {
	for {
		select {
		case newLeader := <-k.newLeaderCh:
			k.restartscheduler(newLeader)
		}
	}
}
