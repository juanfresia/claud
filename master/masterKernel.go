package master

import (
	"fmt"
	"time"
	//"github.com/satori/go.uuid"
)

// ----------------------- Data type definitions ----------------------

// masterKernel is the core of all masters. The different master
// modules interact with it.
type masterKernel struct {
	sch         scheduler
	mt          mastersTracker
	newLeaderCh chan string
}

// -------------------------------------------------------------------
//                               Functions
// -------------------------------------------------------------------

// newMasterKernel creates a new masterKernel together with
// its scheduler and mastersTracker.
func newMasterKernel(mem uint64) masterKernel {
	k := &masterKernel{}

	StartupMasterLog("./kernel-" + myUuid.String()[:8] + ".log")
	k.newLeaderCh = make(chan string, 10)
	k.mt = newMastersTracker(k.newLeaderCh)
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
		time.Sleep(learningTmr)
	}
	mastersAmount := int32(len(k.getMasters()))
	k.sch.openConnections(leaderIp, imLeader, mastersAmount)
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
