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
func newMasterKernel() masterKernel {
	k := &masterKernel{}

	StartupMasterLog("./kernel-" + myUuid.String()[:8] + ".log")
	k.newLeaderCh = make(chan string, 10)
	k.mt = newMastersTracker(k.newLeaderCh)
	k.sch = newScheduler()

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
	k.sch.openConnections(leaderIp, imLeader)
}

// ------------------ Functions for the masterServer -----------------

// getMasters returns a map containing info of all the master
// nodes alive, in the form {"uuid": "IP:PORT"}.
func (k *masterKernel) getMasters() map[string]string {
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
