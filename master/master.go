// Package master keeps together all the necessary stuff to launch
// a master node of claud.
package master

import (
	"fmt"
	"net/http"

	"github.com/satori/go.uuid"
)

var myUuid uuid.UUID

// --------------------------- Master struct ---------------------------

// MasterServer provides some nice HTTP API for the claud users.
type MasterServer struct {
	kernel masterKernel
}

// newMasterServer creates a new MasterServer with an already
// initialized masterKernel.
func newMasterServer() *MasterServer {
	m := &MasterServer{}
	m.kernel = newMasterKernel()
	return m
}

// myStatus provides info on this master's status.
func (m *MasterServer) myStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I am master %v\n", myUuid.String())
}

// leaderStatus shows how the master leader election is going and
// which master is the real leader.
func (m *MasterServer) leaderStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "My leader status: %v\n", m.kernel.getLeaderState())
	fmt.Fprintf(w, "Leader UUID is: %s\n", m.kernel.getLeaderId())
}

// aliveMasters prints a nice list of all masters in the cluster.
func (m *MasterServer) aliveMasters(w http.ResponseWriter, r *http.Request) {
	msg := "ALIVE MASTER NODES\n"
	aliveMasters := m.kernel.getMasters()
	for uuid, ipPort := range aliveMasters {
		msg += ("UUID: " + uuid + " IP:PORT: " + ipPort + "\n")
	}
	fmt.Fprintf(w, msg)
}

// --------------------------- Main function ---------------------------

// LaunchMaster starts a master on the given IP and port.
func LaunchMaster(masterIp, port string) {
	myUuid = uuid.NewV4()
	m := newMasterServer()

	http.HandleFunc("/", m.myStatus)
	http.HandleFunc("/masters", m.aliveMasters)
	http.HandleFunc("/leader", m.leaderStatus)
	http.ListenAndServe(masterIp+":"+port, nil)
}
