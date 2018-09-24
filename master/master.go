package master

import (
	"fmt"
	"net/http"
)

// --------------------------- Master struct ---------------------------

type MasterServer struct {
	kernel MasterKernel
}

func newMasterServer() *MasterServer {
	m := &MasterServer{}
	m.kernel = newMasterKernel()
	return m
}

func (m *MasterServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I am master %v\n", m.kernel.uuid)
}

func (m *MasterServer) leaderStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "My leader status: %v\n", m.kernel.GetLeaderState())
	fmt.Fprintf(w, "Leader UUID is: %s\n", m.kernel.GetLeaderId())
}

func (m *MasterServer) aliveMasters(w http.ResponseWriter, r *http.Request) {
	msg := m.kernel.GetMasters()
	fmt.Fprintf(w, msg)
}

// --------------------------- Main function ---------------------------

func LaunchMaster(masterIp, port string) {
	m := newMasterServer()

	http.HandleFunc("/", m.statusHandler)
	http.HandleFunc("/masters", m.aliveMasters)
	http.HandleFunc("/leader", m.leaderStatus)
	http.ListenAndServe(masterIp+":"+port, nil)
}
