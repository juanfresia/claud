package master

import (
	"fmt"
	"net/http"
)

// --------------------------- Master struct ---------------------------

type MasterServer struct {
	kernel MasterKernel
}

func newMaster() *MasterServer {
	m := &MasterServer{}
	m.kernel = newKernel()
	return m
}

func (m *MasterServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, my uuid is %v\n", m.kernel.uuid)
}

func (m *MasterServer) leaderStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%v\n", m.kernel.GetState())
}

func (m *MasterServer) aliveMasters(w http.ResponseWriter, r *http.Request) {
	msg := m.kernel.GetMasters()
	fmt.Fprintf(w, msg)
}

// --------------------------- Main function ---------------------------

func LaunchMaster(masterIp, port string) {
	m := newMaster()

	http.HandleFunc("/", m.statusHandler)
	http.HandleFunc("/masters", m.aliveMasters)
	http.HandleFunc("/leader", m.leaderStatus)
	http.ListenAndServe(masterIp+":"+port, nil)
}
