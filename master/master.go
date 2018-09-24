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
	fmt.Fprintf(w, "I've received %v messages\n", m.kernel.count)
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
	http.ListenAndServe(masterIp+":"+port, nil)
}
