// Package master keeps together all the necessary stuff to launch
// a master node of claud.
package master

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
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
func newMasterServer(mem uint64) *MasterServer {
	m := &MasterServer{}
	m.kernel = newMasterKernel(mem)
	return m
}

// getMyStatus provides info on this master's status.
func (m *MasterServer) getMyStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	myStatusData := make(map[string]string)
	myStatusData["my_UUID"] = myUuid.String()
	w.WriteHeader(http.StatusOK)
	response, _ := json.Marshal(myStatusData)
	w.Write(response)
}

// getLeaderStatus shows how the master leader election is going and
// which master is the real leader.
func (m *MasterServer) getLeaderStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	leaderStatusData := make(map[string]string)
	leaderStatusData["leader_status"] = m.kernel.getLeaderState()
	leaderStatusData["leader_UUID"] = m.kernel.getLeaderId()
	w.WriteHeader(http.StatusOK)
	response, _ := json.Marshal(leaderStatusData)
	w.Write(response)
}

// getAliveMasters prints a nice list of all masters in the cluster.
func (m *MasterServer) getAliveMasters(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	aliveMasters := m.kernel.getMasters()
	masterResources := m.kernel.getMastersResources()
	masterDataArray := make([]interface{}, len(aliveMasters))
	i := 0
	for _, uuid := range aliveMasters {
		masterData := make(map[string]interface{})
		masterData["UUID"] = uuid
		if resourceData, present := masterResources[uuid]; present {
			masterData["free_memory"] = resourceData.MemFree
			masterData["total_memory"] = resourceData.MemTotal
		}
		masterDataArray[i] = masterData
		i += 1
	}

	aliveMastersResponse := make(map[string]interface{})
	aliveMastersResponse["alive_masters"] = masterDataArray

	response, _ := json.Marshal(aliveMastersResponse)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// --------------------------- Main function ---------------------------

// LaunchMaster starts a master on the given IP and port.
func LaunchMaster(masterIp, port string, mem uint64) {
	myUuid = uuid.NewV4()
	m := newMasterServer(mem)
	server := mux.NewRouter()

	server.HandleFunc("/", m.getMyStatus).Methods("GET")
	server.HandleFunc("/masters", m.getAliveMasters).Methods("GET")
	server.HandleFunc("/leader", m.getLeaderStatus).Methods("GET")
	http.ListenAndServe(masterIp+":"+port, server)
}
