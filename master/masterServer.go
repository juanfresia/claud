// Package master keeps together all the necessary stuff to launch
// a master node of claud.
package master

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"net/http"
)

// The UUID identifier of this master node
var myUuid uuid.UUID

// --------------------------- Server struct ---------------------------

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
		thisMasterData := make(map[string]interface{})
		thisMasterData["UUID"] = uuid
		if resourceData, present := masterResources[uuid]; present {
			thisMasterData["free_memory"] = resourceData.MemFree
			thisMasterData["total_memory"] = resourceData.MemTotal
		}
		masterDataArray[i] = thisMasterData
		i += 1
	}

	aliveMastersResponse := make(map[string]interface{})
	aliveMastersResponse["alive_masters"] = masterDataArray

	response, _ := json.Marshal(aliveMastersResponse)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// getJobsList fetches the jobs list and returns it json formatted.
func (m *MasterServer) getJobsList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	jobsList := m.kernel.getJobsList()
	jobsDataArray := make([]interface{}, len(jobsList))
	i := 0
	for _, data := range jobsList {
		thisJobData := make(map[string]interface{})
		thisJobData["job_full_name"] = data.JobName + "-" + data.JobId
		thisJobData["job_id"] = data.JobId
		thisJobData["image"] = data.ImageName
		thisJobData["asigned_master"] = data.AsignedMaster
		thisJobData["status"] = data.JobStatus.String()

		jobsDataArray[i] = thisJobData
		i += 1
	}

	jobListResponse := make(map[string]interface{})
	jobListResponse["jobs"] = jobsDataArray

	response, _ := json.Marshal(jobListResponse)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// TODO: Somehow kill a running job
func (m *MasterServer) stopJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	jobID := mux.Vars(r)["id"]

	jobsList := m.kernel.getJobsList()

	jobToKill, exists := jobsList[jobID]
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("{\"message\": \"Job " + jobID + " does not exist\"}"))
		return
	}

	jobHostID := jobToKill.AsignedMaster
	if jobHostID != myUuid.String() {
		// TODO: Forward to master node where job is running
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("{\"message\": \"Not running in this host\"}"))
		return
	}
	// TODO: add error checking
	jobID = m.kernel.stopJob(jobID)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"job_id\": \"" + jobID + "\"}"))
}

// launchNewJob launches a new job based on the request body received.
// It forwards the user a message with the job id.
func (m *MasterServer) launchNewJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var newJob struct {
		Mem   uint64
		Name  string
		Image string
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("%v", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("{\"message\": \"Bad arguments received\"}"))
		return
	}

	err = json.Unmarshal(body, &newJob)
	if err != nil {
		fmt.Printf("%v", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("{\"message\": \"Bad arguments received\"}"))
		return
	}

	if m.kernel.getLeaderId() != myUuid.String() {
		// TODO: Forward to master leader somehow
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("{\"message\": \"Not the leader\"}"))
		return
	}
	// TODO: refactor this to pass only one job type element
	jobId := m.kernel.launchJob(newJob.Name, newJob.Mem, newJob.Image)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"job_id\": \"" + jobId + "\"}"))
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
	server.HandleFunc("/jobs", m.getJobsList).Methods("GET")
	server.HandleFunc("/jobs", m.launchNewJob).Methods("POST")
	server.HandleFunc("/jobs/{id}", m.stopJob).Methods("DELETE")
	http.ListenAndServe(masterIp+":"+port, server)
}
