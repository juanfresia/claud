package slave

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	. "github.com/juanfresia/claud/common"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"net/http"
)

// The UUID identifier of this master node
var myUuid uuid.UUID

// --------------------------- Server struct ---------------------------

// SlaveServer provides some nice HTTP API for the claud users.
type SlaveServer struct {
	kernel slaveKernel
}

// newSlaveServer creates a new SlaveServer with an already
// initialized slaveKernel.
func newSlaveServer(mem uint64, clusterSize uint) *SlaveServer {
	s := &SlaveServer{}
	s.kernel = newSlaveKernel(mem, clusterSize)
	return s
}

// getMyStatus provides info on this slave's status.
func (s *SlaveServer) getMyStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	myStatusData := make(map[string]string)
	myStatusData["my_UUID"] = myUuid.String()
	w.WriteHeader(http.StatusOK)
	response, _ := json.Marshal(myStatusData)
	w.Write(response)
}

// getLeaderStatus shows how the master leader election is going and
// which master is the real leader.
func (s *SlaveServer) getLeaderStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	leaderStatusData := make(map[string]string)
	leaderStatusData["leader_status"] = s.kernel.getLeaderState()
	leaderStatusData["leader_UUID"] = s.kernel.getLeaderId()
	w.WriteHeader(http.StatusOK)
	response, _ := json.Marshal(leaderStatusData)
	w.Write(response)
}

// getAliveMasters prints a nice list of all masters in the cluster.
func (s *SlaveServer) getAliveMasters(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	aliveMasters := s.kernel.getMasters()
	masterDataArray := make([]interface{}, len(aliveMasters))
	leaderId := s.kernel.getLeaderId()
	i := 0
	for _, uuid := range aliveMasters {
		thisMasterData := make(map[string]interface{})
		thisMasterData["UUID"] = uuid
		if uuid == leaderId {
			thisMasterData["status"] = "LEADER"
		} else {
			thisMasterData["status"] = "NOT LEADER"
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

// getAliveMasters prints a nice list of all masters in the cluster.
func (s *SlaveServer) getSlavesData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	nodeResources := s.kernel.getMastersResources()
	slaveResources := make(map[string]NodeResourcesData)
	for uuid, resourceData := range nodeResources {
		slaveResources[uuid] = resourceData
	}
	// Skip nodes that are masters <- Leave or not?
	/*  aliveMasters := s.kernel.getMasters()
	for _, uuid := range aliveMasters {
		delete(slaveResources, uuid)
	}*/
	slaveDataArray := make([]interface{}, len(slaveResources))
	i := 0
	for uuid, resourceData := range slaveResources {
		thisNodeData := make(map[string]interface{})
		thisNodeData["UUID"] = uuid
		thisNodeData["free_memory"] = resourceData.MemFree
		thisNodeData["total_memory"] = resourceData.MemTotal
		slaveDataArray[i] = thisNodeData
		i += 1
	}

	slavesDataResponse := make(map[string]interface{})
	slavesDataResponse["alive_slaves"] = slaveDataArray

	response, _ := json.Marshal(slavesDataResponse)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// getJobsList fetches the jobs list and returns it json formatted.
func (s *SlaveServer) getJobsList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	jobsList := s.kernel.getJobsList()
	jobsDataArray := make([]interface{}, len(jobsList))
	i := 0
	for _, data := range jobsList {
		thisJobData := make(map[string]interface{})
		thisJobData["job_full_name"] = data.JobName + "-" + data.JobId
		thisJobData["job_id"] = data.JobId
		thisJobData["image"] = data.ImageName
		thisJobData["asigned_master"] = data.AssignedMaster
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
func (s *SlaveServer) stopJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	jobID := mux.Vars(r)["id"]

	jobsList := s.kernel.getJobsList()

	_, exists := jobsList[jobID]
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("{\"message\": \"Job " + jobID + " does not exist\"}"))
		return
	}

	// TODO: add error checking
	jobID = s.kernel.stopJob(jobID)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"job_id\": \"" + jobID + "\"}"))
}

// launchNewJob launches a new job based on the request body received.
// It forwards the user a message with the job id.
func (s *SlaveServer) launchNewJob(w http.ResponseWriter, r *http.Request) {
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

	if s.kernel.getLeaderId() != myUuid.String() {
		// TODO: Forward to master leader somehow
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("{\"message\": \"Not the leader\"}"))
		return
	}
	// TODO: refactor this to pass only one job type element
	jobId := s.kernel.launchJob(newJob.Name, newJob.Mem, newJob.Image)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"job_id\": \"" + jobId + "\"}"))
}

// --------------------------- Main function ---------------------------

func LaunchSlave(slaveIp, port string, mem uint64, mastersTotal uint) {
	myUuid = uuid.NewV4()
	s := newSlaveServer(mem, mastersTotal)
	server := mux.NewRouter()

	server.HandleFunc("/", s.getMyStatus).Methods("GET")
	server.HandleFunc("/masters", s.getAliveMasters).Methods("GET")
	server.HandleFunc("/slaves", s.getSlavesData).Methods("GET")
	server.HandleFunc("/leader", s.getLeaderStatus).Methods("GET")
	server.HandleFunc("/jobs", s.getJobsList).Methods("GET")
	server.HandleFunc("/jobs", s.launchNewJob).Methods("POST")
	server.HandleFunc("/jobs/{id}", s.stopJob).Methods("DELETE")
	http.ListenAndServe(slaveIp+":"+port, server)
}
