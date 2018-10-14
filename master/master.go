// Package master keeps together all the necessary stuff to launch
// a master node of claud.
package master

import (
	"github.com/satori/go.uuid"
)

// The UUID identifier of this master node
var myUuid uuid.UUID

const (
	maxMasterAmount = 100
)

// masterState keeps track of the leader election state.
type masterState int

const (
	LEADER masterState = iota
	NOT_LEADER
	ANARCHY
)

func (ms masterState) String() string {
	strMap := [...]string{
		"LEADER",
		"NOT LEADER",
		"IN ANARCHY",
	}
	return strMap[ms]
}

// masterResourcesData represents the resources of a master node
type masterResourcesData struct {
	MasterUuid uuid.UUID
	MemFree    uint64
	MemTotal   uint64
}

// jobState tracks the state of a running job.
type jobState int

const (
	JOB_RUNNING jobState = iota
	JOB_FINISHED
	JOB_FAILED
)

func (js jobState) String() string {
	strMap := [...]string{
		"RUNNING",
		"FINISHED",
		"FAILED",
	}
	return strMap[js]
}

// jobData represents all the info of a running/to run job
type jobData struct {
	JobName       string
	MemUsage      uint64
	AsignedMaster string
	JobId         string
	JobStatus     jobState
}
