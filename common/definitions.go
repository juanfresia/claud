package common

import (
	"github.com/satori/go.uuid"
)

type EventType int

const (
	EV_NULL EventType = iota
	EV_ACK
	EV_RES_F
	EV_RES_L
	EV_JOB_L
	EV_JOB_FF
	EV_JOBEND_L
	EV_JOBEND_F
	EV_JOBEND_FF
)

type Event struct {
	Type    EventType
	Payload interface{}
}

// ----------------------- Data type definitions ----------------------

// NodeResourcesData represents the resources of a master node
type NodeResourcesData struct {
	MasterUuid uuid.UUID
	MemFree    uint64
	MemTotal   uint64
}

// JobState tracks the state of a running job.
type JobState int

const (
	JOB_RUNNING JobState = iota
	JOB_FINISHED
	JOB_FAILED
)

func (js JobState) String() string {
	strMap := [...]string{
		"RUNNING",
		"FINISHED",
		"FAILED",
	}
	return strMap[js]
}

// JobData represents all the info of a running/to run job
type JobData struct {
	JobName        string
	ImageName      string
	MemUsage       uint64
	AssignedMaster string
	JobId          string
	JobStatus      JobState
}

type ConnectionMessage struct {
	MasterResources map[string]NodeResourcesData
	JobsTable       map[string]JobData
}
