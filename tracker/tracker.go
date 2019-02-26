package tracker

import (
	"bytes"
	"encoding/gob"
	"github.com/juanfresia/claud/logger"
	"github.com/satori/go.uuid"
	"net"
	"strings"
	"sync"
	"time"
)

// ----------------------- Data type definitions ----------------------

const (
	multicastAddr   = "224.0.0.28:1504"
	maxDatagramSize = 8192
	KeepAliveTmr    = 4 * time.Second
	DefunctTmr      = 8 * time.Second
	LearningTmr     = 16 * time.Second
	maxNodes        = 50
)

// nodeState keeps track of the leader election state.
type nodeState int

const (
	NULL_STATE nodeState = iota
	LEADER
	NOT_LEADER
	ANARCHY
	SLAVE
	FREE_SLAVE
)

func (ms nodeState) String() string {
	strMap := [...]string{
		"NULL STATE",
		"LEADER",
		"NOT LEADER",
		"IN ANARCHY",
		"SLAVE",
		"FREE SLAVE",
	}
	return strMap[ms]
}

type KeepAliveMessage struct {
	Uuid  string
	State nodeState
}

// nodeData stores the UUID, UDP address and defunct timer of
// a master node.
type nodeData struct {
	uuid  string
	state nodeState
	addr  *net.UDPAddr
	timer *time.Timer
}

// Tracker goal is to keep a table of alive masters nodes for
// choosing a leader. It reads and writes the UDP multicastAddr.
type Tracker struct {
	mu           *sync.Mutex
	clusterSize  int
	aliveMasters map[string]nodeData
	state        *nodeState
	nodeUuid     uuid.UUID
	leaderUuid   *string
	anarchyTmr   *time.Timer

	keepAliveCh  chan nodeData
	deadMasterCh chan nodeData
	newLeaderCh  chan<- string
}

// ----------------------------- Functions ----------------------------

// NewTracker creates a Tracker and launches goroutines
// for listening and writing the UDP multicast address.
func NewTracker(newLeaderCh chan<- string, nodeUuid uuid.UUID, clusterSize uint, isMaster bool) Tracker {
	mt := &Tracker{newLeaderCh: newLeaderCh}
	mt.state = new(nodeState)
	mt.clusterSize = int(clusterSize)
	mt.nodeUuid = nodeUuid
	mt.leaderUuid = new(string)
	if isMaster {
		*mt.state = ANARCHY
	} else {
		*mt.state = FREE_SLAVE
		logger.Logger.Info("Started node as slave!")
	}

	*mt.leaderUuid = "NO LEADER"
	mt.aliveMasters = make(map[string]nodeData)

	mt.keepAliveCh = make(chan nodeData, maxNodes)
	mt.deadMasterCh = make(chan nodeData, maxNodes)

	mt.anarchyTmr = time.NewTimer(LearningTmr)

	mt.mu = &sync.Mutex{}

	go mt.keepAliveSender(multicastAddr)
	go mt.listenMulticastUDP(multicastAddr)
	go mt.eventLoop()
	return *mt
}

// keepAliveSender forwards a keep alive message into the UDP
// multicast address every KeepAliveTmr seconds.
func (mt *Tracker) keepAliveSender(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		logger.Logger.Error("UDP socket creation failed: " + err.Error())
	}
	c, err := net.DialUDP("udp", nil, addr)

	// TODO: change this for a ticker (to be able to gracefully quit)
	for {
		msg := KeepAliveMessage{mt.nodeUuid.String(), *mt.state}
		//fmt.Printf("UUID/state when sending: %v/%v\n", msg.Uuid[:8], msg.State)
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
			logger.Logger.Error("gob encoding failed for keepalive message: " + err.Error())
		}
		c.Write(buf.Bytes())
		buf.Reset()
		time.Sleep(KeepAliveTmr)
	}
}

// listenMulticastUDP keeps track of the keep alive messages received
// on the multicast UDP address. It forwards the master UUID read into
// the Tracker main thread.
func (mt *Tracker) listenMulticastUDP(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		logger.Logger.Error("UDP socket creation failed: " + err.Error())
	}
	l, err := net.ListenMulticastUDP("udp", nil, addr)
	l.SetReadBuffer(maxDatagramSize)

	// Keep listening for updates
	var msg KeepAliveMessage
	//fmt.Printf("State just before listening: %v\n", msg.State)
	buf := make([]byte, maxDatagramSize)
	for {
		n, src, err := l.ReadFromUDP(buf)
		if err != nil {
			logger.Logger.Error("ReadFromUDP failed: " + err.Error())
			continue
		}
		if err = gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&msg); err != nil {
			logger.Logger.Error("gob decodidng failed for keepalive message: " + err.Error())
			continue
		}
		if (msg.State != FREE_SLAVE) && (msg.State != SLAVE) {
			mt.keepAliveCh <- nodeData{uuid: msg.Uuid, addr: src, state: msg.State}
		}
		//fmt.Printf("UUID/state just after listening: %v/%v\n", msg.Uuid[:8], msg.State)
	}
}

// trackNode refreshes some master's entry in the alive master
// nodes table, launching a defunct timer every time. Returns true if
// a new master has appeared (meaning a new leader election should
// take place on the Trackers of all master nodes).
func (mt *Tracker) trackNode(data nodeData) bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	masterUuid := data.uuid

	newMaster := false
	_, present := mt.aliveMasters[masterUuid]
	if present {
		mt.aliveMasters[masterUuid].timer.Stop()
	} else {
		logger.Logger.Info("A new master has appeared!")
		newMaster = true
	}

	killMasterNode := func() {
		mt.deadMasterCh <- mt.aliveMasters[masterUuid]
	}

	data.timer = time.AfterFunc(DefunctTmr, killMasterNode)
	mt.aliveMasters[masterUuid] = data
	return newMaster
}

// killDeadMaster erases a master from the alive master nodes table.
func (mt *Tracker) killDeadMaster(masterUuid string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	logger.Logger.Warning("Master " + masterUuid + " has died!")
	delete(mt.aliveMasters, masterUuid)
}

// resetAnarchyTimer restarts the anarchyTmr of the Tracker.
// This function should only be called in the main Tracker
// main thread to prevent race conditions.
func (mt *Tracker) resetAnarchyTimer(lockOnStop bool) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if (*mt.state == ANARCHY) || (*mt.state == FREE_SLAVE) {
		if !mt.anarchyTmr.Stop() {
			if lockOnStop {
				<-mt.anarchyTmr.C
			}
		}
	}
	if (*mt.state == FREE_SLAVE) || (*mt.state == SLAVE) {
		logger.Logger.Info("Leaders are in ANARCHY! Slave is free!")
		*mt.state = FREE_SLAVE
	} else {
		logger.Logger.Info("No leader detected, embrace ANARCHY!")
		*mt.state = ANARCHY
	}
	*mt.leaderUuid = "NO LEADER"
	mt.anarchyTmr = time.NewTimer(LearningTmr)
}

// chooseLeader takes the alive master node with minimum UUID from
// the table and makes it leader. It also forwards the identity
// of such new leader into the channel that communicates with
// the masterKernel.
func (mt *Tracker) chooseLeader() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	logger.Logger.Info("ANARCHY has ended, choosing a new leader")
	var leader nodeData
	for _, node := range mt.aliveMasters {
		leader = node
		break
	}

	for _, node := range mt.aliveMasters {
		if node.uuid <= leader.uuid {
			leader = node
		}
	}

	mt.gotNewLeader(leader)
}

func (mt *Tracker) gotNewLeader(leader nodeData) {
	*mt.leaderUuid = leader.uuid
	logger.Logger.Info("We all hail the new leader: " + leader.uuid)
	if leader.uuid == mt.nodeUuid.String() {
		*mt.state = LEADER
	} else if *mt.state == FREE_SLAVE {
		*mt.state = SLAVE
	} else if *mt.state != SLAVE {
		*mt.state = NOT_LEADER
	}
	logger.Logger.Info("New node state: " + mt.state.String())
	// Forward the new leader IP address back to kernel
	leaderIP := leader.addr.String()
	leaderIP = leaderIP[:strings.Index(leaderIP, ":")]
	mt.newLeaderCh <- leaderIP
}

// -------------------------- Main eventLoop -------------------------

// eventLoop for the Tracker.
func (mt *Tracker) eventLoop() {
	for {
		select {
		case data := <-mt.keepAliveCh:
			//fmt.Printf("UUID/state after poping from keepAliveCh: %v/%v\n", data.uuid[:8], data.state)
			new_master := mt.trackNode(data)
			if new_master && (len(mt.aliveMasters) < mt.clusterSize) {
				mt.resetAnarchyTimer(true)
			} else if (data.state == LEADER) && ((*mt.state == ANARCHY) || (*mt.state == FREE_SLAVE)) {
				logger.Logger.Info("Joining an already existing cluster")
				mt.gotNewLeader(data)
			}
		case corpse := <-mt.deadMasterCh:
			wasLeader := corpse.uuid == *mt.leaderUuid
			mt.killDeadMaster(corpse.uuid)
			if len(mt.aliveMasters) < mt.clusterSize || wasLeader {
				mt.resetAnarchyTimer(true)
			}
		case <-mt.anarchyTmr.C:
			if (*mt.state == ANARCHY) || (*mt.state == FREE_SLAVE) {
				if len(mt.aliveMasters) >= mt.clusterSize {
					mt.chooseLeader()
				} else {
					mt.resetAnarchyTimer(false)
				}
			}
		}
	}
}

// TODO: Review the functions down here after refactor is completed
// (most are master responsabilities, not Tracker)

// GetMasters returns a slice containing the UUID of all the
// alive master nodes.
func (mt *Tracker) GetMasters() []string {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	aliveMasters := make([]string, len(mt.aliveMasters))
	i := 0
	for uuid := range mt.aliveMasters {
		aliveMasters[i] = uuid
		i++
	}
	return aliveMasters
}

// GetNodeState returns a string representing the leader state
// of this master (leader, not leader, or anarchy).
func (mt *Tracker) GetNodeState() string {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return mt.state.String()
}

// GetLeaderId retrieves the leader UUID (or a "no leader" message
// if no leader has been chosen yet).
func (mt *Tracker) GetLeaderId() string {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return *mt.leaderUuid
}

// ImLeader simply returns true if this master is the leader.
func (mt *Tracker) ImLeader() bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return (*mt.state == LEADER)
}
