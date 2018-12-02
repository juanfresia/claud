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
	KeepAliveTmr    = 5 * time.Second
	DefunctTmr      = 10 * time.Second
	LearningTmr     = 20 * time.Second
)

// masterState keeps track of the leader election state.
type masterState int

const (
	NULL_STATE masterState = iota
	LEADER
	NOT_LEADER
	ANARCHY
)

func (ms masterState) String() string {
	strMap := [...]string{
		"NULL STATE",
		"LEADER",
		"NOT LEADER",
		"IN ANARCHY",
	}
	return strMap[ms]
}

type KeepAliveMessage struct {
	Uuid  string
	State masterState
}

// masterData stores the UUID, UDP address and defunct timer of
// a master node.
type masterData struct {
	uuid  string
	state masterState
	addr  *net.UDPAddr
	timer *time.Timer
}

// Tracker goal is to keep a table of alive masters nodes for
// choosing a leader. It reads and writes the UDP multicastAddr.
type Tracker struct {
	mu          *sync.Mutex
	clusterSize int
	aliveNodes  map[string]masterData
	state       *masterState
	nodeUuid uuid.UUID
	leaderUuid  *string
	anarchyTmr  *time.Timer

	keepAliveCh  chan masterData
	deadMasterCh chan masterData
	newLeaderCh  chan<- string
}

// ----------------------------- Functions ----------------------------

// NewTracker creates a Tracker and launches goroutines
// for listening and writing the UDP multicast address.
func NewTracker(newLeaderCh chan<- string, nodeUuid uuid.UUID, clusterSize uint) Tracker {
	mt := &Tracker{newLeaderCh: newLeaderCh}
	mt.state = new(masterState)
	mt.clusterSize = int(clusterSize)
	mt.nodeUuid = nodeUuid
	mt.leaderUuid = new(string)
	*mt.state = ANARCHY
	*mt.leaderUuid = "NO LEADER"
	mt.aliveNodes = make(map[string]masterData)

	mt.keepAliveCh = make(chan masterData, clusterSize)
	mt.deadMasterCh = make(chan masterData, clusterSize)

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
		mt.keepAliveCh <- masterData{uuid: msg.Uuid, addr: src, state: msg.State}
		//fmt.Printf("UUID/state just after listening: %v/%v\n", msg.Uuid[:8], msg.State)
	}
}

// trackMasterNode refreshes some master's entry in the alive master
// nodes table, launching a defunct timer every time. Returns true if
// a new master has appeared (meaning a new leader election should
// take place on the Trackers of all master nodes).
func (mt *Tracker) trackMasterNode(data masterData) bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	masterUuid := data.uuid

	newMaster := false
	_, present := mt.aliveNodes[masterUuid]
	if present {
		mt.aliveNodes[masterUuid].timer.Stop()
	} else {
		logger.Logger.Info("A new master has appeared!")
		newMaster = true
	}

	killMasterNode := func() {
		mt.deadMasterCh <- mt.aliveNodes[masterUuid]
	}

	data.timer = time.AfterFunc(DefunctTmr, killMasterNode)
	mt.aliveNodes[masterUuid] = data
	return newMaster
}

// killDeadMaster erases a master from the alive master nodes table.
func (mt *Tracker) killDeadMaster(masterUuid string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	logger.Logger.Warning("Master " + masterUuid + " has died!")
	delete(mt.aliveNodes, masterUuid)
}

// resetAnarchyTimer restarts the anarchyTmr of the Tracker.
// This function should only be called in the main Tracker
// main thread to prevent race conditions.
func (mt *Tracker) resetAnarchyTimer() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	logger.Logger.Info("No leader detected, embrace ANARCHY!")
	if *mt.state == ANARCHY {
		if !mt.anarchyTmr.Stop() {
			<-mt.anarchyTmr.C
		}
	}
	*mt.state = ANARCHY
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
	logger.Logger.Info("ANARCHY has ended. Choosing a new leader")
	leader := mt.aliveNodes[mt.nodeUuid.String()]
	for _, node := range mt.aliveNodes {
		if node.uuid <= leader.uuid {
			leader = node
		}
	}

	mt.gotNewLeader(leader)
}

func (mt *Tracker) gotNewLeader(leader masterData) {
	*mt.leaderUuid = leader.uuid
	logger.Logger.Info("We all hail the new leader: " + leader.uuid)
	if leader.uuid == mt.nodeUuid.String() {
		*mt.state = LEADER
	} else {
		*mt.state = NOT_LEADER
	}
	logger.Logger.Info("New leader state: " + mt.state.String())
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
			new_master := mt.trackMasterNode(data)
			if new_master && (len(mt.aliveNodes) < mt.clusterSize) {
				mt.resetAnarchyTimer()
			} else if (data.state == LEADER) && (*mt.state == ANARCHY) {
				logger.Logger.Info("Joining an already existing cluster")
				mt.gotNewLeader(data)
			}
		case corpse := <-mt.deadMasterCh:
			wasLeader := corpse.uuid == *mt.leaderUuid
			mt.killDeadMaster(corpse.uuid)
			if len(mt.aliveNodes) < mt.clusterSize || wasLeader {
				mt.resetAnarchyTimer()
			}
		case <-mt.anarchyTmr.C:
			if *mt.state == ANARCHY {
				if len(mt.aliveNodes) >= mt.clusterSize {
					mt.chooseLeader()
				} else {
					mt.resetAnarchyTimer()
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
	aliveMasters := make([]string, len(mt.aliveNodes))
	i := 0
	for uuid := range mt.aliveNodes {
		aliveMasters[i] = uuid
		i++
	}
	return aliveMasters
}

// GetLeaderState returns a string representing the leader state
// of this master (leader, not leader, or anarchy).
func (mt *Tracker) GetLeaderState() string {
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
