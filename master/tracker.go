package master

import (
	"bytes"
	"encoding/gob"
	"net"
	"strings"
	"sync"
	"time"
)

// ----------------------- Data type definitions ----------------------

const (
	multicastAddr   = "224.0.0.28:1504"
	maxDatagramSize = 8192
	keepAliveTmr    = 5 * time.Second
	defunctTmr      = 10 * time.Second
	learningTmr     = 20 * time.Second
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

type KeepAliveMessage struct {
	Uuid  string
	State masterState
}

// masterData stores the UUID, UDP address and defunct timer of
// a master node.
type masterData struct {
	uuid  string
	addr  *net.UDPAddr
	timer *time.Timer
}

// tracker goal is to keep a table of alive masters nodes for
// choosing a leader. It reads and writes the UDP multicastAddr.
type tracker struct {
	mu          *sync.Mutex
	clusterSize int
	aliveNodes  map[string]masterData
	state       *masterState
	leaderUuid  *string
	anarchyTmr  *time.Timer

	keepAliveCh  chan masterData
	deadMasterCh chan string
	newLeaderCh  chan<- string
}

// ----------------------------- Functions ----------------------------

// newtracker creates m tracker and launches goroutines
// for listening and writing the UDP multicast address.
func newtracker(newLeaderCh chan<- string, clusterSize uint) tracker {
	mt := &tracker{newLeaderCh: newLeaderCh}
	mt.state = new(masterState)
	mt.clusterSize = int(clusterSize)
	mt.leaderUuid = new(string)
	*mt.state = ANARCHY
	*mt.leaderUuid = "NO LEADER"
	mt.aliveNodes = make(map[string]masterData)

	mt.keepAliveCh = make(chan masterData, maxMasterAmount)
	mt.deadMasterCh = make(chan string, maxMasterAmount)

	mt.anarchyTmr = time.NewTimer(learningTmr)

	mt.mu = &sync.Mutex{}

	go mt.keepAliveSender(multicastAddr)
	go mt.listenMulticastUDP(multicastAddr)
	go mt.eventLoop()
	return *mt
}

// keepAliveSender forwards a keep alive message into the UDP
// multicast address every keepAliveTmr seconds.
func (mt *tracker) keepAliveSender(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		masterLog.Error("UDP socket creation failed: " + err.Error())
	}
	c, err := net.DialUDP("udp", nil, addr)
	msg := KeepAliveMessage{myUuid.String(), *mt.state}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
		masterLog.Error("gob encoidng failed for keepalive message: " + err.Error())
	}

	// TODO: change this for a ticker (to be able to gracefully quit)
	for {
		c.Write(buf.Bytes())
		time.Sleep(keepAliveTmr)
	}
}

// listenMulticastUDP keeps track of the keep alive messages received
// on the multicast UDP address. It forwards the master UUID read into
// the tracker main thread.
func (mt *tracker) listenMulticastUDP(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		masterLog.Error("UDP socket creation failed: " + err.Error())
	}
	l, err := net.ListenMulticastUDP("udp", nil, addr)
	l.SetReadBuffer(maxDatagramSize)

	// Keep listening for updates
	var msg KeepAliveMessage
	buf := make([]byte, maxDatagramSize)
	for {
		n, src, err := l.ReadFromUDP(buf)
		if err != nil {
			masterLog.Error("ReadFromUDP failed: " + err.Error())
			continue
		}
		if err = gob.NewDecoder(bytes.NewReader(buf[:n])).Decode(&msg); err != nil {
			masterLog.Error("gob decodidng failed for keepalive message: " + err.Error())
			continue
		}
		uuidReceived := msg.Uuid
		mt.keepAliveCh <- masterData{uuid: uuidReceived, addr: src}
	}
}

// trackMasterNode refreshes some master's entry in the alive master
// nodes table, launching a defunct timer every time. Returns true if
// a new master has appeared (meaning a new leader election should
// take place on the trackers of all master nodes).
func (mt *tracker) trackMasterNode(masterUuid string, addr *net.UDPAddr) bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	newMaster := false
	_, present := mt.aliveNodes[masterUuid]
	if present {
		mt.aliveNodes[masterUuid].timer.Stop()
	} else {
		masterLog.Info("A new master has appeared! Dropping current leader.")
		newMaster = true
	}

	killMasterNode := func() {
		mt.deadMasterCh <- masterUuid
	}

	timer := time.AfterFunc(defunctTmr, killMasterNode)
	mt.aliveNodes[masterUuid] = masterData{masterUuid, addr, timer}
	return newMaster
}

// killDeadMaster erases a master from the alive master nodes table.
func (mt *tracker) killDeadMaster(masterUuid string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	masterLog.Info("WARNING: Master " + masterUuid + " has died!")
	delete(mt.aliveNodes, masterUuid)
}

// resetAnarchyTimer restarts the anarchyTmr of the tracker.
// This function should only be called in the main tracker
// main thread to prevent race conditions.
func (mt *tracker) resetAnarchyTimer() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	masterLog.Info("No leader detected, embrace ANARCHY!")
	if *mt.state == ANARCHY {
		if !mt.anarchyTmr.Stop() {
			<-mt.anarchyTmr.C
		}
	}
	*mt.state = ANARCHY
	*mt.leaderUuid = "NO LEADER"
	mt.anarchyTmr = time.NewTimer(learningTmr)
}

// chooseLeader takes the alive master node with minimum UUID from
// the table and makes it leader. It also forwards the identity
// of such new leader into the channel that communicates with
// the masterKernel.
func (mt *tracker) chooseLeader() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	masterLog.Info("ANARCHY has ended. Choosing a new leader")
	leader := mt.aliveNodes[myUuid.String()]
	for _, node := range mt.aliveNodes {
		if node.uuid <= leader.uuid {
			leader = node
		}
	}

	*mt.leaderUuid = leader.uuid
	masterLog.Info("We all hail the new leader: " + leader.uuid)
	if leader.uuid == myUuid.String() {
		*mt.state = LEADER
	} else {
		*mt.state = NOT_LEADER
	}
	masterLog.Info("New leader state: " + mt.state.String())
	// Forward the new leader IP address back to kernel
	leaderIP := leader.addr.String()
	leaderIP = leaderIP[:strings.Index(leaderIP, ":")]
	mt.newLeaderCh <- leaderIP
}

// -------------------------- Main eventLoop -------------------------

// eventLoop for the tracker.
func (mt *tracker) eventLoop() {
	for {
		select {
		case data := <-mt.keepAliveCh:
			new_master := mt.trackMasterNode(data.uuid, data.addr)
			if new_master && (len(mt.aliveNodes) < mt.clusterSize) {
				mt.resetAnarchyTimer()
			}
		case corpse := <-mt.deadMasterCh:
			mt.killDeadMaster(corpse)
			if len(mt.aliveNodes) < mt.clusterSize {
				mt.resetAnarchyTimer()
			}
		case <-mt.anarchyTmr.C:
			if len(mt.aliveNodes) >= mt.clusterSize {
				mt.chooseLeader()
			} else {
				mt.resetAnarchyTimer()
			}
		}
	}
}

// TODO: Review the functions down here after refactor is completed
// (most are master responsabilities, not tracker)

// getMasters returns a slice containing the UUID of all the
// alive master nodes.
func (mt *tracker) getMasters() []string {
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

// getLeaderState returns a string representing the leader state
// of this master (leader, not leader, or anarchy).
func (mt *tracker) getLeaderState() string {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return mt.state.String()
}

// getLeaderId retrieves the leader UUID (or a "no leader" message
// if no leader has been chosen yet).
func (mt *tracker) getLeaderId() string {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return *mt.leaderUuid
}

// imLeader simply returns true if this master is the leader.
func (mt *tracker) imLeader() bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return (*mt.state == LEADER)
}
