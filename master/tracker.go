package master

import (
	"net"
	"strings"
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
	aliveNodes map[string]masterData
	state      *masterState
	leaderUuid *string
	anarchyTmr *time.Timer

	keepAliveCh  chan masterData
	deadMasterCh chan string
	newLeaderCh  chan<- string
}

// ----------------------------- Functions ----------------------------

// newtracker creates m tracker and launches goroutines
// for listening and writing the UDP multicast address.
func newtracker(newLeaderCh chan<- string) tracker {
	mt := &tracker{newLeaderCh: newLeaderCh}
	mt.state = new(masterState)
	mt.leaderUuid = new(string)
	*mt.state = ANARCHY
	*mt.leaderUuid = "NO LEADER"
	mt.aliveNodes = make(map[string]masterData)

	mt.keepAliveCh = make(chan masterData, maxMasterAmount)
	mt.deadMasterCh = make(chan string, maxMasterAmount)

	mt.anarchyTmr = time.NewTimer(learningTmr)

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
	msg := myUuid.String()
	// TODO: change this for a ticker (to be able to gracefully quit)
	for {
		c.Write([]byte(msg))
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
	msg := make([]byte, maxDatagramSize)
	for {
		n, src, err := l.ReadFromUDP(msg)
		if err != nil {
			masterLog.Error("ReadFromUDP failed: " + err.Error())
		}
		uuidReceived := string(msg[:n])
		mt.keepAliveCh <- masterData{uuid: uuidReceived, addr: src}
	}
}

// trackMasterNode refreshes some master's entry in the alive master
// nodes table, launching a defunct timer every time. Returns true if
// a new master has appeared (meaning a new leader election should
// take place on the trackers of all master nodes).
func (mt *tracker) trackMasterNode(masterUuid string, addr *net.UDPAddr) bool {
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
	masterLog.Info("WARNING: Master " + masterUuid + " has died!")
	delete(mt.aliveNodes, masterUuid)
}

// resetAnarchyTimer restarts the anarchyTmr of the tracker.
// This function should only be called in the main tracker
// main thread to prevent race conditions.
func (mt *tracker) resetAnarchyTimer() {
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
	masterLog.Info("ANARCHY has ended. Choosing a new leader")
	leader := mt.aliveNodes[myUuid.String()]
	for _, master := range mt.aliveNodes {
		if master.uuid <= leader.uuid {
			leader = master
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
			if new_master {
				mt.resetAnarchyTimer()
			}
		case corpse := <-mt.deadMasterCh:
			mt.killDeadMaster(corpse)
			mt.resetAnarchyTimer()
		case <-mt.anarchyTmr.C:
			mt.chooseLeader()
		}
	}
}

// TODO: Remove the function down here after refactor is completed
// (They are all master responsabilities, not tracker)

// getMasters returns a slice containing the UUID of all the
// alive master nodes.
func (mt *tracker) getMasters() []string {
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
	return mt.state.String()
}

// getLeaderId retrieves the leader UUID (or a "no leader" message
// if no leader has been chosen yet).
func (mt *tracker) getLeaderId() string {
	return *mt.leaderUuid
}

// imLeader simply returns true if this master is the leader.
func (mt *tracker) imLeader() bool {
	return (*mt.state == LEADER)
}