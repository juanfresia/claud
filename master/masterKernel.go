package master

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/satori/go.uuid"
	"gopkg.in/natefinch/lumberjack.v2"
)

// ----------------------- Data type definitions ----------------------

const (
	multicastAddr   = "224.0.0.28:1504"
	maxDatagramSize = 8192
	keepAliveTmr    = 5 * time.Second
	defunctTmr      = 10 * time.Second
	learningTmr     = 20 * time.Second
	produceLogs     = true
)

type MasterState int

const (
	LEADER MasterState = iota
	NOT_LEADER
	ANARCHY
)

func (ms MasterState) String() string {
	strMap := [...]string{
		"LEADER",
		"NOT LEADER",
		"IN ANARCHY",
	}

	return strMap[ms]
}

type MasterData struct {
	uuid  string
	addr  *net.UDPAddr
	timer *time.Timer
}

type MasterKernel struct {
	uuid       uuid.UUID
	aliveNodes map[string]MasterData
	state      *MasterState
	leaderUuid *string

	keepAliveCh  chan MasterData
	deadMasterCh chan string
	anarchyTmr   *time.Timer

	kernelLog *log.Logger
	sch       Scheduler
}

// --------------------------- Functions --------------------------

func newMasterKernel() MasterKernel {
	k := &MasterKernel{uuid: uuid.NewV4()}
	k.state = new(MasterState)
	k.leaderUuid = new(string)
	*k.state = ANARCHY
	*k.leaderUuid = "NO LEADER"
	k.aliveNodes = make(map[string]MasterData)

	// This ones should be buffered to prevent for message drops
	// TODO: define buffer length as master count
	k.keepAliveCh = make(chan MasterData, 10)
	k.deadMasterCh = make(chan string, 10)

	k.startupKernelLog("./kernel-" + k.uuid.String()[:8] + ".log")
	k.sch = newScheduler(k.uuid, k.kernelLog)

	k.anarchyTmr = time.NewTimer(learningTmr)

	go k.keepAliveSender(multicastAddr)
	go k.listenMulticastUDP(multicastAddr)
	go k.eventLoop()
	fmt.Println("Master Kernel is up!")
	return *k
}

func (k *MasterKernel) startupKernelLog(logFile string) {
	if produceLogs {
		l, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

		if err != nil {
			fmt.Printf("Error opening logfile: %v", err)
			os.Exit(1)
		}

		k.kernelLog = log.New(l, "", log.Ldate|log.Ltime)
		k.kernelLog.SetOutput(&lumberjack.Logger{
			Filename:   logFile,
			MaxSize:    1,  // MB after which new logfile is created
			MaxBackups: 3,  // old logfiles kept at the same time
			MaxAge:     10, // days until automagically delete logfiles
		})
	} else {
		k.kernelLog = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	}
}

func (k *MasterKernel) keepAliveSender(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		k.kernelLog.Printf("ERROR: UDP socket creation failed: %v", err)
	}
	c, err := net.DialUDP("udp", nil, addr)
	msg := k.uuid.String()
	// TODO: change this for a ticker (to be able to gracefully quit)
	for {
		c.Write([]byte(msg))
		time.Sleep(keepAliveTmr)
	}
}

func (k *MasterKernel) listenMulticastUDP(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		k.kernelLog.Printf("ERROR: UDP socket creation failed: %v", err)
	}
	l, err := net.ListenMulticastUDP("udp", nil, addr)
	l.SetReadBuffer(maxDatagramSize)

	// Keep listening for updates
	msg := make([]byte, maxDatagramSize)
	for {
		n, src, err := l.ReadFromUDP(msg)
		if err != nil {
			k.kernelLog.Printf("ERROR: ReadFromUDP failed: %v", err)
		}
		uuidReceived := string(msg[:n])
		data := MasterData{uuid: uuidReceived, addr: src}
		// Block until event is caught
		k.keepAliveCh <- data
	}
}

func (k *MasterKernel) trackMasterNode(masterUuid string, addr *net.UDPAddr) bool {
	new_master := false
	_, present := k.aliveNodes[masterUuid]
	if present {
		k.aliveNodes[masterUuid].timer.Stop()
	} else {
		k.kernelLog.Printf("A new master has appeared! Dropping current leader.")
		new_master = true
	}

	killMasterNode := func() {
		k.deadMasterCh <- masterUuid
	}

	timer := time.AfterFunc(defunctTmr, killMasterNode)
	k.aliveNodes[masterUuid] = MasterData{masterUuid, addr, timer}
	return new_master
}

func (k *MasterKernel) killDeadMaster(masterUuid string) {
	k.kernelLog.Printf("WARNING: Master " + masterUuid + " has died!")
	delete(k.aliveNodes, masterUuid)
}

// This function should only be called in the kernel main thread
// to prevent race conditions
func (k *MasterKernel) resetAnarchyTimer() {
	k.kernelLog.Printf("No leader detected, embrace ANARCHY!")

	if *k.state == ANARCHY {
		// Need to stop timer
		if !k.anarchyTmr.Stop() {
			<-k.anarchyTmr.C
		}
	}
	*k.state = ANARCHY
	*k.leaderUuid = "NO LEADER"
	k.anarchyTmr = time.NewTimer(learningTmr)
}

func (k *MasterKernel) restartScheduler(leaderData *MasterData) {
	if leaderData.uuid != k.uuid.String() {
		time.Sleep(learningTmr)
	}
	k.sch.OpenConnections(leaderData)
}

func (k *MasterKernel) chooseLeader() {
	k.kernelLog.Printf("ANARCHY has ended. Choosing a new leader")
	leader := k.aliveNodes[k.uuid.String()]
	for _, master := range k.aliveNodes {
		if master.uuid <= leader.uuid {
			leader = master
		}
	}

	*k.leaderUuid = leader.uuid
	k.kernelLog.Printf("We all hail the new leader: %v\n", leader.uuid)
	if leader.uuid == k.uuid.String() {
		k.kernelLog.Printf("I am the leader")
		*k.state = LEADER
	} else {
		*k.state = NOT_LEADER
	}
	k.kernelLog.Printf("New leader state: %v\n", k.state.String())
	go k.restartScheduler(&leader)
}

func (k *MasterKernel) GetMasters() string {
	ans := "ALIVE MASTER NODES\n"
	for uuid, md := range k.aliveNodes {
		ipPort := md.addr.String()
		ans += ("UUID: " + uuid + " IP:PORT: " + ipPort + "\n")
	}
	return ans
}

func (k *MasterKernel) GetLeaderState() string {
	return k.state.String()
}

func (k *MasterKernel) GetLeaderId() string {
	return *k.leaderUuid
}

func (k *MasterKernel) eventLoop() {
	for {
		select {
		case data := <-k.keepAliveCh:
			if new_master := k.trackMasterNode(data.uuid, data.addr); new_master {
				k.resetAnarchyTimer()
			}
		case corpse := <-k.deadMasterCh:
			k.killDeadMaster(corpse)
			k.resetAnarchyTimer()
		case <-k.anarchyTmr.C:
			k.chooseLeader()
		}
	}
}
