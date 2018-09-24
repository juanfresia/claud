package master

import (
	"fmt"
	"net"
	"time"

	"github.com/satori/go.uuid"
)

const (
	multicastAddr   = "224.0.0.28:1504"
	maxDatagramSize = 8192
	keepAliveTmr    = 5 * time.Second
	defunctTmr      = 10 * time.Second
	learningTmr     = 20 * time.Second
)

type MasterState int

const (
	LEADER MasterState = iota
	NOT_LEADER
	ANARCHY
)

type MasterData struct {
	uuid  string
	addr  *net.UDPAddr
	timer *time.Timer
}

type MasterKernel struct {
	uuid       uuid.UUID
	count      int
	aliveNodes map[string]MasterData
	state      MasterState

	keepAliveCh  chan MasterData
	deadMasterCh chan string
	anarchyTmr   *time.Timer
}

func newKernel() MasterKernel {
	k := &MasterKernel{uuid: uuid.NewV4(), state: ANARCHY}
	k.aliveNodes = make(map[string]MasterData)

	// This ones should be buffered to prevent for message drops
	// TODO: define buffer length as master count
	k.keepAliveCh = make(chan MasterData, 10)
	k.deadMasterCh = make(chan string, 10)

	k.anarchyTmr = time.NewTimer(learningTmr)

	go k.keepAliveSender(multicastAddr)
	go k.listenMulticastUDP(multicastAddr)
	go k.eventLoop()
	return *k
}

func (k *MasterKernel) keepAliveSender(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		fmt.Println("Error:", err)
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
		fmt.Println("Error:", err)
	}
	l, err := net.ListenMulticastUDP("udp", nil, addr)
	l.SetReadBuffer(maxDatagramSize)

	// Keep listening for updates
	msg := make([]byte, maxDatagramSize)
	for {
		n, src, err := l.ReadFromUDP(msg)
		if err != nil {
			fmt.Println("ReadFromUDP failed:", err)
		}
		uuidReceived := string(msg[:n])
		data := MasterData{uuid: uuidReceived, addr: src}
		// Block until event is catched
		k.keepAliveCh <- data
	}
}

func (k *MasterKernel) trackMasterNode(masterUuid string, addr *net.UDPAddr) bool {
	new_master := false
	_, present := k.aliveNodes[masterUuid]
	if present {
		k.aliveNodes[masterUuid].timer.Stop()
	} else {
		fmt.Println("A new master! Let's meet them")
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
	fmt.Println("Warning: Master " + masterUuid + " has died!")
	delete(k.aliveNodes, masterUuid)
}

// This function should only be called in the kernel main thread
// to prevent race conditions
func (k *MasterKernel) resetAnarchyTimer() {
	fmt.Println("Something went horribly wrong, embrace ANARCHY!")
	if !k.anarchyTmr.Stop() {
		<-k.anarchyTmr.C
	}
	k.anarchyTmr.Reset(learningTmr)
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
			fmt.Println("Hey, anarchy has ended. Time to choose a leader")
		}
	}
}

func (k *MasterKernel) GetMasters() string {
	ans := "ALIVE MASTER NODES\n"
	for uuid, md := range k.aliveNodes {
		ipPort := md.addr.String()
		ans += ("UUID: " + uuid + " IP:PORT: " + ipPort + "\n")
	}
	return ans
}
