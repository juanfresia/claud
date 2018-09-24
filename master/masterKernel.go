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
	learningTmr     = 12 * time.Second
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
}

func newKernel() MasterKernel {
	k := &MasterKernel{uuid: uuid.NewV4()}
	k.aliveNodes = make(map[string]MasterData)
	go k.keepAliveSender(multicastAddr)
	go k.listenMulticastUDP(multicastAddr)
	return *k
}

func (k *MasterKernel) keepAliveSender(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		fmt.Println("Error:", err)
	}
	c, err := net.DialUDP("udp", nil, addr)
	msg := k.uuid.String()
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
	for {
		msg := make([]byte, maxDatagramSize)
		n, src, err := l.ReadFromUDP(msg)
		if err != nil {
			fmt.Println("ReadFromUDP failed:", err)
		}
		uuidReceived := string(msg[:n])
		k.trackMasterNode(uuidReceived, src)
	}
}

func (k *MasterKernel) trackMasterNode(masterUuid string, addr *net.UDPAddr) {
	_, present := k.aliveNodes[masterUuid]
	if present {
		k.aliveNodes[masterUuid].timer.Stop()
	}

	killMasterNode := func() {
		fmt.Println("Warning: Master " + masterUuid + " has died!")
		delete(k.aliveNodes, masterUuid)
	}

	timer := time.AfterFunc(defunctTmr, killMasterNode)
	k.aliveNodes[masterUuid] = MasterData{masterUuid, addr, timer}
}

func (k *MasterKernel) GetMasters() string {
	ans := "ALIVE MASTER NODES\n"
	for uuid, md := range k.aliveNodes {
		ipPort := md.addr.String()
		ans += ("UUID: " + uuid + " IP:PORT: " + ipPort + "\n")
	}
	return ans
}
