package ringo

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

// --------------------------- Ringo struct ---------------------------

type masterData struct {
	uuid  string
	addr  *net.UDPAddr
	timer *time.Timer
}

type ringo struct {
	uuid       uuid.UUID
	fromMaster <-chan string
	toMaster   chan<- string
	aliveNodes map[string]masterData
}

func newRingo(uuid uuid.UUID, fromMaster <-chan string, toMaster chan<- string) *ringo {
	r := &ringo{uuid, fromMaster, toMaster, nil}
	r.aliveNodes = make(map[string]masterData)
	go r.keepAliveSender(multicastAddr)
	return r
}

func (r *ringo) keepAliveSender(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		fmt.Println("Error:", err)
	}
	c, err := net.DialUDP("udp", nil, addr)
	for {
		msg := r.uuid.String()
		c.Write([]byte(msg))
		time.Sleep(keepAliveTmr)
	}
}

func (r *ringo) trackMasterNode(masterUuid string, addr *net.UDPAddr) {
	_, present := r.aliveNodes[masterUuid]
	if present {
		r.aliveNodes[masterUuid].timer.Stop()
	}

	killMasterNode := func() {
		fmt.Println("Warning: Master " + masterUuid + " has died!")
		delete(r.aliveNodes, masterUuid)
	}

	timer := time.AfterFunc(defunctTmr, killMasterNode)
	r.aliveNodes[masterUuid] = masterData{masterUuid, addr, timer}
}

func (r *ringo) listenMulticastUDP(multicastAddr string) {
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
		r.trackMasterNode(uuidReceived, src)
	}
}

// TODO: Refactor this code below with nice custom messages
func (r *ringo) handleMasterMessage() {
	msg := <-r.fromMaster
	if msg == "MASTERS" {
		ans := "ALIVE MASTER NODES\n"
		for uuid, md := range r.aliveNodes {
			ipPort := md.addr.String()
			ans += ("UUID: " + uuid + " IP:PORT: " + ipPort + "\n")
		}
		r.toMaster <- ans
	}
}

// --------------------------- Main function ---------------------------

func StartRing(uuid uuid.UUID, fromMaster <-chan string, toMaster chan<- string) {
	r := newRingo(uuid, fromMaster, toMaster)
	go r.listenMulticastUDP(multicastAddr)
	time.Sleep(learningTmr)
	// TODO: Generate ring
	for {
		r.handleMasterMessage()
	}
}
