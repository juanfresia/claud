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
)

// --------------------------- Ringo struct ---------------------------

type ringo struct {
	uuid       uuid.UUID
	fromMaster <-chan string
	toMaster   chan<- string
	aliveNodes map[string]string
}

func newRingo(uuid uuid.UUID, fromMaster <-chan string, toMaster chan<- string) *ringo {
	r := &ringo{uuid, fromMaster, toMaster, nil}
	r.aliveNodes = make(map[string]string)
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
		r.aliveNodes[uuidReceived] = src.String()
	}
}

// TODO: Refactor this code below with nice custom messages
func (r *ringo) handleMasterMessage() {
	msg := <-r.fromMaster
	if msg == "MASTERS" {
		ans := "ALIVE MASTER NODES\n"
		for uuid, ipPort := range r.aliveNodes {
			ans += ("UUID: " + uuid + " IP:PORT: " + ipPort + "\n")
		}
		r.toMaster <- ans
	}
}

// --------------------------- Main function ---------------------------

func StartRing(uuid uuid.UUID, fromMaster <-chan string, toMaster chan<- string) {
	r := newRingo(uuid, fromMaster, toMaster)
	go r.listenMulticastUDP(multicastAddr)
	// TODO: Generate ring after some time of listening on multicast
	for {
		r.handleMasterMessage()
	}
}
