package ringo

import (
	"fmt"
	"net"
	"time"
)

const (
	multicastAddr   = "224.0.0.28:1504"
	maxDatagramSize = 8192
	keepAliveTmr    = 5 * time.Second
)

func StartRing(fromMaster <-chan string, toMaster chan<- string) {
	go keepAliveSender(multicastAddr)
	serverMulticastUDP(multicastAddr, toMaster)
	// TODO: Generate ring after some time of listening on multicast
}

func keepAliveSender(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		fmt.Println("Error:", err)
	}
	c, err := net.DialUDP("udp", nil, addr)
	for {
		msg := "KeepAlive " + c.LocalAddr().String()
		c.Write([]byte(msg))
		time.Sleep(keepAliveTmr)
	}
}

func serverMulticastUDP(multicastAddr string, toMaster chan<- string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		fmt.Println("Error:", err)
	}
	l, err := net.ListenMulticastUDP("udp", nil, addr)
	l.SetReadBuffer(maxDatagramSize)
	for {
		msg := make([]byte, maxDatagramSize)
		n, _, err := l.ReadFromUDP(msg)
		if err != nil {
			fmt.Println("ReadFromUDP failed:", err)
		}
		toMaster <- string(msg[:n])
	}
}
