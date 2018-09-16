package master

import (
	"fmt"
	"net"
	"time"
)

const (
	srvAddr         = "224.0.0.28:1504"
	maxDatagramSize = 8192
	keepAliveTmr    = 5 * time.Second
)

func LaunchMaster() {
	go ping(srvAddr)
	serverMulticastUDP(srvAddr, msgHandler)
}

func ping(a string) {
	addr, err := net.ResolveUDPAddr("udp", a)
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

func msgHandler(src *net.UDPAddr, n int, b []byte) {
	fmt.Println("------------------------------------------------")
	fmt.Println(n, "bytes read from", src)
	fmt.Println("Message was: " + string(b[:n]))
}

func serverMulticastUDP(a string, handler func(*net.UDPAddr, int, []byte)) {
	addr, err := net.ResolveUDPAddr("udp", a)
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
		handler(src, n, msg)
	}
}
