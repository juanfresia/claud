package master

import (
	"fmt"
	"net/http"

	"github.com/juanfresia/claud/master/ringo"
	"github.com/satori/go.uuid"
)

// --------------------------- Master struct ---------------------------

type master struct {
	uuid      uuid.UUID
	count     int
	toRingo   chan string
	fromRingo chan string
}

func newMaster() *master {
	m := &master{uuid: uuid.NewV4(), count: 0}

	m.toRingo = make(chan string, 10)
	m.fromRingo = make(chan string, 10)

	go ringo.StartRing(m.uuid, m.toRingo, m.fromRingo)
	//go m.receiveFromRingo()
	fmt.Println("New master launched with UUID:", m.uuid.String())
	return m
}

func (m *master) statusHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, my uuid is %v\n", m.uuid)
	fmt.Fprintf(w, "I've received %v messages\n", m.count)
}

// TODO: Refactor this code below with nice custom messages
func (m *master) aliveMasters(w http.ResponseWriter, r *http.Request) {
	m.toRingo <- "MASTERS"
	msg := <-m.fromRingo
	fmt.Fprintf(w, msg)
}

func (m *master) receiveFromRingo() {
	for {
		fmt.Println("------------------------------------------------")
		msg := <-m.fromRingo
		fmt.Println("Received message: " + msg)
		m.count++
	}
}

// --------------------------- Main function ---------------------------

func LaunchMaster(masterIp, port string) {
	m := newMaster()

	http.HandleFunc("/", m.statusHandler)
	http.HandleFunc("/masters", m.aliveMasters)
	http.ListenAndServe(masterIp+":"+port, nil)
}
