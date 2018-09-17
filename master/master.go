package master

import (
	"fmt"
	"net/http"

	"github.com/juanfresia/claud/master/ringo"
	"github.com/satori/go.uuid"
)

type Master struct {
	uuid  uuid.UUID
	count int
}

func (m *Master) statusHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, my uuid is %v\n", m.uuid)
	fmt.Fprintf(w, "I've received %v messages\n", m.count)
}

func LaunchMaster() {
	master := &Master{uuid: uuid.NewV4(), count: 0}

	toRingo := make(chan string, 10)
	fromRingo := make(chan string, 10)

	go ringo.StartRing(toRingo, fromRingo)
	go receiveFromRingo(fromRingo, master)

	http.HandleFunc("/", master.statusHandler)
	http.ListenAndServe(":8081", nil)
}

func receiveFromRingo(fromRingo <-chan string, master *Master) {
	for {
		fmt.Println("------------------------------------------------")
		msg := <-fromRingo
		fmt.Println("Received message: " + msg)
		master.count++
	}
}
