package master

import (
	"fmt"
	"github.com/juanfresia/claud/master/ringo"
)

func LaunchMaster() {
	toRingo := make(chan string, 10)
	fromRingo := make(chan string, 10)

	go ringo.StartRing(toRingo, fromRingo)

	for {
		// TODO: Expose http endpoints for user here
		receiveFromRingo(fromRingo)
	}
}

func receiveFromRingo(fromRingo <-chan string) {
	fmt.Println("------------------------------------------------")
	msg := <-fromRingo
	fmt.Println("Received message: " + msg)
}
