package main

import (
	"log"
)

func main() {
	err := initDB()
	if err != nil {
		log.Fatal(err)
	}

	live_nodes := make(chan Node, LIVE_NODE_BUFFER_SIZE)
	end := make(chan bool, 2)

	go getNodes(live_nodes, end) // Get nodes which respond
	go updateNodes(live_nodes, end)

	<-end
	<-end
}
