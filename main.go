package main

import (
	"flag"
	"log"
)

var flagBootstrap string // Bootstrap from the given host

func init() {
	flag.StringVar(&flagBootstrap, "bootstrap", "", "Node to bootstrap from if none are known")
	flag.Parse()
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

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
