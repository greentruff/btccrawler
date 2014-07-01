package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"
)

var flagBootstrap string // Bootstrap from the given host

var cpuprofile string  // Profile CPU
var heapprofile string // Profile Memory

var fcpu, fheap *os.File

func init() {
	flag.StringVar(&flagBootstrap, "bootstrap", "", "Node to bootstrap from if none are known")

	flag.StringVar(&cpuprofile, "cpuprofile", "", "Write CPU profile to file")
	flag.StringVar(&heapprofile, "heapprofile", "", "Write heap profile to file")

	flag.Parse()
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Print("Starting up")

	var err error
	if cpuprofile != "" {
		fcpu, err = os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(fcpu)
		defer pprof.StopCPUProfile()
	}

	if heapprofile != "" {
		fheap, err = os.Create(heapprofile)
		if err != nil {
			log.Fatal(err)
		}

		defer pprof.WriteHeapProfile(fheap)
	}

	err = initDB()
	if err != nil {
		log.Fatal(err)
	}

	live_nodes := make(chan Node, LIVE_NODE_BUFFER_SIZE)
	end := make(chan bool, 2)

	go getNodes(live_nodes, end) // Get nodes which respond
	go updateNodes(live_nodes, end)

	<-end
	<-end

	cleanDB()
}
