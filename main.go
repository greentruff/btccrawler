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
var verbose bool // Verbose logging

var fcpu, fheap *os.File

func init() {
	flag.StringVar(&flagBootstrap, "bootstrap", "", "Node to bootstrap from if none are known")

	flag.StringVar(&cpuprofile, "cpuprofile", "", "Write CPU profile to file")
	flag.StringVar(&heapprofile, "heapprofile", "", "Write heap profile to file")

	verboseFlag := flag.Bool("v", false, "Verbose output")

	flag.Parse()

	verbose = *verboseFlag

	logFlags := 0 // No log flags by default
	if verbose {
		logFlags = logFlags | log.Ldate | log.Ltime | log.Lshortfile
	}
	log.SetFlags(logFlags)
}

func main() {
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

	addresses := make(chan ip_port, 2*ADDRESSES_NUM)
	nodes := make(chan Node, NODE_BUFFER_SIZE)
	end := make(chan bool, 2)

	go getNodes(addresses, end)
	go connectNodes(addresses, nodes, end)
	go updateNodes(nodes, end)
	
	// Wait for all three main goroutines to end
	<-end
	<-end
	<-end

	cleanDB()
}
