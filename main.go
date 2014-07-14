package main

import (
	"flag"
	"log"
	"net"
	"os"
	"runtime/pprof"
	"sync"
)

var flagBootstrap string // Bootstrap from the given host
var flagConnect string   // Connect only to the given address

var cpuprofile string  // Profile CPU
var heapprofile string // Profile Memory
var memusage string    // Memory usage over time
var verbose bool       // Verbose logging

var fcpu, fheap, fmem *os.File

func init() {
	flag.StringVar(&flagBootstrap, "bootstrap", "", "Node to bootstrap from if none are known")
	flag.StringVar(&flagConnect, "connect", "", "Connect only to the given node")

	flag.StringVar(&cpuprofile, "cpuprofile", "", "Write CPU profile to file")
	flag.StringVar(&heapprofile, "heapprofile", "", "Write heap profile to file")

	flag.StringVar(&memusage, "memusage", "", "Write memory usage to file on every node refresh")

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
		defer fcpu.Close()

		pprof.StartCPUProfile(fcpu)
		defer pprof.StopCPUProfile()
	}

	if heapprofile != "" {
		fheap, err = os.Create(heapprofile)
		if err != nil {
			log.Fatal(err)
		}
		go UpdateHeapProfile()

		defer fheap.Close()
		defer pprof.WriteHeapProfile(fheap)
	}

	if memusage != "" {
		fmem, err = os.Create(memusage)
		if err != nil {
			log.Fatal(err)
		}
		defer fmem.Close()
	}

	err = initDB()
	if err != nil {
		log.Fatal(err)
	}

	addresses := make(chan ip_port, 2*ADDRESSES_NUM)
	nodes := make(chan Node, NODE_BUFFER_SIZE)
	save := make(chan Node, NODE_BUFFER_SIZE)
	wg := &sync.WaitGroup{}

	if flagConnect != "" {
		ip, port, err := net.SplitHostPort(flagConnect)
		if err != nil {
			log.Fatal("Could not parse address to connect to: ", err)
		}

		if ip == "" {
			log.Fatal("IP must be specified")
		}

		log.Print("Connecting to ", flagConnect)
		addresses <- ip_port{ip, port}

		close(addresses)
	} else {
		wg.Add(1)
		go getNodes(addresses, wg)
	}
	wg.Add(3)
	go connectNodes(addresses, nodes, wg)
	go updateNodes(nodes, save, wg)
	go saveNodes(save, wg)

	go stats(60, true)

	// Wait for all three main goroutines to end
	wg.Wait()

	cleanDB()
}
