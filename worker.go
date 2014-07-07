package main

import (
	"log"
	"net"
	"strconv"
	"time"
)

type Node struct {
	NetAddr NetAddr
	Conn    net.Conn

	Version   *MsgVersion
	Addresses []NetAddr
}

// Periodically get addresses of Nodes which need to be updated
// Closes addresses on exit
func getNodes(addresses chan<- ip_port, end chan<- bool) {
	defer func() {
		end <- true
	}()

	// Add a bootstrap address if necessary
	if !haveKnownNodes() {
		// A bootstrap address MUST be provided on first launch
		if flagBootstrap == "" {
			log.Fatal("No known nodes in DB and no bootstrap address provided.")
		}

		ip, port, err := net.SplitHostPort(flagBootstrap)
		if err != nil {
			log.Fatal("Could not parse address to bootstrap from: ", err)
		}

		if ip == "" {
			log.Fatal("Bootstrap IP must be specified")
		}

		log.Print("Bootstrapping from ", flagBootstrap)
		addresses <- ip_port{ip, port}

		// Give connection to bootstraped address time to succeed before
		// attempting to get more addresses
		time.Sleep(time.Minute)
	}

	// Attempt to get new addresses endlessly.

	for {
		log.Print(len(addresses), " addresses in queue")

		// Only get new addresses if we consumed at least half of the addresses fetched
		// during the last iteration
		if len(addresses) < ADDRESSES_NUM/2 {
			fetched_addresses, max_addresses := addressesToUpdate()

			log.Print("Adding ", len(fetched_addresses), "/", max_addresses, " addresses")

			for _, addr := range fetched_addresses {
				addresses <- addr
			}
		}

		time.Sleep(ADDRESSES_INTERVAL)
	}

}

// Attempt to connect to the addresses provided by `addresses` and sends the
// resulting Node to `nodes`
// The number of addresses which are checked simultaneously is defined by
// NUM_CONNECTION_GOROUTINES.
// Closes nodes on exit
func connectNodes(addresses <-chan ip_port, nodes chan<- Node, end chan<- bool) {
	// Declare here for defered check
	rate_limiter := make(chan bool, NUM_CONNECTION_GOROUTINES)
	defer func() {
		// Wait for goroutines to finish
		for i := 0; i < NUM_CONNECTION_GOROUTINES; i++ {
			<-rate_limiter
		}

		close(nodes)
		end <- true
	}()

	// Attempt to get a connection to each node
	for i := 0; i < NUM_CONNECTION_GOROUTINES; i++ {
		rate_limiter <- true
	}
	for ipp := range addresses {
		<-rate_limiter
		go getSingleNode(ipp, nodes, rate_limiter)
	}
}

func getSingleNode(ipp ip_port, nodes chan<- Node, end chan<- bool) {
	defer func() {
		end <- true
	}()

	hostport := net.JoinHostPort(ipp.ip, ipp.port)
	conn, err := net.DialTimeout("tcp", hostport, NODE_CONNECT_TIMEOUT*time.Second)
	if err != nil {
		conn = nil
	}

	portval, err := strconv.Atoi(ipp.port)
	if err != nil {
		if verbose {
			log.Print("Port conversion error ", ipp.port)
		}
	}

	node := Node{
		NetAddr: NetAddr{
			IP:   net.ParseIP(ipp.ip),
			Port: uint16(portval),
		},
		Conn: conn,
	}

	nodes <- node
}

func updateNodes(nodes <-chan Node, end chan<- bool) {
	defer func() {
		end <- true
	}()

	db := acquireDBConn()
	defer releaseDBConn(db)

	for node := range nodes {
		var upd Node
		if node.Conn != nil {
			// if verbose {
			// 	log.Print("Refreshing ", node.NetAddr.IP.String(), " ", node.NetAddr.Port)
			// }
			upd = refreshNode(node)
		} else {
			upd = node
		}
		upd.Save(db)
	}
}

// Connect to the node and retrieve updated information
func refreshNode(node Node) (updated Node) {
	updated.NetAddr = node.NetAddr
	updated.Conn = node.Conn

	ip := node.NetAddr.IP.String()
	port := node.NetAddr.Port

	err := sendVersion(node)
	if err != nil {
		if err.Error() == "connection refused" || err.Error() == "connection timed out" {
			// Firewall blocking port
			updated.Conn = nil
			return
		}
		if verbose {
			log.Printf("Sending version (%s %d): %v", ip, port, err)
		}
		return
	}

	version, err := receiveVersion(node)
	if err != nil {
		if verbose {
			log.Printf("Receiving version (%s %d): %v", ip, port, err)
		}
		return
	}

	updated.Version = &version

	msg, err := receiveMessage(node)
	if err != nil || msg.Type != "verack" {
		if verbose {
			log.Printf("Receiving verack (%s %d): %v", ip, port, err)
		}
		return // Expected verack to finish handshake
	}

	err = sendGetAddr(node)
	if err != nil {
		if verbose {
			log.Printf("Sending getaddr (%s %d): %v", ip, port, err)
		}
		return
	}
	num_getaddr := 1

	addresses := make([]NetAddr, 0)

	for num_getaddr < 4 {
		msg, err = receiveMessage(node)

		if err != nil {
			// TODO: Connection error ? Retry ?
			// manage timeout for new getaddr
			if verbose {
				log.Printf("Receiving message (%s %d): %v", ip, port, err)
			}

			return
		}

		switch msg.Type {
		case "addr":
			new_addresses, err := parseAddr(msg)
			if err != nil {
				return
			}
			addresses = append(addresses, new_addresses...)

			// Consider that all messages have been received for this getaddr
			// Get the result of getaddr 10 times
			if len(new_addresses) < 1000 {
				num_getaddr += 1

				err = sendGetAddr(node)
				if err != nil {
					// TODO: partial address retrieval, retry ?
					return
				}
			}
		default:
			if verbose {
				log.Printf("Received %s from %v", msg.Type, node.Conn.RemoteAddr())
			}
		}
	}

	updated.Addresses = addresses

	return
}
