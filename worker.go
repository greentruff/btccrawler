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

// ...
// Closes nodes on exit
func getNodes(nodes chan<- Node, end chan<- bool) {
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

	// Retrieve known addresses from DB
	addresses := addressesToUpdate()

	// Add a bootstrap address if necessary
	if len(addresses) == 0 && flagBootstrap != "" {
		ip, port, err := net.SplitHostPort(flagBootstrap)
		if err != nil {
			log.Fatal("Could not parse address to bootstrap from: ", err)
		}

		if ip == "" {
			log.Fatal("Bootstrap IP must be specified")
		}

		log.Print("Bootstrapping from ", flagBootstrap)
		addresses = append(addresses, ip_port{ip, port})
	}

	// Attempt to get a connection to each node
	for i := 0; i < NUM_CONNECTION_GOROUTINES; i++ {
		rate_limiter <- true
	}
	for _, ipp := range addresses {
		<-rate_limiter
		go getSingleNode(ipp, nodes, rate_limiter)
	}

	log.Print("All addresses queued")
}

func getSingleNode(ipp ip_port, nodes chan<- Node, end chan<- bool) {
	defer func() {
		end <- true
	}()

	hostport := net.JoinHostPort(ipp.ip, ipp.port)
	conn, err := net.DialTimeout("tcp", hostport,
		time.Duration(NODE_CONNECT_TIMEOUT)*time.Second)
	if err != nil {
		conn = nil
	}

	portval, err := strconv.Atoi(ipp.port)
	if err != nil {
		log.Print("Port conversion error ", ipp.port)
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

	for node := range nodes {
		var upd Node
		if node.Conn != nil {
			upd = refreshNode(node)
		} else {
			upd = node
		}
		upd.Save()
	}
}

// Connect to the node and retrieve updated information
func refreshNode(node Node) (updated Node) {
	updated.NetAddr = node.NetAddr

	err := sendVersion(node)
	if err != nil {
		return
	}

	version, err := receiveVersion(node)
	if err != nil {

		return
	}

	updated.Version = &version

	msg, err := receiveMessage(node)
	if err != nil || msg.Type != "verack" {
		return // Expected verack to finish handshake
	}

	err = sendGetAddr(node)
	if err != nil {
		return
	}
	num_getaddr := 1

	addresses := make([]NetAddr, 0)

	for num_getaddr < 4 {
		msg, err = receiveMessage(node)

		if err != nil {
			// TODO: Connection error ? Retry ?
			// manage timeout for new getaddr
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
			log.Printf("Received %s from %v", msg.Type, node.Conn.RemoteAddr())
		}
	}

	updated.Addresses = addresses

	return
}
