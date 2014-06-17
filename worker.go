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

func getNodes(nodes chan<- Node, end chan<- bool) {
	defer func() {
		close(live_nodes)
		end <- true
	}()

	// Retrieve known addresses from DB
	conn := connectDB()
	rows, err := conn.Query("SELECT ip, port FROM nodes WHERE refresh=1 AND port!=0")
	if err != nil {
		log.Fatal(err)
	}

	var ip, portstr string
	count := 0
	addresses := make(map[string]string)

	for rows.Next() {
		rows.Scan(&ip, &portstr)
		count++

		addresses[ip] = portstr
	}
	conn.Close()

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
		addresses[ip] = port
	}

	// Attempt to get a connection to each node
	rate_limiter := make(chan bool, NUM_THREADS_GET)
	for i:=0; i<NUM_THREADS_GET; ++i {
		rate_limiter <- true
	}

	for ip, port := range addresses {
		<-rate_limiter

		go getSingleNode(ip, port, nodes, end)
	}

	log.Print("All addresses checked")
}

func getSingleNode(ip, port string, nodes chan<- Node, end chan<- bool) {
	defer func() {
		end <- true
	}()

	hostport := net.JoinHostPort(ip, port)

	conn, err := net.DialTimeout("tcp", hostport, time.Second)
	if err != nil {
		conn = nil
	}

	portval, err := strconv.Atoi(port)
	if err != nil {
		log.Print("Port conversion error ", portstr)
	}
	node := Node{
		NetAddr: NetAddr{
			IP:   net.ParseIP(ip),
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
