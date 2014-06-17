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

func getNodes(live_nodes chan<- Node, end chan<- bool) {
	defer func() {
		close(live_nodes)
		end <- true
	}()

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

	rate_limiter := make(chan bool, NUM_THREADS_GET)
	for ip, portstr := range addresses {
		rate_limiter <- true

		ip := ip
		portstr := portstr
		go func() {
			defer func() {
				<-rate_limiter
			}()

			hostport := net.JoinHostPort(ip, string(portstr))
			//log.Print("Checking ", hostport)

			conn, err := net.DialTimeout("tcp", hostport, time.Second)
			if err != nil {
				return
			}

			port, err := strconv.Atoi(portstr)
			if err != nil {
				log.Print("Port conversion error ", portstr)
			}
			node := Node{
				NetAddr: NetAddr{
					IP:   net.ParseIP(ip),
					Port: uint16(port),
				},
				Conn: conn,
			}

			live_nodes <- node
		}()
	}

	log.Print("All addresses checked")
}

func updateNodes(live_nodes <-chan Node, end chan<- bool) {
	defer func() {
		end <- true
	}()

	for node := range live_nodes {
		upd := refreshNode(node)
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
