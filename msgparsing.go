package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"
)

// A BTC version message
type MsgVersion struct {
	Protocol  uint32      // Protocol version used by node
	Services  ServiceFlag // Services provided by node
	Timestamp time.Time   // UNIX timestamp of node's time
	AddrLocal NetAddr     // Address of the local node

	AddrRemote  NetAddr // Address of the remote node
	Nonce       uint64  // Random, used for detecting connections to self
	UserAgent   string  //
	StartHeight int32   // Last known block

	Relay bool // Whether the remote peer should relay transactions
}

// Service flags
type ServiceFlag uint64

const (
	NODE_NETWORK ServiceFlag = 1
)

// A BTC net_addr
type NetAddr struct {
	Timestamp time.Time
	Services  uint64
	IP        net.IP
	Port      uint16
}

// Parse a version message
// The payload has the following format :
//   protocol        0.. 3    uint32
//   services        4..11    uint64
//   timestamp      12..19    int64
//   addr_recv      20..45    NetAddr \ message is assumed to be INBOUND,
//   addr_send      46..71    NetAddr / no timestamp
//   nonce          72..79    uint64
//   user_agent     80..??    varstr
//   start_height ??+1..??+4  int32
//   relay        ??+5..??+5  bool (version > VERSION_BIP_0037)
func parseVersion(msg Message) (ver MsgVersion, err error) {
	// Check that the payload is at least big enough to contain the first byte
	// of the user_agent varstr and all the attributes leading up to it
	if len(msg.Payload) < 81 {
		err = fmt.Errorf("parseVersion: Payload too small (%d)", len(msg.Payload))
		return
	}

	ver.Protocol = binary.LittleEndian.Uint32(msg.Payload[:4])
	ver.Services = ServiceFlag(binary.LittleEndian.Uint64(msg.Payload[4:12]))
	ver.Timestamp = time.Unix(int64(binary.LittleEndian.Uint64(msg.Payload[12:20])), 0)

	ver.AddrLocal, err = parseNetAddr(msg.Payload[20:46], false)
	if err != nil {
		return
	}
	ver.AddrRemote, err = parseNetAddr(msg.Payload[46:72], false)
	if err != nil {
		return
	}

	ver.Nonce = binary.LittleEndian.Uint64(msg.Payload[72:80])

	var n int
	ver.UserAgent, n, err = varStr(msg.Payload[80:])
	if err != nil {
		return
	}

	var data = msg.Payload[(80 + n):] // Slice of data after varstr

	// Check if the remaining buffer is large enough
	if len(data) < 4 {
		err = fmt.Errorf("parseVersion: Payload too small (%d) for start_height", len(msg.Payload))
		return
	}

	ver.StartHeight = int32(binary.LittleEndian.Uint32(data[:4]))

	if ver.Protocol >= VERSION_BIP_0037 {
		if len(data) == 5 {
			if data[4] != byte(0) {
				ver.Relay = true
			}
		} else {
			if verbose {
				log.Printf("Node should support relay but does not (ver %d / ua %s)",
					ver.Protocol, ver.UserAgent)
			}
		}
	}

	return
}

// Create a version message to initiate a connection to given node
//   protocol        0.. 3    uint32
//   services        4..11    uint64
//   timestamp      12..19    int64
//   addr_recv      20..45    NetAddr \ message is assumed to be OUTBOUND,
//   addr_send      46..71    NetAddr / no timestamp
//   nonce          72..79    uint64
//   user_agent     80..??    varstr
//   start_height ??+1..??+4  int32
//   relay        ??+5..??+5  bool (version > VERSION_BIP_0037)
func makeVersion(node Node) (msg Message) {
	if len(USER_AGENT) >= 0xfd {
		log.Fatal("Cannot create version message: USER_AGENT too long")
	}
	msg.Type = "version"
	msg.Payload = make([]byte, 85+1+len(USER_AGENT))

	binary.LittleEndian.PutUint32(msg.Payload[0:4], uint32(CURRENT_PROTOCOL))    // Protocol
	binary.LittleEndian.PutUint64(msg.Payload[4:12], 0)                          // Services
	binary.LittleEndian.PutUint64(msg.Payload[12:20], uint64(time.Now().Unix())) // timestamp

	// addr_recv
	addr_recv := msg.Payload[20:46]
	tcpRemote, ok := node.Conn.RemoteAddr().(*net.TCPAddr)
	if !ok {
		log.Fatal("Not a TCP connection")
	}
	binary.LittleEndian.PutUint64(addr_recv[0:8], 1)                     // services
	copy(addr_recv[8:24], []byte(tcpRemote.IP))                          // ip
	binary.BigEndian.PutUint16(addr_recv[24:26], uint16(tcpRemote.Port)) //port

	//addr_send
	addr_send := msg.Payload[46:72]
	tcpLocal, ok := node.Conn.LocalAddr().(*net.TCPAddr)
	if !ok {
		log.Fatal("Not a TCP connection")
	}
	binary.LittleEndian.PutUint64(addr_send[0:8], 0) // services
	copy(addr_send[8:24], []byte(tcpLocal.IP))       // ip
	binary.BigEndian.PutUint16(addr_send[24:26], 0)  //port

	// nonce
	// Secure randomness not needed
	rand.Seed(time.Now().UTC().UnixNano())
	nonce := uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
	binary.LittleEndian.PutUint64(msg.Payload[72:80], nonce)

	// Useragent saves size as an one byte since its length is <0xfd
	msg.Payload[80] = byte(len(USER_AGENT))
	copy(msg.Payload[81:], USER_AGENT)

	// StartHeight and relay are both set to 0
	return
}

// Parse an addr message. The format is a var_int with the number of addresses
// followed by the list net_addr.
// Assumes protocol version > VERSION_TIME_IN_NETADDR
func parseAddr(msg Message) (addresses []NetAddr, err error) {
	length, n, err := varInt(msg.Payload)
	if err != nil {
		return
	}

	if len(msg.Payload) < n+int(length)*SIZE_NETADDR_WITH_TIME {
		err = fmt.Errorf("parseAddr: Payload too small (%d)", len(msg.Payload))
		return
	}

	var num_addr = int(length)

	addresses = make([]NetAddr, num_addr)

	for i := 0; i < num_addr; i++ {
		start := n + i*SIZE_NETADDR_WITH_TIME
		end := n + (i+1)*SIZE_NETADDR_WITH_TIME
		addresses[i], err = parseNetAddr(msg.Payload[start:end], true)

		if err != nil {
			return
		}
	}

	return
}

// Parse a network address from the given slice. The slice is assumed to
// contain only the net_addr and fails otherwise
// Time indicates whether the NA contains the time or not
// Layout of a network address is :
//   time      0.. 3          uint32   current time
//   services  4..11   0.. 7  uint64   services provided by node
//   ip       12..27   8..23  [16]byte node ip
//   port     28..29  24..25  uint16   port (big endian)
func parseNetAddr(data []byte, time_field bool) (na NetAddr, err error) {
	if (time_field && len(data) != SIZE_NETADDR_WITH_TIME) ||
		(!time_field && len(data) != SIZE_NETADDR) {
		err = fmt.Errorf("parseNetAddr: Unexpected data size %d", len(data))
		return
	}

	if time_field {
		na.Timestamp = time.Unix(int64(binary.LittleEndian.Uint32(data[:4])), 0)
		data = data[4:] // Normalize slice
	}

	na.Services = binary.LittleEndian.Uint64(data[:8])
	na.IP = net.IP(data[8:24])
	na.Port = binary.BigEndian.Uint16(data[24:26])

	return
}

func (na NetAddr) String() string {
	return fmt.Sprintf("<NetAddr: <%v>:%v  %v  %v>", na.IP, na.Port, na.Services, na.Timestamp)
}
