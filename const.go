package main

import (
	"time"
)

// Magic numbers specific to each network
var (
	NETWORK_MAIN     []byte = []byte{0xF9, 0xBE, 0xB4, 0xD9}
	NETWORK_TESTNET  []byte = []byte{0xFA, 0xBF, 0xB5, 0xDA}
	NETWORK_TESTNET3 []byte = []byte{0x0B, 0x11, 0x09, 0x07}
	NETWORK_NAMECOIN []byte = []byte{0xF9, 0xBE, 0xB4, 0xFE}

	NETWORK_CURRENT = NETWORK_MAIN // The network in use
)

// Maximum size payload that a message can have
const MAX_PAYLOAD = 1024 * 100

const VERSION_TIME_IN_NETADDR = 31402
const VERSION_BIP_0037 = 70001

const SIZE_NETADDR = 26
const SIZE_NETADDR_WITH_TIME = 30

// Length must be less then 0xfd
const CURRENT_PROTOCOL = 70001
const USER_AGENT = "/BTCCRAWLER/0.4/"

// Number of goroutines
const NUM_CONNECTION_GOROUTINES = 20
const NUM_UPDATE_GOROUTINES = 4

// Timeout
const NODE_CONNECT_TIMEOUT = 10

// Size of channel of nodes which are live but haven't been refreshed yet
const NODE_BUFFER_SIZE = 20

// Concurrent connections to DB
const NUM_DB_CONN = 10

const ADDRESSES_NUM = 5000                 // Number of addresses to fetch
const ADDRESSES_INTERVAL = 5 * time.Minute // Interval to check for new addresses to update

// Minimum update interval for nodes (hours)
const NODE_REFRESH_INTERVAL = 24
