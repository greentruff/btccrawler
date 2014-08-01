package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Default max number of arguments for an SQLite query
const SQLITE_MAX_VARIABLE_NUMBER = 999

// Special values for ids
const (
	ID_UNKNOWN   = 0  // The state of the node in the DB is unknown
	ID_NOT_IN_DB = -1 // The node was not found in the DB
)

type ip_port struct {
	ip   string
	port string
}

type nodeDB struct {
	node *Node

	tx           *sql.Tx
	now          int64 // Current time for updated_at, next_refresh..
	dbInfo       dbNodeInfo
	dbNeighbours map[string]dbNeighbourInfo // Key is joined IP/Port
}

// Node attributes which are stored in the DB
type dbNodeInfo struct {
	id int64

	ip   string
	port string

	protocol   int
	user_agent string

	next_refresh int64

	online     bool
	online_at  int64
	success    bool
	success_at int64
}

// Node neighbour partial attributes stored in the DB
type dbNeighbourInfo struct {
	id           int64
	next_refresh int64
}

// In schemas, type DATE is used instead of DATETIME so that the sqlite driver
// does not try to convert the underlying int to a time.Time. SQLite considers
// both types as NUMERIC (see http://www.sqlite.org/datatype3.html)
const INIT_SCHEMA_NODES = `
	CREATE TABLE IF NOT EXISTS "nodes" (
		"id"           INTEGER PRIMARY KEY AUTOINCREMENT,

		"ip"           TEXT NOT NULL,
		"port"         INTEGER NOT NULL,
		"protocol"     INTEGER NOT NULL DEFAULT 0,
		"user_agent"   TEXT DEFAULT '',

		"online"       BOOLEAN NOT NULL DEFAULT 0, 
		"success"      BOOLEAN NOT NULL DEFAULT 0,

		"next_refresh" DATE NOT NULL DEFAULT 0,

		"online_at"    DATE NOT NULL DEFAULT 0, -- Move to seperate table ?
		"success_at"   DATE NOT NULL DEFAULT 0,

		"created_at"   DATE NOT NULL DEFAULT (strftime('%s', 'now')),
		"updated_at"   DATE NOT NULL,

		UNIQUE (ip, port)
	);
	`

const INIT_SCHEMA_NODES_KNOWN = `
	CREATE TABLE IF NOT EXISTS "nodes_known" (
		"id" INTEGER PRIMARY KEY,

		"id_source" INTEGER,
		"id_known" INTEGER,

		"created_at" DATE DEFAULT (strftime('%s', 'now')),
		"updated_at" DATE,

		UNIQUE (id_source, id_known)
	);
	`

const INDEX_IP_PORT = "CREATE INDEX IF NOT EXISTS node_ip_port ON nodes (ip, port);"
const INDEX_SOURCE_KNOWN = "CREATE INDEX IF NOT EXISTS nodes_known_source_known ON nodes_known (id_source, id_known);"

var dbConnectionPool chan *sql.DB

// Initialize pool of DB connections
func initDB() (err error) {
	log.Print("Initializing DB connections")

	dbConnectionPool = make(chan *sql.DB, NUM_DB_CONN)
	for i := 0; i < NUM_DB_CONN; i++ {
		db, err := sql.Open("sqlite3", "data.db")
		if err != nil {
			return err
		}

		if _, err = db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
			log.Fatal("Failed to Exec PRAGMA journal_mode:", err)
		}

		dbConnectionPool <- db
	}

	db := acquireDBConn()
	defer releaseDBConn(db)

	setupDB(db)

	return
}

// Set up the database schema
func setupDB(db *sql.DB) {
	for _, q := range []string{
		INIT_SCHEMA_NODES,
		INIT_SCHEMA_NODES_KNOWN,
		INDEX_IP_PORT,
		INDEX_SOURCE_KNOWN,
	} {
		_, err := db.Exec(q)
		if err != nil {
			logQueryError(q, err)
		}
	}
}

// Clean up pool of DB connections
func cleanDB() {
	log.Print("Cleaning up DB connections")

	for i := 0; i < NUM_DB_CONN; i++ {
		db := <-dbConnectionPool
		db.Close()
	}
}

// Get a connection from the pool of DB connections
func acquireDBConn() (db *sql.DB) {
	return <-dbConnectionPool
}

// Release a connection back to the pool of DB connections
func releaseDBConn(db *sql.DB) {
	dbConnectionPool <- db
}

// Returns whether there are nodes in the DB which can be used to crawl the
// bitcoin network
func haveKnownNodes() bool {
	db := acquireDBConn()
	defer releaseDBConn(db)

	row := db.QueryRow(`SELECT COUNT(*) 
		FROM nodes 
		WHERE success = 1`)

	var count int
	err := row.Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	return count != 0
}

// Retrieves addresses which need to be updated
func addressesToUpdate() (addresses []ip_port, max int) {
	db := acquireDBConn()
	defer releaseDBConn(db)

	query := fmt.Sprintf(`SELECT ip, port 
		FROM nodes 
		WHERE port!=0
			AND next_refresh < strftime('%%s', 'now')
		ORDER BY next_refresh
		LIMIT %d`, ADDRESSES_NUM)

	rows, err := db.Query(query)
	if err != nil {
		logQueryError(query, err)
	}

	var ip, port string
	addresses = make([]ip_port, 0, ADDRESSES_NUM)

	for rows.Next() {
		rows.Scan(&ip, &port)
		// if verbose {
		// 	log.Print("Getting ", ip, " ", port)
		// }
		addresses = append(addresses, ip_port{ip: ip, port: port})
	}

	// Get max count
	query = `SELECT COUNT(*) 
		FROM nodes 
		WHERE port!=0
			AND next_refresh < strftime('%s', 'now')`

	row := db.QueryRow(query)
	err = row.Scan(&max)
	if err != nil {
		logQueryError(query, err)
	}

	return addresses, max
}

// Save the node to the database
func (node *Node) Save(db *sql.DB) (err error) {
	dbnode := nodeDB{node: node}
	return dbnode.Save(db)
}

// Save or the node to the database. The relation to other nodes is also saved.
func (n *nodeDB) Save(db *sql.DB) (err error) {
	n.dbInfo = dbNodeInfo{
		ip:   n.node.NetAddr.IP.String(),
		port: strconv.Itoa(int(n.node.NetAddr.Port)),
	}

	if n.node.Version != nil {
		if n.node.Version.UserAgent != "" || len(n.node.Addresses) > 0 {
			log.Print(n.node.Version.UserAgent, " ", len(n.node.Addresses),
				" peers ", n.dbInfo.ip, " ", n.dbInfo.port)
		}
	}

	n.tx, err = db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer n.tx.Rollback()

	// Get existing information from current node if any
	n.dbGetNode()
	// Update last updated time
	n.now = time.Now().Unix()

	//Was able to connect to node
	if n.node.Conn == nil {
		n.dbInfo.online = false
		n.dbInfo.next_refresh = 0 // stop updating node
	} else {
		n.dbInfo.online = true
		n.dbInfo.online_at = n.now

		n.dbInfo.next_refresh = n.now + (NODE_REFRESH_INTERVAL * 3600)
	}

	// Was able initiate communication with node
	if n.node.Version != nil {
		n.dbInfo.protocol = int(n.node.Version.Protocol)
		n.dbInfo.user_agent = n.node.Version.UserAgent

		n.dbInfo.success = true
		n.dbInfo.success_at = n.now
	} else {
		n.dbInfo.success = false
	}

	n.dbPutNode()

	// Update neighbour nodes

	// Initialize struct and get existing information on neighnours, if any
	n.dbGetNeighbours()

	// Update next_refresh if necessary
	for _, addr := range n.node.Addresses {
		canon_addr := net.JoinHostPort(addr.IP.String(), strconv.Itoa(int(addr.Port)))

		neigh := n.dbNeighbours[canon_addr]
		if neigh.next_refresh < n.now {
			neigh.next_refresh = n.dbInfo.next_refresh
			n.dbNeighbours[canon_addr] = neigh
		}
	}
	n.dbPutNeighbours()

	err = n.tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	return
}

// Retrive database information about a single node
func (n *nodeDB) dbGetNode() {
	if n.tx == nil {
		log.Fatal("Transaction not initialized")
	}

	// Get dates with strftime to get timestamps
	query := `SELECT id, protocol, user_agent, online, online_at, 
				success, success_at, next_refresh
			FROM nodes 
			WHERE ip=?
  			  AND port=?`
	row := n.tx.QueryRow(query, n.dbInfo.ip, n.dbInfo.port)

	err := row.Scan(&(n.dbInfo.id), &(n.dbInfo.protocol), &(n.dbInfo.user_agent),
		&(n.dbInfo.online), &(n.dbInfo.online_at),
		&(n.dbInfo.success), &(n.dbInfo.success_at),
		&(n.dbInfo.next_refresh))

	// Ignore if err if node does not exist
	switch {
	case err == sql.ErrNoRows:
		n.dbInfo.id = -1
	case err != nil:
		logQueryError(query, err)
	}
}

// Retrieve only the id for the given node
func (n *nodeDB) dbGetNodeId() {
	if n.tx == nil {
		log.Fatal("Transaction not initialized")
	}

	// Get dates with strftime to get timestamps
	query := `SELECT id
			FROM nodes 
			WHERE ip=?
  			  AND port=?`
	row := n.tx.QueryRow(query, n.dbInfo.ip, n.dbInfo.port)

	err := row.Scan(&(n.dbInfo.id))

	// Ignore if err if node does not exist
	switch {
	case err == sql.ErrNoRows:
		n.dbInfo.id = -1
	case err != nil:
		logQueryError(query, err)
	}
}

// Save a node to the DB and store its id
func (n *nodeDB) dbPutNode() {
	if n.tx == nil {
		log.Fatal("Transaction not initialized")
	}

	// Retrieve info from DB if state unknown
	if n.dbInfo.id == ID_UNKNOWN {
		n.dbGetNodeId()
	}

	var (
		err   error
		query string
	)
	params := [11]interface{}{n.dbInfo.ip, n.dbInfo.port, n.dbInfo.next_refresh,
		n.dbInfo.protocol, n.dbInfo.user_agent,
		n.dbInfo.online, n.dbInfo.online_at,
		n.dbInfo.success, n.dbInfo.success_at,
		n.now, 0}

	if n.dbInfo.id == ID_NOT_IN_DB {
		query = `INSERT INTO nodes (ip, port, next_refresh, protocol, user_agent, 
					online, online_at, success, success_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		_, err = n.tx.Exec(query, params[:10]...)
	} else {
		query = `UPDATE nodes SET ip=?, port=?, next_refresh=?, protocol=?, 
					user_agent=?, online=?, online_at=?, success=?, success_at=?, 
					updated_at=?
					WHERE id=?`
		params[10] = n.dbInfo.id
		_, err = n.tx.Exec(query, params[:11]...)
	}

	if err != nil {
		logQueryError(query, err)
	}

	// Retrieve the inserted row's id if previously unknown
	if n.dbInfo.id == ID_UNKNOWN || n.dbInfo.id == ID_NOT_IN_DB {
		n.dbGetNode()
	}
}

// Gets id and next_refresh for neighbour nodes. Stores in n.dbNeighbours
// Uses prepared statements insted of creating one big query
func (n *nodeDB) dbGetNeighbours() {

	if n.node.Addresses == nil {
		return
	}

	var init = (n.dbNeighbours == nil)

	// Initialize neighbours map if this is the first call to dbGetNeighbours
	if init {
		n.dbNeighbours = make(map[string]dbNeighbourInfo)
	}

	// Prepare query
	query := "SELECT id, next_refresh FROM nodes WHERE ip=? AND port=?"
	stmt, err := n.tx.Prepare(query)
	if err != nil {
		logQueryError(query, err)
	}
	defer stmt.Close()

	var (
		row        *sql.Row
		neigh      dbNeighbourInfo
		canon_addr string

		id           int64
		ip           string
		port         string
		next_refresh int64
	)

	// Retrieve neighbour information
	for i := 0; i < len(n.node.Addresses); i++ {
		ip = n.node.Addresses[i].IP.String()
		port = strconv.Itoa(int(n.node.Addresses[i].Port))
		canon_addr = net.JoinHostPort(ip, port)

		row = stmt.QueryRow(ip, port)
		err = row.Scan(&id, &next_refresh)

		switch {
		case err == sql.ErrNoRows:
			neigh = dbNeighbourInfo{
				id: ID_NOT_IN_DB,
			}
		case err != nil:
			// Unexpected DB error
			log.Fatal(err)
		default:
			if !init {
				// Update existing
				neigh = n.dbNeighbours[canon_addr]
				neigh.id = id
				neigh.next_refresh = next_refresh
			} else {
				// Create new
				neigh = dbNeighbourInfo{
					id:           id,
					next_refresh: next_refresh,
				}
			}
		}

		n.dbNeighbours[canon_addr] = neigh
	}
}

// Update neighbour nodes and relations in DB
func (n *nodeDB) dbPutNeighbours() {
	if len(n.dbNeighbours) == 0 {
		return
	}

	if n.dbInfo.id == ID_UNKNOWN || n.dbInfo.id == ID_NOT_IN_DB {
		n.dbGetNodeId()
		if n.dbInfo.id == ID_UNKNOWN || n.dbInfo.id == ID_NOT_IN_DB {
			log.Fatal("Attempted to insert neighbours for a node which is not in DB")
		}
	}

	// Prepare node queries
	select_node_query := "SELECT id FROM nodes WHERE ip=? AND port=?"
	select_node_stmt, err := n.tx.Prepare(select_node_query)
	if err != nil {
		logQueryError(select_node_query, err)
	}
	defer select_node_stmt.Close()

	insert_node_query := "INSERT INTO nodes (ip, port, next_refresh, updated_at) VALUES (?, ?, ?, ?)"
	insert_node_stmt, err := n.tx.Prepare(insert_node_query)
	if err != nil {
		logQueryError(insert_node_query, err)
	}
	defer insert_node_stmt.Close()

	update_node_query := "UPDATE nodes SET next_refresh=?, updated_at=? WHERE id=?"
	update_node_stmt, err := n.tx.Prepare(update_node_query)
	if err != nil {
		logQueryError(update_node_query, err)
	}
	defer update_node_stmt.Close()

	// Prepare known nodes queries
	select_known_query := "SELECT id FROM nodes_known WHERE id_source=? AND id_known=?"
	select_known_stmt, err := n.tx.Prepare(select_known_query)
	if err != nil {
		logQueryError(select_known_query, err)
	}
	defer select_known_stmt.Close()

	insert_known_query := "INSERT INTO nodes_known (id_source, id_known, updated_at) VALUES (?, ?, ?)"
	insert_known_stmt, err := n.tx.Prepare(insert_known_query)
	if err != nil {
		logQueryError(insert_known_query, err)
	}
	defer insert_known_stmt.Close()

	update_known_query := "UPDATE nodes_known SET updated_at=? WHERE id=?"
	update_known_stmt, err := n.tx.Prepare(update_known_query)
	if err != nil {
		logQueryError(update_known_query, err)
	}
	defer update_known_stmt.Close()

	// Insert nodes
	var (
		row *sql.Row

		id_rel int64
		ip     string
		port   string
	)
	for hostport, info := range n.dbNeighbours {
		ip, port, err = net.SplitHostPort(hostport)
		if err != nil {
			log.Fatal(err)
		}

		// Check if node is in DB if currently unknown
		if info.id == ID_UNKNOWN {
			row = select_node_stmt.QueryRow(ip, port)

			err = row.Scan(&(info.id))

			switch {
			case err == sql.ErrNoRows:
				info.id = ID_NOT_IN_DB
			case err != nil:
				// Unexpected DB error
				log.Fatal(err)
			}
		}

		// Insert/update node in DB
		if info.id == ID_NOT_IN_DB {
			// insert
			_, err = insert_node_stmt.Exec(ip, port, info.next_refresh, n.now)
			if err != nil {
				log.Fatal(err)
			}

			// retrieve new id
			row = select_node_stmt.QueryRow(ip, port)
			err = row.Scan(&(info.id))
			if err != nil {
				log.Fatal(err)
			}
		} else {
			//update
			_, err = update_node_stmt.Exec(info.next_refresh, n.now, info.id)
			if err != nil {
				log.Fatal(err)
			}
		}

		// insert/update known nodes relation
		row = select_known_stmt.QueryRow(n.dbInfo.id, info.id)
		err = row.Scan(&id_rel)

		switch {
		case err == sql.ErrNoRows:
			_, err = insert_known_stmt.Exec(n.dbInfo.id, info.id, n.now)
			if err != nil {
				log.Fatal(err)
			}
		case err != nil:
			log.Fatal(err)
		default:
			_, err = update_known_stmt.Exec(n.now, id_rel)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

// Log a query error. Calls os.Exit(1)
func logQueryError(query string, err error) {
	log.Print(query)
	log.Fatal(err)
}
