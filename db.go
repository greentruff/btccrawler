package main

import (
	"database/sql"
	"fmt"
	"log"
	"runtime/pprof"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type ip_port struct {
	ip   string
	port string
}

const INIT_SCHEMA_NODES = `
	CREATE TABLE IF NOT EXISTS "nodes" (
		"id" INTEGER PRIMARY KEY,
		"ip" TEXT,
		"port" INTEGER,
		"protocol" INTEGER,
		"user_agent" TEXT,

		"online" BOOL, 
		"success" BOOL,

		"next_refresh" DATETIME,

		"online_at" DATETIME,
		"success_at" DATETIME,

		"created_at" DATETIME,
		"updated_at" DATETIME
	);
	`

const INIT_SCHEMA_NODES_KNOWN = `
	CREATE TABLE IF NOT EXISTS "nodes_known" (
		"id" INTEGER PRIMARY KEY,

		"id_source" INTEGER,
		"id_known" INTEGER,

		"created_at" DATETIME,
		"updated_at" DATETIME
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
		dbConnectionPool <- db
	}

	db := acquireDBConn()
	defer releaseDBConn(db)

	for _, q := range []string{
		INIT_SCHEMA_NODES,
		INIT_SCHEMA_NODES_KNOWN,
		INDEX_IP_PORT,
		INDEX_SOURCE_KNOWN,
	} {
		_, err = db.Exec(q)
		if err != nil {
			logQueryError(q, err)
		}
	}

	return
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
			AND next_refresh < datetime()
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
		addresses = append(addresses, ip_port{ip: ip, port: port})
	}

	// Get max count
	query = `SELECT COUNT(*) 
		FROM nodes 
		WHERE port!=0
			AND next_refresh < datetime()`

	row := db.QueryRow(query)
	err = row.Scan(&max)
	if err != nil {
		logQueryError(query, err)
	}

	return addresses, max
}

// Save a node to persistence. If the node does not already exist in the DB,
// create it. The relation to other nodes is also saved.
func (node Node) Save() (err error) {
	db := acquireDBConn()
	defer releaseDBConn(db)

	var query string

	ip := node.NetAddr.IP.String()
	port := node.NetAddr.Port

	// Columns to set/update
	col := make(map[string]interface{})

	if node.Version != nil {
		log.Print("Saving ", ip, " ", port, " v: ", node.Version.UserAgent, " n: ", len(node.Addresses))

		// Update heap profile on each success
		if heapprofile != "" {
			defer pprof.WriteHeapProfile(fheap)
		}
	}

	col["ip"] = "'" + ip + "'"
	col["port"] = port

	// Able to TCP connect to node
	if node.Conn == nil {
		col["online"] = "0"
		// Do not refresh until the node is seen again as a neighbour
		col["next_refresh"] = "NULL"
	} else {
		col["online"] = "1"
		col["online_at"] = "datetime()"

		col["next_refresh"] = fmt.Sprintf("datetime('now', '+%d HOURS')",
			NODE_REFRESH_INTERVAL)
	}

	// Able initiate communication with node
	if node.Version != nil {
		col["protocol"] = node.Version.Protocol
		col["user_agent"] = "'" + node.Version.UserAgent + "'"

		col["success"] = "1"
		col["success_at"] = "datetime()"
	} else {
		col["success"] = "0"
	}

	// Begin saving to DB
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	// Find if node has already been contacted
	rows, err := tx.Query("SELECT id FROM nodes WHERE ip=? AND port=?", ip, port)
	if err != nil {
		logQueryError(query, err)
	}

	var (
		node_id int64
		res     sql.Result
	)
	if rows.Next() {
		rows.Scan(&node_id)
		rows.Close()

		//Node exists, update
		col["updated_at"] = "datetime()"

		query = makeUpdateQuery("nodes", node_id, col)
		res, err = tx.Exec(query)

		if err != nil {
			logQueryError(query, err)
		}
	} else {
		col["created_at"] = "datetime()"
		col["updated_at"] = "datetime()"
		query = makeInsertQuery("nodes", col)
		res, err = tx.Exec(query)

		if err != nil {
			logQueryError(query, err)
		}

		node_id, err = res.LastInsertId()
	}

	// Save known nodes
	var (
		remote_id, known_node_id int64
		recent_update            int
		remote_next_refresh      string
	)

	if node.Addresses != nil {
		for _, addr := range node.Addresses {
			query = fmt.Sprintf(`SELECT id, 
					datetime(updated_at, '+%d HOURS') > datetime(), 
					next_refresh
				FROM nodes 
				WHERE ip=? AND port=?`, NODE_REFRESH_INTERVAL)
			rows, err = tx.Query(query,
				addr.IP.String(), addr.Port)
			if err != nil {
				logQueryError(query, err)
			}

			if rows.Next() {
				// Remote node already known
				rows.Scan(&remote_id, &recent_update, &remote_next_refresh)
				rows.Close()

				// Check if already a known neighbour and insert/update accordingly
				rows, err = tx.Query("SELECT id FROM nodes_known WHERE id_source=? AND id_known=?;",
					node_id, remote_id)
				if err != nil {
					logQueryError(query, err)
				}

				if rows.Next() {
					rows.Scan(&known_node_id)
					rows.Close()

					query = makeUpdateQuery("nodes_known", known_node_id,
						map[string]interface{}{
							"updated_at": "datetime()",
						})
				} else {
					query = makeInsertQuery("nodes_known",
						map[string]interface{}{
							"created_at": "datetime()",
							"updated_at": "datetime()",
						})
				}
				res, err = tx.Exec(query)
				if err != nil {
					logQueryError(query, err)
				}

				// Set the next refresh date if necessary
				if remote_next_refresh == "" && recent_update == 0 {
					query = makeUpdateQuery("nodes", remote_id, map[string]interface{}{
						"next_refresh": fmt.Sprintf("datetime('now', '+%d HOURS')", NODE_REFRESH_INTERVAL),
					})

					res, err = tx.Exec(query)
					if err != nil {
						logQueryError(query, err)
					}
				}
			} else {
				// New peer node
				query = makeInsertQuery("nodes", map[string]interface{}{
					"ip":   "'" + addr.IP.String() + "'",
					"port": addr.Port,

					"created_at":   "datetime()",
					"updated_at":   "datetime()",
					"next_refresh": "datetime()",
				})

				res, err = tx.Exec(query)
				if err != nil {
					logQueryError(query, err)
				}

				remote_id, err = res.LastInsertId()

				query = makeInsertQuery("nodes_known",
					map[string]interface{}{
						"created_at": "datetime()",
						"updated_at": "datetime()",
					})
				res, err = tx.Exec(query)
				if err != nil {
					logQueryError(query, err)
				}
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	return
}

// Make a parametrized SQL INSERT query and associated values for `table` using
// the columns in `cols`
func makeInsertQuery(table string, cols map[string]interface{}) (query string) {
	query = fmt.Sprintf("INSERT INTO %s (%%s) VALUES (%%s)", table)

	query_columns := make([]string, 0, len(cols))
	query_values := make([]string, 0, len(cols))

	for name, value := range cols {
		query_columns = append(query_columns, name)

		switch v := value.(type) {
		case string:
			query_values = append(query_values, v)
		case uint16:
			query_values = append(query_values, strconv.Itoa(int(v)))
		case uint32:
			query_values = append(query_values, strconv.Itoa(int(v)))
		case int64:
			query_values = append(query_values, strconv.Itoa(int(v)))
		default:
			panic(v)
		}
	}

	query = fmt.Sprintf(query, strings.Join(query_columns, ","), strings.Join(query_values, ","))

	return query
}

// Make an SQL UPDATE query for `table` updating the columns in `cols` for row with `id`
func makeUpdateQuery(table string, id int64, cols map[string]interface{}) (query string) {
	query = fmt.Sprintf("UPDATE %s SET %%s WHERE id=%d", table, id)

	query_columns := make([]string, 0, len(cols))

	for name, value := range cols {
		switch v := value.(type) {
		case string:
			query_columns = append(query_columns, name+"="+v)
		case uint16:
			query_columns = append(query_columns, name+"="+strconv.Itoa(int(v)))
		case uint32:
			query_columns = append(query_columns, name+"="+strconv.Itoa(int(v)))
		case int64:
			query_columns = append(query_columns, name+"="+strconv.Itoa(int(v)))
		default:
			panic(v)
		}
	}

	query = fmt.Sprintf(query, strings.Join(query_columns, ","))

	return query
}

// Log a query error. Calls os.Exit(1)
func logQueryError(query string, err error) {
	log.Print(query)
	log.Fatal(err)
}
