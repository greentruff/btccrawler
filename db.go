package main

import (
	"database/sql"
	"fmt"
	"log"
	"runtime/pprof"
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

func acquireDBConn() (db *sql.DB) {
	return <-dbConnectionPool
}

func releaseDBConn(db *sql.DB) {
	dbConnectionPool <- db
}

func initDB() (err error) {
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
			return
		}
	}

	return
}

func cleanDB() {
	for i := 0; i < NUM_DB_CONN; i++ {
		db := <-dbConnectionPool
		db.Close()
	}
}

func addressesToUpdate() (addresses []ip_port) {
	db := acquireDBConn()
	defer releaseDBConn(db)

	rows, err := db.Query("SELECT ip, port FROM nodes WHERE updated_at IS NULL AND port!=0")
	if err != nil {
		log.Fatal(err)
	}

	var ip, port string
	addresses = make([]ip_port, 0)

	for rows.Next() {
		rows.Scan(&ip, &port)
		addresses = append(addresses, ip_port{ip: ip, port: port})
	}

	return addresses
}

// Save a node to persistence. If the node does not already exist in the DB,
// create it. The relation to other nodes is also saved.
func (node Node) Save() (err error) {
	db := acquireDBConn()
	defer releaseDBConn(db)

	var query string
	var query_values []interface{}

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

	// Unable to connect to node

	if node.Conn == nil {
		col["online"] = "0"
	} else {
		col["online"] = "1"
		col["online_at"] = "NOW()"
	}

	// Able to communicate with node
	if node.Version != nil {
		col["protocol"] = node.Version.Protocol
		col["user_agent"] = node.Version.UserAgent

		col["success"] = "1"
		col["success_at"] = "NOW()"
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
	rows, err := tx.Query("SELECT id FROM nodes	WHERE ip=? AND port=?;", ip, port)
	if err != nil {
		log.Fatal(err)
	}

	var (
		node_id int64
		res     sql.Result
	)
	if rows.Next() {
		rows.Scan(&node_id)
		rows.Close()

		//Node exists, update
		col["updated_at"] = "NOW()"
		query, query_values = makeUpdateQuery("nodes", node_id, col)
		res, err = tx.Exec(query, query_values...)
	} else {
		col["created_at"] = "NOW()"
		col["updated_at"] = "NOW()"
		query, query_values = makeInsertQuery("nodes", col)
		res, err = tx.Exec(query, query_values...)

		node_id, err = res.LastInsertId()
	}
	if err != nil {
		log.Fatal(err)
	}

	var remote_id, known_node_id int64
	if node.Addresses != nil {
		for _, addr := range node.Addresses {
			rows, err = tx.Query("SELECT id FROM nodes WHERE ip=? AND port=?;",
				addr.IP.String(), addr.Port)
			if err != nil {
				log.Fatal(err)
			}

			if !rows.Next() {
				// New peer node
				query, query_values = makeInsertQuery("nodes", map[string]interface{}{
					"ip":   addr.IP.String(),
					"port": addr.Port,

					"created_at": "NOW()",
				})

				res, err = tx.Exec(query, query_values...)
				if err != nil {
					log.Fatal(err)
				}

				remote_id, err = res.LastInsertId()

				query, query_values = makeInsertQuery("nodes_known",
					map[string]interface{}{
						"created_at": "NOW()",
						"updated_at": "NOW()",
					})
				res, err = tx.Exec(query, query_values...)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				// Remote node already known
				rows.Scan(&remote_id)
				rows.Close()

				rows, err = tx.Query("SELECT id FROM nodes_known WHERE id_source=? AND id_known=?;",
					node_id, remote_id)
				if err != nil {
					log.Fatal(err)
				}

				if rows.Next() {
					rows.Scan(&known_node_id)
					rows.Close()

					query, query_values = makeUpdateQuery("nodes_known", known_node_id,
						map[string]interface{}{
							"updated_at": "NOW()",
						})
				} else {
					query, query_values = makeInsertQuery("nodes_known",
						map[string]interface{}{
							"created_at": "NOW()",
							"updated_at": "NOW()",
						})
				}
				res, err = tx.Exec(query, query_values...)
				if err != nil {
					log.Fatal(err)
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

func makeInsertQuery(table string, cols map[string]interface{}) (query string, query_values []interface{}) {
	query = fmt.Sprintf("INSERT INTO %s (%%s) VALUES (%%s)", table)

	query_columns := make([]string, len(cols))
	query_placeholders := make([]string, len(cols))
	query_values = make([]interface{}, len(cols))
	idx := 0

	for name, value := range cols {
		query_columns[idx] = name
		query_placeholders[idx] = "?"
		query_values[idx] = value

		idx += 1
	}

	query = fmt.Sprintf(query, strings.Join(query_columns, ","), strings.Join(query_placeholders, ","))

	return query, query_values
}

func makeUpdateQuery(table string, id int64, cols map[string]interface{}) (query string, query_values []interface{}) {
	query = fmt.Sprintf("UPDATE %s SET %%s WHERE id=?", table)

	query_columns := make([]string, len(cols))
	query_values = make([]interface{}, len(cols)+1)
	idx := 0

	for name, value := range cols {
		query_columns[idx] = name + "=?"
		query_values[idx] = value

		idx += 1
	}
	query_values[idx] = id

	query = fmt.Sprintf(query, strings.Join(query_columns, ","))

	return query, query_values
}
