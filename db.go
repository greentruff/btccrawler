package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

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

func connectDB() (db *sql.DB) {
	db, err := sql.Open("sqlite3", "data.db")
	if err != nil {
		log.Fatal(err)
	}
	return
}

func initDB() (err error) {
	db := connectDB()
	defer db.Close()

	_, err = db.Exec(INIT_SCHEMA_NODES)
	if err != nil {
		return
	}

	_, err = db.Exec(INIT_SCHEMA_NODES_KNOWN)
	if err != nil {
		return
	}

	return
}

// Save a node to persistence. If the node does not already exist in the DB,
// create it. The relation to other nodes is also saved.
func (node Node) Save() (err error) {
	db := connectDB()
	var query string
	var query_values []interface{}

	ip := node.NetAddr.IP.String()
	port := node.NetAddr.Port

	// Columns to set/update
	col := make(map[string]interface{})

	log.Print("Saving ", ip, " ", port, " v: ", node.Version != nil, " n: ", len(node.Addresses))

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
		query, query_values = makeUpdateQuery(node_id, col)
		res, err = tx.Exec(query, query_values...)
	} else {
		col["created_at"] = "NOW()"
		col["updated_at"] = "NOW()"
		query, query_values = makeInsertQuery(col)
		res, err = tx.Exec(query, query_values...)

		node_id, err = res.LastInsertId()
	}
	if err != nil {
		log.Fatal(err)
	}

	var remote_id int64
	if node.Addresses != nil {
		for _, addr := range node.Addresses {
			rows, err = tx.Query("SELECT * FROM nodes WHERE ip=? AND port=?;",
				addr.IP.String(), addr.Port)
			if err != nil {
				log.Fatal(err)
			}

			if !rows.Next() {
				query, query_values = makeInsertQuery(map[string]interface{}{
					"ip":      addr.IP.String(),
					"port":    addr.Port,
					"refresh": 1,
				})

				res, err = tx.Exec(query, query_values...)
				if err != nil {
					log.Fatal(err)
				}

				remote_id, err = res.LastInsertId()
			}
			rows.Close()

			res, err = tx.Exec("INSERT INTO nodes_known (id_source, id_known) VALUES (?, ?);",
				node_id, remote_id)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	return
}

func makeInsertQuery(cols map[string]interface{}) (query string, query_values []interface{}) {
	query = "INSERT INTO nodes (%s) VALUES (%s)"

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

func makeUpdateQuery(id int64, cols map[string]interface{}) (query string, query_values []interface{}) {
	query = "UPDATE nodes SET %s WHERE id=?"

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
