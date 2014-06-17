package main

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"time"
)

const INIT_SCHEMA_NODES = `
	CREATE TABLE IF NOT EXISTS "nodes" (
		"id" INTEGER PRIMARY KEY,
		"ip" TEXT,
		"port" INTEGER,
		"protocol" INTEGER,
		"user_agent" TEXT,
		"valid" BOOL,
		"refresh" BOOL,
		"created_at" DATETIME,
		"updated_at" DATETIME
	);`

const INIT_SCHEMA_NODES_KNOWN = `
	CREATE TABLE IF NOT EXISTS "nodes_known" (
		"id" INTEGER PRIMARY KEY,
		"id_source" INTEGER,
		"id_known" INTEGER
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

// nodes table
type NodeDB struct {
	Id int64

	Ip        string
	Port      int
	Protocol  int
	UserAgent string

	Valid   bool
	Refresh bool

	CreatedAt time.Time
	UpdatedAt time.Time
}

// nodes_known table
type NodesKnownDB struct {
	Id int64

	IdSource int64
	IdKnown  int64
}

func (node Node) Save() (err error) {
	db := connectDB()
	// Convert node data to string
	local := make(map[string]interface{})

	log.Print("Saving ", node.NetAddr, " v: ", node.Version != nil,
		" n: ", len(node.Addresses))
	local["ip"] = node.NetAddr.IP.String()
	local["port"] = node.NetAddr.Port

	if node.Version != nil {
		local["protocol"] = node.Version.Protocol
		local["user_agent"] = node.Version.UserAgent

		local["valid"] = "1"
		local["refresh"] = "0"
	} else {
		local["protocol"] = ""
		local["user_agent"] = ""

		local["valid"] = "0"
		local["refresh"] = "0"
	}

	tx, err := db.Begin()
	defer tx.Rollback()

	if err != nil {
		log.Fatal(err)
	}

	rows, err := tx.Query(`SELECT id
		FROM nodes
		WHERE ip=? AND port=?;`, local["ip"], local["port"])

	if err != nil {
		log.Fatal(err)
	}

	var (
		local_id int64
		res      sql.Result
	)
	if rows.Next() {
		rows.Scan(&local_id)
		rows.Close()

		res, err = tx.Exec(`UPDATE nodes 
			SET protocol=?, user_agent=?, valid=?, refresh=?
			WHERE id=?;`,
			local["protocol"], local["user_agent"], local["valid"], local["refresh"],
			local_id)
	} else {

		res, err = tx.Exec(`INSERT INTO nodes 
			(ip, port, protocol, user_agent, valid, refresh) VALUES (?, ?, ?, ?, ?, ?);`,
			local["ip"], local["port"], local["protocol"], local["user_agent"],
			local["valid"], local["refresh"])

		local_id, err = res.LastInsertId()
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
				res, err = tx.Exec("INSERT INTO nodes (ip, port, refresh) VALUES (?, ?, ?);",
					addr.IP.String(), addr.Port, "1")
				if err != nil {
					log.Fatal(err)
				}

				remote_id, err = res.LastInsertId()
			}
			rows.Close()

			res, err = tx.Exec("INSERT INTO nodes_known (id_source, id_known) VALUES (?, ?);",
				local_id, remote_id)
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
