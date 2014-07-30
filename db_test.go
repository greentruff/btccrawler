package main

import (
	"database/sql"
	"net"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// Get a connection to an temporary empty DB
func tempDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	setupDB(db)

	return db
}

func TestDbGetNode(t *testing.T) {
	var err error
	db := tempDB(t)
	defer db.Close()

	// TEST: No node in DB
	// Info should be untouched if node does not exist
	n := &nodeDB{}

	n.tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	n.dbInfo = dbNodeInfo{
		ip:   "test",
		port: "999",
	}

	n.dbGetNode()

	expected := dbNodeInfo{
		id:   ID_NOT_IN_DB,
		ip:   "test",
		port: "999",
	}
	if !reflect.DeepEqual(n.dbInfo, expected) {
		t.Error("Non existing node expected ", expected, " got ", n.dbInfo)
	}

	n.tx.Rollback()

	// TEST: Node exists in DB
	n.tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = n.tx.Exec(`INSERT INTO nodes (id, ip, port, next_refresh, protocol, 
										user_agent, online, 
										online_at, success, success_at, 
										updated_at) VALUES 
						(5, 'ip', '999', 456, 27, 'user_agent', 1, 123, 1, 321, 234)`)
	if err != nil {
		t.Fatal(err)
	}

	n.dbInfo = dbNodeInfo{
		ip:   "ip",
		port: "999",
	}
	n.dbGetNode()

	expected = dbNodeInfo{
		id:           5,
		ip:           "ip",
		port:         "999",
		next_refresh: 456,
		protocol:     27,
		user_agent:   "user_agent",
		online:       true,
		online_at:    123,
		success:      true,
		success_at:   321,
	}

	if !reflect.DeepEqual(n.dbInfo, expected) {
		t.Error("Existing node expected ", expected, " got ", n.dbInfo)
	}

	n.tx.Rollback()
}

func TestDbPutNode(t *testing.T) {
	var err error
	db := tempDB(t)
	defer db.Close()

	// TEST: Add a new Node
	n := &nodeDB{}

	n.tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	n.dbInfo = dbNodeInfo{
		ip:           "ip",
		port:         "999",
		next_refresh: 456,
		protocol:     27,
		user_agent:   "user_agent",
		online:       true,
		online_at:    123,
		success:      true,
		success_at:   321,
	}
	n.dbPutNode()

	got := dbNodeInfo{}
	row := n.tx.QueryRow(`SELECT id, ip, port, next_refresh, protocol, 
		user_agent, online, online_at, success, success_at 
		FROM nodes WHERE ip='ip' AND port='999'`)
	err = row.Scan(&(got.id), &(got.ip), &(got.port), &(got.next_refresh),
		&(got.protocol), &(got.user_agent), &(got.online), &(got.online_at),
		&(got.success), &(got.success_at))
	if err != nil {
		t.Fatal(err)
	}

	expected := dbNodeInfo{
		ip:           "ip",
		port:         "999",
		next_refresh: 456,
		protocol:     27,
		user_agent:   "user_agent",
		online:       true,
		online_at:    123,
		success:      true,
		success_at:   321,
	}
	expected.id = got.id

	if !reflect.DeepEqual(expected, got) {
		t.Error("New node expected ", expected, " got ", got)
	}
	if n.dbInfo.id != got.id {
		t.Error("New node id not updated expected ", got.id, " got ", n.dbInfo.id)
	}

	n.tx.Rollback()

	// TEST: Update an existing node
	n.tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = n.tx.Exec(`INSERT INTO nodes (id, ip, port, next_refresh, protocol, 
										user_agent, online, 
										online_at, success, success_at, 
										updated_at) VALUES 
						(5, 'ip', '999', 99456, 9927, '99user_agent', 0, 99123, 0, 99321, 99234)`)
	if err != nil {
		t.Fatal(err)
	}

	n.dbInfo = dbNodeInfo{
		ip:           "ip",
		port:         "999",
		next_refresh: 456,
		protocol:     27,
		user_agent:   "user_agent",
		online:       true,
		online_at:    123,
		success:      true,
		success_at:   321,
	}
	n.dbPutNode()

	got = dbNodeInfo{}
	row = n.tx.QueryRow(`SELECT id, ip, port, next_refresh, protocol, 
		user_agent, online, online_at, success, success_at
		FROM nodes WHERE ip='ip' AND port='999'`)
	err = row.Scan(&(got.id), &(got.ip), &(got.port), &(got.next_refresh),
		&(got.protocol), &(got.user_agent), &(got.online), &(got.online_at),
		&(got.success), &(got.success_at))
	if err != nil {
		t.Fatal(err)
	}

	expected = dbNodeInfo{
		id:           5,
		ip:           "ip",
		port:         "999",
		next_refresh: 456,
		protocol:     27,
		user_agent:   "user_agent",
		online:       true,
		online_at:    123,
		success:      true,
		success_at:   321,
	}

	if !reflect.DeepEqual(expected, got) {
		t.Error("Updating node expected ", expected, " got ", got)
	}
	if n.dbInfo.id != got.id {
		t.Error("Updating node id not updated expected ", got.id, " got ", n.dbInfo.id)
	}

	n.tx.Rollback()
}

func TestDbGetNeighbours(t *testing.T) {
	var err error
	db := tempDB(t)
	defer db.Close()

	// Add nodes to the DB
	stmt, err := db.Prepare("INSERT INTO nodes (id, ip, port, next_refresh) VALUES (?,?,?,?)")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		id := i + 1
		ip := net.IPv4(byte(i), byte(i), byte(i), byte(i))
		port := uint16(i)
		next_refresh := i * 2
		_, err = stmt.Exec(id, ip.String(), port, next_refresh)
		if err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()

	// TEST: Exising nodes
	n := &nodeDB{}
	n.tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	addresses := []NetAddr{
		NetAddr{IP: net.IPv4(0, 0, 0, 0), Port: uint16(0)},
		NetAddr{IP: net.IPv4(1, 1, 1, 1), Port: uint16(1)},
		NetAddr{IP: net.IPv4(2, 2, 2, 2), Port: uint16(2)},
		NetAddr{IP: net.IPv4(3, 3, 3, 3), Port: uint16(3)},
	}

	n.node = &Node{
		Addresses: addresses[:],
	}
	n.dbGetNeighbours()

	expected := make(map[string]dbNeighbourInfo)
	expected["0.0.0.0:0"] = dbNeighbourInfo{id: 1, next_refresh: 0}
	expected["1.1.1.1:1"] = dbNeighbourInfo{id: 2, next_refresh: 2}
	expected["2.2.2.2:2"] = dbNeighbourInfo{id: 3, next_refresh: 4}
	expected["3.3.3.3:3"] = dbNeighbourInfo{id: 4, next_refresh: 6}

	if !reflect.DeepEqual(expected, n.dbNeighbours) {
		t.Error("Existing nodes expected ", expected, " got ", n.dbNeighbours)
	}
	n.tx.Rollback()

	// TEST: Non existing nodes
	n = &nodeDB{}
	n.tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	addresses = []NetAddr{
		NetAddr{IP: net.IPv4(10, 0, 0, 0), Port: uint16(0)},
		NetAddr{IP: net.IPv4(11, 1, 1, 1), Port: uint16(1)},
		NetAddr{IP: net.IPv4(12, 2, 2, 2), Port: uint16(2)},
		NetAddr{IP: net.IPv4(13, 3, 3, 3), Port: uint16(3)},
	}

	n.node = &Node{
		Addresses: addresses[:],
	}
	n.dbGetNeighbours()

	expected = make(map[string]dbNeighbourInfo)
	expected["10.0.0.0:0"] = dbNeighbourInfo{id: -1, next_refresh: 0}
	expected["11.1.1.1:1"] = dbNeighbourInfo{id: -1, next_refresh: 0}
	expected["12.2.2.2:2"] = dbNeighbourInfo{id: -1, next_refresh: 0}
	expected["13.3.3.3:3"] = dbNeighbourInfo{id: -1, next_refresh: 0}

	if !reflect.DeepEqual(expected, n.dbNeighbours) {
		t.Error("Non existing nodes expected ", expected, " got ", n.dbNeighbours)
	}
	n.tx.Rollback()

	// TEST: Update for known nodes
	// WARNING: reuses internal state from previous test
	n.tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	n.node.Addresses = append(n.node.Addresses, NetAddr{IP: net.IPv4(1, 1, 1, 1), Port: uint16(1)})
	n.node.Addresses = append(n.node.Addresses, NetAddr{IP: net.IPv4(2, 2, 2, 2), Port: uint16(2)})

	n.dbGetNeighbours()

	expected = make(map[string]dbNeighbourInfo)
	expected["10.0.0.0:0"] = dbNeighbourInfo{id: -1, next_refresh: 0}
	expected["11.1.1.1:1"] = dbNeighbourInfo{id: -1, next_refresh: 0}
	expected["12.2.2.2:2"] = dbNeighbourInfo{id: -1, next_refresh: 0}
	expected["13.3.3.3:3"] = dbNeighbourInfo{id: -1, next_refresh: 0}
	expected["1.1.1.1:1"] = dbNeighbourInfo{id: 2, next_refresh: 2}
	expected["2.2.2.2:2"] = dbNeighbourInfo{id: 3, next_refresh: 4}

	if !reflect.DeepEqual(expected, n.dbNeighbours) {
		t.Error("Update internal state ", expected, " got ", n.dbNeighbours)
	}
	n.tx.Rollback()
}

func TestDbPutNeighbours(t *testing.T) {
	var err error
	db := tempDB(t)
	defer db.Close()
	// Setup
	// Add nodes to the DB
	stmt, err := db.Prepare("INSERT INTO nodes (id, ip, port, next_refresh, updated_at) VALUES (?,?,?,?,?)")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		id := i + 1
		ip := net.IPv4(byte(i), byte(i), byte(i), byte(i))
		port := uint16(i)
		next_refresh := i * 2
		updated_at := 900 + i
		_, err = stmt.Exec(id, ip.String(), port, next_refresh, updated_at)
		if err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	// Add relations
	stmt, err = db.Prepare("INSERT INTO nodes_known (id, id_source, id_known, updated_at) VALUES (?,?,?,?)")
	if err != nil {
		t.Fatal(err)
	}

	// node with id=1 already knowns nodes 2-5 (1.1.1.1:1-4.4.4.4:4)
	for i := 2; i < 6; i++ {
		id := i
		id_source := 1
		id_known := i
		updated_at := 700 + i
		_, err = stmt.Exec(id, id_source, id_known, updated_at)
		if err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()

	// TEST: New neighbours
	n := &nodeDB{
		dbInfo: dbNodeInfo{id: 1},
		now:    222}
	n.tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	n.dbNeighbours = make(map[string]dbNeighbourInfo)
	n.dbNeighbours["15.15.15.15:15"] = dbNeighbourInfo{id: -1, next_refresh: 666}
	n.dbNeighbours["16.16.16.16:16"] = dbNeighbourInfo{id: -1, next_refresh: 777}

	n.dbPutNeighbours()

	type node struct {
		Id           int64
		Ip           string
		Port         int
		Next_refresh int64
		Updated_at   int64
	}
	type rel struct {
		Id_source  int64
		Id_known   int64
		Updated_at int64
	}
	got_node := make([]node, 0)
	rows, err := n.tx.Query(`SELECT id, ip, port, next_refresh, updated_at FROM nodes
		WHERE ip='15.15.15.15' OR ip='16.16.16.16'
		ORDER BY ip`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		n := node{}
		rows.Scan(&(n.Id), &(n.Ip), &(n.Port), &(n.Next_refresh), &(n.Updated_at))
		got_node = append(got_node, n)
	}

	expected_node := []node{
		node{Id: got_node[0].Id, Ip: "15.15.15.15", Port: 15,
			Next_refresh: 666, Updated_at: n.now},
		node{Id: got_node[1].Id, Ip: "16.16.16.16", Port: 16,
			Next_refresh: 777, Updated_at: n.now},
	}

	if !reflect.DeepEqual(got_node, expected_node[:]) {
		t.Error("New neighbours: nodes expected ", expected_node, " got ", got_node)
	}

	got_rel := make([]rel, 0)
	rows, err = n.tx.Query(`SELECT id_source, id_known, nodes_known.updated_at 
		FROM nodes_known 
		LEFT JOIN nodes ON nodes_known.id_known=nodes.id
		WHERE ip='15.15.15.15' OR ip='16.16.16.16'
		ORDER BY nodes.ip`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		r := rel{}
		rows.Scan(&(r.Id_source), &(r.Id_known), &(r.Updated_at))
		got_rel = append(got_rel, r)
	}

	expected_rel := []rel{
		rel{Id_source: 1, Id_known: got_node[0].Id, Updated_at: n.now},
		rel{Id_source: 1, Id_known: got_node[1].Id, Updated_at: n.now},
	}

	if !reflect.DeepEqual(got_rel, expected_rel[:]) {
		t.Error("New neighbours: rel expected ", expected_rel, " got ", got_rel)
	}
	n.tx.Rollback()

	// TEST: Existing neighbours, no relation
	n = &nodeDB{
		dbInfo: dbNodeInfo{id: 1},
		now:    222}
	n.tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	n.dbNeighbours = make(map[string]dbNeighbourInfo)
	n.dbNeighbours["5.5.5.5:5"] = dbNeighbourInfo{id: 0, next_refresh: 666}
	n.dbNeighbours["6.6.6.6:6"] = dbNeighbourInfo{id: 0, next_refresh: 777}

	n.dbPutNeighbours()

	got_node = make([]node, 0)
	rows, err = n.tx.Query(`SELECT id, ip, port, next_refresh, updated_at FROM nodes
		WHERE ip='5.5.5.5' OR ip='6.6.6.6'
		ORDER BY ip`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		n := node{}
		rows.Scan(&(n.Id), &(n.Ip), &(n.Port), &(n.Next_refresh), &(n.Updated_at))
		got_node = append(got_node, n)
	}

	expected_node = []node{
		node{Id: 6, Ip: "5.5.5.5", Port: 5,
			Next_refresh: 666, Updated_at: n.now},
		node{Id: 7, Ip: "6.6.6.6", Port: 6,
			Next_refresh: 777, Updated_at: n.now},
	}

	if !reflect.DeepEqual(got_node, expected_node[:]) {
		t.Error("Existing neighbours: nodes expected ", expected_node, " got ", got_node)
	}

	got_rel = make([]rel, 0)
	rows, err = n.tx.Query(`SELECT id_source, id_known, nodes_known.updated_at 
		FROM nodes_known 
		LEFT JOIN nodes ON nodes_known.id_known=nodes.id
		WHERE ip='5.5.5.5' OR ip='6.6.6.6'
		ORDER BY nodes.ip`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		r := rel{}
		rows.Scan(&(r.Id_source), &(r.Id_known), &(r.Updated_at))
		got_rel = append(got_rel, r)
	}

	expected_rel = []rel{
		rel{Id_source: 1, Id_known: got_node[0].Id, Updated_at: n.now},
		rel{Id_source: 1, Id_known: got_node[1].Id, Updated_at: n.now},
	}

	if !reflect.DeepEqual(got_rel, expected_rel[:]) {
		t.Error(expected_rel)
		t.Error(got_rel)
	}
	n.tx.Rollback()

	// TEST: Existing neighbours, existing relation
	n = &nodeDB{
		dbInfo: dbNodeInfo{id: 1},
		now:    222}
	n.tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	n.dbNeighbours = make(map[string]dbNeighbourInfo)
	n.dbNeighbours["2.2.2.2:2"] = dbNeighbourInfo{id: 0, next_refresh: 666}
	n.dbNeighbours["3.3.3.3:3"] = dbNeighbourInfo{id: 0, next_refresh: 777}

	n.dbPutNeighbours()

	got_node = make([]node, 0)
	rows, err = n.tx.Query(`SELECT id, ip, port, next_refresh, updated_at FROM nodes
		WHERE ip='2.2.2.2' OR ip='3.3.3.3'
		ORDER BY ip`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		n := node{}
		rows.Scan(&(n.Id), &(n.Ip), &(n.Port), &(n.Next_refresh), &(n.Updated_at))
		got_node = append(got_node, n)
	}

	expected_node = []node{
		node{Id: 3, Ip: "2.2.2.2", Port: 2,
			Next_refresh: 666, Updated_at: n.now},
		node{Id: 4, Ip: "3.3.3.3", Port: 3,
			Next_refresh: 777, Updated_at: n.now},
	}

	if !reflect.DeepEqual(got_node, expected_node[:]) {
		t.Error("Existing neighbours: nodes expected ", expected_node, " got ", got_node)
	}

	got_rel = make([]rel, 0)
	rows, err = n.tx.Query(`SELECT id_source, id_known, nodes_known.updated_at 
		FROM nodes_known 
		LEFT JOIN nodes ON nodes_known.id_known=nodes.id
		WHERE ip='2.2.2.2' OR ip='3.3.3.3'
		ORDER BY nodes.ip`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		r := rel{}
		rows.Scan(&(r.Id_source), &(r.Id_known), &(r.Updated_at))
		got_rel = append(got_rel, r)
	}

	expected_rel = []rel{
		rel{Id_source: 1, Id_known: got_node[0].Id, Updated_at: n.now},
		rel{Id_source: 1, Id_known: got_node[1].Id, Updated_at: n.now},
	}

	if !reflect.DeepEqual(got_rel, expected_rel[:]) {
		t.Error(expected_rel)
		t.Error(got_rel)
	}
	n.tx.Rollback()
}

// Get a database which is based in a file. This is used for benchmarks in case
// disk IO is the limiting factor
func tempDBBench(b *testing.B) *sql.DB {
	db, err := sql.Open("sqlite3", "/tmp/dbdb")
	if err != nil {
		b.Fatal(err)
	}

	setupDB(db)

	return db
}

func BenchmarkDbGetNeighbours(b *testing.B) {
	n := &nodeDB{
		node: &Node{
			Addresses: make([]NetAddr, 0, 400),
		},
	}
	db := tempDBBench(b)
	defer db.Close()

	stmt, err := db.Prepare("INSERT INTO nodes (ip, port, next_refresh) VALUES (?,?,?)")
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		ip := net.IPv4(byte((i+15)%256), byte((i+8)%256), byte((i)%256), byte((i+3)%256))
		port := uint16(i)
		next_refresh := i % 2

		if i%3 == 0 {
			n.node.Addresses = append(n.node.Addresses, NetAddr{IP: ip, Port: port})
		}
		_, err = stmt.Exec(ip.String(), port, next_refresh)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n.tx, err = db.Begin()
		if err != nil {
			b.Fatal(err)
		}

		n.dbGetNeighbours()

		n.tx.Rollback()
	}

	db.Exec("DELETE FROM nodes")
}
