package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

type Message struct {
	Type    string
	Payload []byte
}

// Send a version message to initiate a connection with node
func sendVersion(node Node) (err error) {
	msg := makeVersion(node)

	return sendMessage(node, msg)
}

// Receive a message which is expected to be a Version
func receiveVersion(node Node) (version MsgVersion, err error) {
	msg, err := receiveMessage(node)
	if err != nil {
		return
	}

	if msg.Type != "version" {
		return MsgVersion{}, fmt.Errorf("Expected version got %s", msg.Type)
	}

	version, err = parseVersion(msg)
	if err != nil {
		return
	}

	return
}

// Ask the node to provide us with addresses
func sendGetAddr(node Node) (err error) {
	return sendMessage(node, Message{
		Type:    "getaddr",
		Payload: []byte{},
	})
}

// Read on message from the given node. This call is blocking so
// a timeout should be put on the net.Conn in node
func receiveMessage(node Node) (msg Message, err error) {
	// Header has the following format
	//   magic     0.. 3  [4]byte  magic number
	//   command   4..15  [12]byte command contained by this message
	//   length   16..19  int32    size of payload
	//   checksum 20..23  [4]byte  checksum of the payload
	var header [24]byte

	_, err = io.ReadFull(node.Conn, header[:])
	if err != nil {
		return
	}

	// Check magic
	if !bytes.Equal(header[0:4], NETWORK_CURRENT) {
		err = fmt.Errorf("Wrong network")
		return
	}

	msg.Type = string(bytes.TrimRight(header[4:16], string(0)))
	length := binary.LittleEndian.Uint32(header[16:20])
	if length > MAX_PAYLOAD {
		err = fmt.Errorf("Message payload to big %d", length)
		return
	}

	payload := make([]byte, length)
	_, err = io.ReadFull(node.Conn, payload)
	if err != nil {
		if verbose && err.Error() == "EOF" {
			log.Printf("%v", payload)
		}
		return
	}
	msg.Payload = payload

	// check checksum
	if !bytes.Equal(header[20:], doubleSha256(payload)[:4]) {
		err = fmt.Errorf("Invalid checksum")
		return
	}

	return
}

// Send the given message to the given node
func sendMessage(node Node, msg Message) (err error) {
	var header [24]byte

	//Generate header, format:
	//   magic     0.. 3  [4]byte  magic number
	//   command   4..15  [12]byte command contained by this message
	//   length   16..19  int32    size of payload
	//   checksum 20..23  [4]byte  checksum of the payload
	copy(header[0:4], NETWORK_CURRENT)
	copy(header[4:16], msg.Type)
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(msg.Payload)))
	copy(header[20:], doubleSha256(msg.Payload)[:4])

	_, err = node.Conn.Write(header[:])
	if err != nil {
		return
	}
	_, err = node.Conn.Write(msg.Payload)
	if err != nil {
		return
	}

	return
}
