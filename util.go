package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// Double sha256 for calculating checksums
func doubleSha256(data []byte) []byte {
	hash := sha256.Sum256(data)
	hash = sha256.Sum256(hash[:])

	return hash[:]
}

//Read a variable length integer from the payload
//The integer has a prefix depending on it's size
// Size | Prefix
//  8   | None
//  16  | 0xfd
//  32  | 0xfe
//  64  | 0xff
// Returns:
//   val : value of the varint
//   n : number of bytes in the representation
func varInt(data []byte) (val uint64, n int, err error) {
	if len(data) < 1 {
		err = fmt.Errorf("varInt: Not enough data (%d)", len(data))
		return
	}

	switch uint8(data[0]) {
	case 0xfd:
		if len(data) < 3 {
			err = fmt.Errorf("varInt: Not enough data for uint16 (%d)", len(data))
			return
		}
		n = 3
		val = uint64(binary.LittleEndian.Uint16(data[1:3]))
	case 0xfe:
		if len(data) < 5 {
			err = fmt.Errorf("varInt: Not enough data for uint32 (%d)", len(data))
			return
		}
		n = 5
		val = uint64(binary.LittleEndian.Uint32(data[1:5]))
	case 0xff:
		if len(data) < 9 {
			err = fmt.Errorf("varInt: Not enough data for uint64 (%d)", len(data))
			return
		}
		n = 5
		val = binary.LittleEndian.Uint64(data[1:9])
	default: // No prefix
		n = 1
		val = uint64(uint8(data[0]))
	}

	return
}

// Read a var_str. This is a byte string prefixed with its length represented as
// a varInt
// Returns:
//   str: string
//   n : size in bytes of the var_str
func varStr(data []byte) (str string, n int, err error) {
	length, n, err := varInt(data)
	if err != nil {
		return
	}

	// 0 length string
	if length == 0 {
		return "", n, nil
	}

	if len(data) < n+int(length) {
		err = fmt.Errorf("varStr: Not enough data (%d)", len(data))
		return
	}

	str_data := make([]byte, length)
	copy(str_data, data[n:n+int(length)])

	return string(bytes.TrimRight(str_data, string(0))), n + int(length), nil
}
