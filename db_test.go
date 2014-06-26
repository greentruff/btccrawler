package main

import (
	"fmt"
	"testing"
)

func TestMakeInsertQuery(t *testing.T) {
	cols := map[string]interface{}{
		"one": 1,
		"two": 2,
		"three": 3,
	}

	query, values := makeInsertQuery(cols)

	fmt.Println("Query: ", query)
	fmt.Println("Values: ", values)
}

func TestMakeUpdateQuery(t *testing.T) {
	cols := map[string]interface{}{
		"one": 1,
		"two": 2,
		"three": 3,
	}

	query, values := makeUpdateQuery(999, cols)

	fmt.Println("Query: ", query)
	fmt.Println("Values: ", values)
}