package util

import (
	"testing"
	"fmt"
)

func TestConnect(t *testing.T) {
	Connect()

	r, e := conn.AllNodesInfo()

	fmt.Println(r, e)
}
