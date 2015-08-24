package hw2

import (
	"github.com/k1xme/CS6200/hw1/util"
	"fmt"
	"encoding/binary"
	"bytes"
)


type InvertedList map[int][]int

type Catalog struct {
	Fname string // the file path that this catalog represents to.
	TermRec map[string][]int64 // the offset and length of each term record .
}

func Encode(l InvertedList) []byte {
	var buf = new(bytes.Buffer)
	// The number of items in l.
	n := len(l)
	err := binary.Write(buf, binary.BigEndian, uint32(n))
	util.HandleError(err)

	for docno, pos := range l {
		// Write header.
		err = binary.Write(buf, binary.BigEndian, uint32(docno))
		util.HandleError(err)
		err = binary.Write(buf, binary.BigEndian, uint32(len(pos)))
		util.HandleError(err)

		for _, v := range pos {
			err = binary.Write(buf, binary.BigEndian, uint16(v))
			util.HandleError(err)
		}
	}
	return buf.Bytes()
}

func Decode(l *InvertedList, b []byte) {
	fmt.Println(b)
	var (
		buf = bytes.NewBuffer(b)
		barr = make([]byte, 4)
	)
	
	buf.Read(barr)

	numItems := BytesToUint(barr)

	for i := 0; i < int(numItems); i++ {
		buf.Read(barr)
		key:= BytesToUint(barr)
		
		var pos []int
		
		numPos := make([]byte, 4)
		posBuf := make([]byte, 2)

		buf.Read(numPos)
		n := BytesToUint(numPos)
		
		for i := 0; i < int(n); i++ {
			buf.Read(posBuf)
			v := BytesToUint(posBuf)
			pos = append(pos, int(v))
		}

		(*l)[int(key)] = pos
	}
}

func BytesToUint(bs []byte) uint64 {
	var rst uint64 = 0
	for _, b := range bs {
		rst = (rst << 8) + uint64(b)
	}
	return rst
}