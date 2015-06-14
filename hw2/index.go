package hw2

import (
	"github.com/k1xme/CS6200/hw1/util"
	"regexp"
	"strings"
	"strconv"
	"sort"
	"fmt"
	"os"
	"encoding/gob"
	"encoding/binary"
	"bytes"
)

var (
	docno_pattenr = regexp.MustCompile("[^AP89-][0-9]+")
)
const (
	MAX_PART_INDEX_SIZE = 1000
)

type InvertedList map[int][]int

func (l *InvertedList) Encode() ([]byte, error) {
	var buf = new(bytes.Buffer)
	// The number of items in l.
	n := len(*l)
	err := binary.Write(buf, binary.BigEndian, uint32(n))

	for key, value := range *l {
		// Write header.
		err = binary.Write(buf, binary.BigEndian, uint32(key))
		util.HandleError(err)
		err = binary.Write(buf, binary.BigEndian, uint32(len(value)))
		util.HandleError(err)

		for _, v := range value {
			err = binary.Write(buf, binary.BigEndian, uint16(v))
			util.HandleError(err)
		}
	}

	return buf.Bytes(), err
}

func Decode(l *map[int][]int, b []byte) {
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

type Catalog struct {
	Fname string // the file path that this catalog represents to.
	TermRec map[string][]int64 // the offset and length of each term record .
}

func SaveIndex(name string, index map[string]InvertedList) {
    name = ".tmp/" + name
    writer := util.NewTmpWriter(name)
	
	catalog := &Catalog{Fname: name, TermRec: make(map[string][]int64)}

	defer writer.Flush()

	var offset int64 = 0

	for token, invertedList := range index {
		data, err := invertedList.Encode()
		util.HandleError(err)
		n, err := writer.Write(data)
		util.HandleError(err)
		
		catalog.TermRec[token] = []int64{offset, int64(n)}
		offset += int64(n)
	}

	index = nil
	SaveCatalog(catalog)
}

func SaveCatalog(catalog *Catalog) {
    writer := util.NewTmpWriter(catalog.Fname+".catalog")

    encoder := gob.NewEncoder(writer)
    err := encoder.Encode(catalog)

	util.HandleError(err)
}

func GenDocID(docno string) (int, error) {
	id := strings.Join(docno_pattenr.FindAllString(docno, -1), "")
	n, e:= strconv.Atoi(id)
	util.HandleError(e)
	return n,e
}

func LoadCatalog(path string) *Catalog {
	reader := util.NewTmpReader(path)

	decoder := gob.NewDecoder(reader)
	
	catalog := new(Catalog)
	//catalog.Fname = strings.TrimSuffix(path, ".catalog")
	
	decoder.Decode(&catalog)

	return catalog
}

func LoadInvertedList(file *os.File, offset, length int64) InvertedList {
	buf := make([]byte, length)
	
	file.ReadAt(buf, offset)
	
	var l = make(map[int][]int)

	Decode(&l, buf)

	return InvertedList(l)
}

func Sort(keys interface{}) {
	switch k := keys.(type) {
		case []string:
			sort.Sort(sort.StringSlice(k))
		case []int:
			sort.Ints(k)
	}
}

func MergeIndex() {
	
}

func MergeList() {
	// Delete item from the first map while traversing.
	// Then iterate through the remaining items in the second map.
}

func GenInvertedIndex() {
	LoadStopWords("/ap_data/stoplist.txt")
	
	docs := make(chan *[]*util.Doc, 2)
	index := make(map[string]InvertedList)
	total := 0
	count := 0
	
	// Save the remaining indices
	defer StoreIndex(strconv.Itoa(total), index)
	
	go util.ParseDir("/ap_data/ap89_collection/", docs)

	for all_docs := range docs {
		for _, d := range *all_docs {
			count ++
			total ++
			
			id, _ := GenDocID(d.Docno)

			text := bytes.ToLower(d.Text)
			tokens := token_pattern.FindAllString(string(text), -1)
			tokens = RemoveStopWords(tokens)
			
			fmt.Print("\rDocs num: ", total)

			for pos, token := range tokens {
				token = Stem(token)
				_, ok := index[token]
				if ! ok {
					index[token] = InvertedList{}
				}

				index[token][id] = append(index[token][id], pos)
			}
			
			if count == MAX_PART_INDEX_SIZE {
				go StoreIndex(strconv.Itoa(total), index)
				count = 0
				index = make(map[string]InvertedList) // Release the memory used by map.
			}
		}
	}
}

func StoreIndex(name string, index map[string]InvertedList) {
	SaveIndex(name, index)
}

func BytesToUint(bs []byte) uint64 {
	var rst uint64 = 0
	for _, b := range bs {
		rst = (rst << 8) + uint64(b)
	}
	return rst
}

func WriteInt64(buf []byte, x uint64) int {
	return binary.PutUvarint(buf, x)
}
