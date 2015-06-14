package hw2

import (
	"github.com/k1xme/CS6200/hw1/util"
	"regexp"
	"strings"
	"strconv"
	"sort"
	"fmt"
	"bufio"
	"os"
	"encoding/gob"
	"encoding/binary"
	"bytes"
	"sync"
)

var (
	docno_pattenr = regexp.MustCompile("[^AP89-][0-9]+")
)
const (
	MAX_PART_INDEX_SIZE = 2000
	TMP_MERGED_INDEX_FILE = "merged.tmp"
)

type InvertedList map[int][]int

type Catalog struct {
	Fname string // the file path that this catalog represents to.
	TermRec map[string][]int64 // the offset and length of each term record .
}

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

func SaveIndex(name string, index map[string]InvertedList) {
    name = ".tmp/" + name
    writer := util.NewTmpWriter(name)
	
	catalog := &Catalog{Fname: name, TermRec: make(map[string][]int64)}

	defer writer.Flush()

	var offset int64 = 0

	for token, invertedList := range index {
		n := WriteIList(invertedList, writer)
		catalog.TermRec[token] = []int64{offset, n}
		offset += n
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
	if reader == nil {return nil}
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

func MergeIndex(clog1, clog2 *Catalog) *Catalog {
	var (
		mergedLog = &Catalog{
			Fname: TMP_MERGED_INDEX_FILE,
			TermRec: make(map[string][]int64)}
		offset int64
		n int64
		e error
		index1, index2 *os.File
	)
	// Check for non-exist path
	if clog1 == nil {return clog2}
	if clog2 == nil {return clog1}

	if index1, e = os.Open(clog1.Fname); e != nil {
		fmt.Println("index1 not exist")
		util.CleanTmps(index1, index2)
		return clog2
	}

	if index2, e = os.Open(clog2.Fname); e != nil {
		util.CleanTmps(index1, index2)
		return clog1
	}

	writer := util.NewTmpWriter(TMP_MERGED_INDEX_FILE)
	
	for t, pos1 := range clog1.TermRec {
		var newList InvertedList

		pos2, ok := clog2.TermRec[t]

		if !ok {
			// Get invertedlist from clog1's tmp file.
			newList = LoadInvertedList(index1, pos1[0], pos1[1])
		} else {
			list1 := LoadInvertedList(index1, pos1[0], pos1[1])
			list2 := LoadInvertedList(index2, pos2[0], pos2[1])
			newList = MergeList(list1, list2)
		}

		delete(clog2.TermRec, t)
		
		n = WriteIList(newList, writer)
		mergedLog.TermRec[t] = []int64{offset, n}
		offset += n
		n = 0
	}
	// Write the remaining.
	for term, pos := range clog2.TermRec {
		list := LoadInvertedList(index2, pos[0], pos[1])
		n = WriteIList(list, writer)
		mergedLog.TermRec[term] = []int64{offset, n}
		offset += n
		n = 0
	}

	writer.Flush()
	util.CleanTmps(index1, index2)

	return mergedLog
}

func MergeList(l1, l2 InvertedList) InvertedList {
	// Delete item from the first map while traversing.
	// Then iterate through the remaining items in the second map.
	for doc, pos := range l1 {
		l2[doc] = pos
	}

	return l2
}

func WriteIList(list InvertedList, writer *bufio.Writer) int64 {
	data, err := list.Encode()
	util.HandleError(err)
	n, err := writer.Write(data)
	util.HandleError(err)
	return int64(n)
}

func GenInvertedIndex() {
	var wg = new(sync.WaitGroup)

	LoadStopWords("/ap_data/stoplist.txt")
	
	docs := make(chan *[]*util.Doc, 2)
	index := make(map[string]InvertedList)
	total := 0
	count := 0
	
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
				wg.Add(1)
				go StoreIndex(strconv.Itoa(total), index, wg)
				count = 0
				index = make(map[string]InvertedList) // Release the memory used by map.
			}
		}
	}
	wg.Add(1)
	// Save the remaining indices
	StoreIndex(strconv.Itoa(total), index, wg)
	wg.Wait()
}

func StoreIndex(name string, index map[string]InvertedList, wg *sync.WaitGroup) {
	SaveIndex(name, index)
	wg.Done()
}

func BytesToUint(bs []byte) uint64 {
	var rst uint64 = 0
	for _, b := range bs {
		rst = (rst << 8) + uint64(b)
	}
	return rst
}

func GetIndexTerms(index map[string][]int64) []string {
	var terms []string
	for key, _ := range index {
		terms = append(terms, key)
	}
	return terms
}

func MergeAllIndices() {
	//var tmps = make(chan *Catalog)
}
