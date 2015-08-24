package hw2

import (
	"github.com/k1xme/CS6200/hw1/util"
	"regexp"
	"strings"
	"strconv"
	"fmt"
	"bufio"
	"os"
	"io"
	"encoding/gob"
	"bytes"
	"path/filepath"
	"io/ioutil"
)

var docno_pattenr = regexp.MustCompile("[^AP89-][0-9]+")
const (
	MAX_PART_INDEX_SIZE = 1000
	MERGED_INDEX_FILE = "ap_index"
)

func InitGob() { gob.Register(InvertedList{}) }

func SaveIndex(name string, index map[string]InvertedList) {
    name = ".tmp/" + name
    writer := util.NewTmpWriter(name)
	
	catalog := &Catalog{Fname: name, TermRec: make(map[string][]int64)}

	defer writer.Flush()

	var offset int64 = 0

	for token, invertedList := range index {
		n := WriteIList(&invertedList, writer)
		catalog.TermRec[token] = []int64{offset, n}
		offset += n
	}

	SaveCatalog(catalog)
}

func SaveCatalog(catalog *Catalog) {
    writer := util.NewTmpWriter(catalog.Fname+".catalog")

    encoder := gob.NewEncoder(writer)
    err := encoder.Encode(catalog)

	util.HandleError(err)
}

func GenDocID(docno string) int {
	id := strings.Join(docno_pattenr.FindAllString(docno, -1), "")
	n, e:= strconv.Atoi(id)
	util.HandleError(e)
	return n
}

func LoadCatalog(path string) *Catalog {
    f, e := os.Open(path)
    defer f.Close()
    util.HandleError(e)
    
    reader := io.Reader(f)
	decoder := gob.NewDecoder(reader)
	catalog := Catalog{}
	e = decoder.Decode(&catalog)
	
	util.HandleError(e)
	
	return &catalog
}

func LoadInvertedList(file *os.File, offset, length int64) *InvertedList {
	buf := io.NewSectionReader(file, offset, length)
	l := InvertedList{}
	dec := gob.NewDecoder(buf)
	
	dec.Decode(&l)
	
	//file.Close()
	
	return &l
}

func MergeList(l1, l2 *InvertedList) *InvertedList {
	for doc, pos := range *l1 {
		(*l2)[doc] = pos
		delete(*l1, doc)
	}
	return l2
}

func WriteIList(list *InvertedList, writer *bufio.Writer) int64 {
	data := bytes.Buffer{}
	enc := gob.NewEncoder(&data)
	enc.Encode(list)
	n, err := writer.Write(data.Bytes())
	util.HandleError(err)
	list = nil
	return int64(n)
}

func GenInvertedIndex() {
	InitGob()
	LoadStopWords("/ap_data/stoplist.txt")
	
	docs := make(chan *[]*util.Doc)
	index := make(map[string]InvertedList)
	total := 0
	count := 0
	
	go util.ParseDir("/ap_data/ap89_collection/", docs)
	// go util.ParseByLine("/ap_data/ap89_collection/ap890101", docs)

	for all_docs := range docs {
		for _, d := range *all_docs {
			count ++
			total ++
			
			id := GenDocID(d.Docno)
			//fmt.Print("Processing Doc", id, d.Docno, "\r")
			text := bytes.ToLower(d.Text)
			tokens := token_pattern.FindAllString(string(text), -1)
			tokens = RemoveStopWords(tokens)

			for i, token := range tokens {
				tokens[i] = Stem(token)
			}

			for pos, token := range tokens {
				_, ok := index[token]
				if ! ok {
					index[token] = InvertedList{}
				}

				index[token][id] = append(index[token][id], pos)
			}
			
			if count == MAX_PART_INDEX_SIZE {
				SaveIndex(strconv.Itoa(total), index)
				count = 0
				index = make(map[string]InvertedList) // Release the memory used by map.
			}
		}
	}
	SaveIndex(strconv.Itoa(total), index)
}

func MergeAllIndices(tmps chan *RecItem) {
	finalCatalog := Catalog{Fname: MERGED_INDEX_FILE, TermRec: make(map[string][]int64)}
	writer := util.NewTmpWriter(finalCatalog.Fname)
	var offset int64
	var count = 0
	preItem := <- tmps
	var f, e = os.Open(preItem.Fname)
	util.HandleError(e)
	
	preList := LoadInvertedList(f, preItem.Offset, preItem.Length)

	for item := range tmps {
		count ++
		if count % 10000 == 0 {fmt.Print("\r Merged TermRec: ", count)}

		f, e = os.Open(item.Fname)
		util.HandleError(e)

		list := LoadInvertedList(f, item.Offset, item.Length)

		if preItem.Term == item.Term {
			list = MergeList(list, preList)

		} else {
			n := WriteIList(preList, writer)
			finalCatalog.TermRec[preItem.Term] = []int64{offset, n}
			offset += n
		}
		
		preItem = item
		preList = list
	}
	// Write the last records into file
	n := WriteIList(preList, writer)
	finalCatalog.TermRec[preItem.Term] = []int64{offset, n}
	offset += n
	writer.Flush()

	SaveCatalog(&finalCatalog)
}

func PopOutLog(logs []*Catalog, itemChan chan *RecItem) {
	var pq RecItems
	if logs == nil || len(logs) == 0 {
		files, _ := ListFiles(".tmp/")
		for _, f := range files {
			log := LoadCatalog(f)
			logs = append(logs, log)
		}
	}

	for _, log := range logs {
		for term, ol := range log.TermRec {
			// f := fileMap[log.Fname]
			// if f == nil {
			// 	f, e = os.Open(log.Fname)
			// 	util.HandleError(e)
			// 	fileMap[log.Fname] = f
			// }
			rec := RecItem{term, log.Fname, ol[0], ol[1]}
			pq = append(pq, &rec)
		}
	}
	
	logs = nil

	InitPQ(&pq)

	fmt.Println("Records Generated:", len(pq))

	for _, item := range pq {
		itemChan <- item
	}

	close(itemChan)
}

func ListFiles(dir string) ([]string, error){
	infos, err := ioutil.ReadDir(dir)
	var files []string

	if err != nil {
		return nil, err
	}

	for _, info := range infos {
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".catalog") {
			continue
		}
		files = append(files, filepath.Join(dir, info.Name()))
	}

	return files, nil
}