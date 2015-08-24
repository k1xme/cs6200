package hw2

import (
	"testing"
	"fmt"
	"os"
	"github.com/k1xme/CS6200/hw1/util"
)

// func TestMeta(t *testing.T) {
// 	//ComputeMeta()
// 	meta := LoadMeta("corpus.meta")
// 	fmt.Println("Dlen of 0208-0293", meta.DLen[2080293])
// 	fmt.Println("Dlen of 1214-0186", meta.DLen[12140186])
// }
/*
func TestStemming(t *testing.T) {
	Stem("eed")
	LoadStopWords("/ap_data/stoplist.txt")
}
**/
/*
func TestGenInvertedList(t *testing.T) {
	GenInvertedIndex()
}
/*
func TestLoadCatalog(t *testing.T) {
	log := LoadCatalog(".tmp/1000.catalog")
	f, _ := os.Open(".tmp/1000")
	list := LoadInvertedList(f, log.TermRec["fishi"][0], log.TermRec["fishi"][1])
	fmt.Println(list)
	list = LoadInvertedList(f, log.TermRec["alleg"][0], log.TermRec["alleg"][1])
	fmt.Println(list)
}*/

// func TestMergeIndex(t *testing.T) {
// 	log1 := LoadCatalog("../.tmp/60.catalog")
// 	log := LoadCatalog("../.tmp/30.catalog")
// 	itemChan := make(chan *RecItem)
// 	logs := []*Catalog{log1, log}
// 	go PopOutLog(logs, itemChan)
// 	MergeAllIndices(itemChan)
// 	fmt.Println("list")
// }

func TestQueryTerm(t *testing.T) {
	InitGob()
	log := LoadCatalog("../ap_index.catalog")
	f, e := os.Open("../"+log.Fname)
	util.HandleError(e)
	list := LoadInvertedList(f, log.TermRec[Stem("reaction")][0], log.TermRec[Stem("reaction")][1])
	fmt.Println(len(*list))
}

// func TestEncode(t *testing.T) {
// 	//gob.Register(InvertedList{})
// 	invert := InvertedList{7030072: []int{123,267,400}, 9110002: []int{123,267,600}, 9140002: []int{1,267,600}}
// 	b := Encode(invert)
// 	list := InvertedList{}
// 	Decode(&list, b)
// 	/*
// 	index := map[string]InvertedList{"long": invert}
// 	b := bytes.Buffer{}
// 	enc := gob.NewEncoder(&b)
// 	enc.Encode(index)

// 	list := map[string]InvertedList{}
// 	dec := gob.NewDecoder(&b)
// 	dec.Decode(&list)*/
// 	fmt.Println(list)
// }