package hw2

import (
	"testing"
	"fmt"
	"os"
)

/*
func TestTokenize(t *testing.T) {
	docs := make(chan *util.Doc)

	go util.ParseDir("/ap_data/ap89_collection/", docs)
	LoadStopWords("/ap_data/stoplist.txt")
	for doc := range docs {
		TokenizeText(doc.Text)
	}

	fmt.Println("\nAll voc:", len(dict))

	SaveTokens()

}*/
/*
func TestStemming(t *testing.T) {
	Stem("eed")
	LoadStopWords("/ap_data/stoplist.txt")
}
*/
/*
func TestGenInvertedList(t *testing.T) {
	GenInvertedIndex()
}*/

func TestLoadCatalog(t *testing.T) {
	log := LoadCatalog(".tmp/1001.catalog")
	f, _ := os.Open(".tmp/1001")
	list := LoadInvertedList(f, log.TermRec["fishi"][0], log.TermRec["fishi"][1])
	fmt.Println(list)
}


func TestDecodeIList(t *testing.T) {
	list := InvertedList{12300073: []int{1,2,3,4}, 12300173: []int{1,3,3,4}}
	ebuf, _ := list.Encode()
	var l = make(map[int][]int)
	Decode(&l, ebuf)
	fmt.Println(InvertedList(l))
}