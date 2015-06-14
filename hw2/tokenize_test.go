package hw2

import (
	"testing"
	"fmt"
	//"os"
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

func TestMergeIndex(t *testing.T) {
	comm := "alleg"
	log1 := LoadCatalog("")
	log := LoadCatalog(".tmp/12000.catalog")

	nlog := MergeIndex(log, log1)

	fmt.Println(nlog.TermRec[comm])
}