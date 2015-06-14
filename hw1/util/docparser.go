package util

import (
	"bufio"
	"os"
	strings "bytes"
	"sync"
//	"fmt"
)

// Constants for parsing TREC docs.
var (
	DOCNO_BEGIN_TAG = []byte("<DOCNO>")
	DOCNO_END_TAG = []byte("</DOCNO>")
	DOC_BEGIN_TAG = []byte("<DOC>")
	DOC_END_TAG = []byte("</DOC>")
	TEXT_BEGIN_TAG = []byte("<TEXT>")
	TEXT_END_TAG = []byte("</TEXT>")
	LINE_ENDING = []byte("\n")
	SPACE = []byte(" ")
)

// Structure that stores the parsed doc information.
type Doc struct {
	Docno string `json:"docno"`
	Text []byte `json:"text"`
}

// Parses the file in `path`. 
// This function supports Goroutine.
func ParseDocs(path string, docs interface{}, sema chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	
	ParseByLine(path, docs)
	// Tell main goroutine we are done, you can proceed to the next waiting file.
	sema <- true
}

func ParseByLine(path string, docs interface{}) {
	var (
		docno string
		text []byte
		docsList []*Doc
	)

	file, e := os.Open(path)

	if e != nil {
		panic(e)
	}
	
	// Close file after the execution finishes.
	defer file.Close()

	var lineScanner = NewLineScanner(file)

	for lineScanner.Scan() {
		line := lineScanner.Bytes()

		switch {
			case strings.Equal(line, DOC_BEGIN_TAG):
				// We are now start to extract the info of a new doc.
				// Initialize the fields.
				docno = ""
				text = nil

			case strings.Equal(line, TEXT_BEGIN_TAG):
				lineScanner.Scan()
				line = lineScanner.Bytes()
				
				// Some docs may contain more than one <TEXT> tag.
				// So we need to keep extract all these tags.
				for !strings.Equal(line, TEXT_END_TAG) {
					line = append(line, LINE_ENDING...)
					text = append(text, line...)
					lineScanner.Scan()
					line = lineScanner.Bytes()
				}

			case strings.HasPrefix(line, DOCNO_BEGIN_TAG):
				// Strip off the prefix and suffix to get DOCNO.
				line = strings.TrimPrefix(line, append(DOCNO_BEGIN_TAG, SPACE...))
				line = strings.TrimSuffix(line, append(SPACE, DOCNO_END_TAG...))
				docno = string(line)

			case strings.Equal(line, DOC_END_TAG):
				// This means we have fully parsed a Doc.
				// So append it into the result array.
				tmp_doc := &Doc{docno, text}

				switch d := docs.(type) {
					case chan *[]*Doc:
						// Change the value of where pointer `d` points to.
						// NOTE: only by doing so gurantees the calling
						//   funtion can see the modification.
						//*d = append(*docs.(*[]*Doc), tmp_doc)
						docsList = append(docsList, tmp_doc)

					case chan *Doc:
						// If d is a Channel, just push the parsed doc to it.
						d <- tmp_doc
				}
		}
	}
	switch d := docs.(type) {
		case chan *[]*Doc:
			d <- &docsList
	}
}

func NewLineScanner(file *os.File) *bufio.Scanner {
	var lineScanner = bufio.NewScanner(file)
	// Set the scanner to line-scanning mode.
	lineScanner.Split(bufio.ScanLines)
	return lineScanner
}