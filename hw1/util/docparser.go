package util

import (
	"bufio"
	"os"
	"strings"
	"sync"
)

// Constants for parsing TREC docs.
var (
	DOCNO_BEGIN_TAG = "<DOCNO>"
	DOCNO_END_TAG = "</DOCNO>"
	DOC_BEGIN_TAG = "<DOC>"
	DOC_END_TAG = "</DOC>"
	TEXT_BEGIN_TAG = "<TEXT>"
	TEXT_END_TAG = "</TEXT>"
	LINE_ENDING = "\n"
	SPACE = " "
)

// Structure that stores the parsed doc information.
type Doc struct {
	Docno string `json:"docno"`
	Text string `json:"text"`
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
		text string
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
		line := lineScanner.Text()

		switch {
			case line == DOC_BEGIN_TAG:
				// We are now start to extract the info of a new doc.
				// Initialize the fields.
				docno = ""
				text = ""

			case line == TEXT_BEGIN_TAG:
				lineScanner.Scan()
				line = lineScanner.Text()
				
				// Some docs may contain more than one <TEXT> tag.
				// So we need to keep extract all these tags.
				for line != TEXT_END_TAG {
					text += line + "\n"
					lineScanner.Scan()
					line = lineScanner.Text()
				}

			case strings.HasPrefix(line, DOCNO_BEGIN_TAG):
				// Strip off the prefix and suffix to get DOCNO.
				line = strings.TrimPrefix(line, DOCNO_BEGIN_TAG + " ")
				line = strings.TrimSuffix(line, " " + DOCNO_END_TAG)
				docno = line

			case line == DOC_END_TAG:
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