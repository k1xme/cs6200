package util

import (
	"fmt"
	"github.com/mattbaird/elastigo/lib"
	"os"
	"io/ioutil"
	"strings"
	"sync"
	"path/filepath"
)
const (
	env_domain = "ELASTICSEARCH_PORT_9200_TCP_ADDR"
	default_dir = "/ap_data/ap89_collection/"
	port = "9200"
	max_conn = 10
	retry_time = 10
	parsing_worker_num = 10
)
var (
	conn *elastigo.Conn
)

func Connect() *elastigo.Conn{
	if conn != nil {return conn}

	conn = elastigo.NewConn()
	domain := os.Getenv(env_domain)
	conn.Domain = domain
	conn.SetPort(port)

	return conn
}

func IndexDocs() {
	indexer := conn.NewBulkIndexerErrors(max_conn, retry_time)
	indexer.BulkMaxDocs = 400
	indexer.BulkMaxBuffer = 4048576
	docs := make(chan *Doc, 600)
	
	indexer.Start()
	fmt.Println("Start...")

	go ParseDir(default_dir, docs)

	for doc := range docs {
		indexer.Index("test", _type, string(doc.Docno), "", nil, doc, false)
	}
	
	indexer.Stop()

	fmt.Println("Finished....")
}

func ParseDir(dir string, docs interface{}) {
	files, err := ListFiles(dir)
	sema := NewSema(parsing_worker_num)
	wg := new(sync.WaitGroup)

	HandleError(err)

	for _, f := range files {
		<- sema 
		wg.Add(1)

		go ParseDocs(f, docs, sema, wg)
	}

	wg.Wait()
	switch d := docs.(type) {
		case chan *[]*Doc:
			close(d)
		case chan *Doc:
			close(d)
	}
}

func ListFiles(dir string) ([]string, error){
	infos, err := ioutil.ReadDir(dir)
	var files []string

	if err != nil {
		return nil, err
	}

	for _, info := range infos {
		if info.IsDir() || !strings.HasPrefix(info.Name(), "ap89") {
			continue
		}
		files = append(files, filepath.Join(dir, info.Name()))
		fmt.Print("Append ", info.Name(), "....\r")
	}

	return files, nil
}

func HandleError(err error) {
	if err == nil { return }
	panic(err)
}