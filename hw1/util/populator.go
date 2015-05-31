package util

import (
	//"fmt"
	"github.com/mattbaird/elastigo/lib"
	"os"
)
const (
	env_domain = "ELASTICSEARCH_PORT_9300_TCP_ADDR"
	port = "9200"
	index = "ap_dataset"
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
