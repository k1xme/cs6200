package main

import (
	"fmt"
	"github.com/k1xme/CS6200/hw1/util"
)

func main() {
	qs, _ := util.ReadQueries()
	
	sema := util.NewSema(15)
    ioctrl := util.InitIOCtrl(25)
	
	defer close(ioctrl.Files)
	
	go util.MergeTmpFiles(ioctrl.Files)

	for i, q := range qs {
		if q == "" {continue}
		
		<- sema
		ioctrl.Wg.Add(1)

		fmt.Println("Go query", i)
		go util.Query(q, sema, ioctrl)
	}

	ioctrl.Wg.Wait()
}