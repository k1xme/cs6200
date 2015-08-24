package main

import (
	"fmt"
	"github.com/k1xme/CS6200/hw1/util"
	"sync"
	"time"
)

func main() {
	qs, _ := util.ReadQueries("/ap_data/query_desc.51-100.short.txt")

	sema := util.NewSema(15)
	ioctrl := util.InitIOCtrl(30)
	wait_merge := new(sync.WaitGroup)

	util.HandleError(util.Initialize())

	wait_merge.Add(1)
	
	fmt.Println("- Start...")
	
	go util.MergeTmpFiles(ioctrl.Files, wait_merge)

	for i, q := range qs {
		if q == "" {
			continue
		}

		<-sema
		ioctrl.Wg.Add(1)

		util.Print("-- Go query ", i+1)

		go util.Query(q, sema, ioctrl)
	}

	fmt.Println("\n- Executed all queries")

	ioctrl.Wg.Wait()
	close(ioctrl.Files)
	wait_merge.Wait()
	fmt.Println("- Finished merge!        ")
}

func timeTrack(start time.Time) {
    elapsed := time.Since(start)

    fmt.Printf("- Runtime: %f seconds \n", elapsed.Seconds())
}