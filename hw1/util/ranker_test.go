package util

import (
	//"testing"
	//"fmt"
)

/*
func TestInitialize(t *testing.T) {
	Initialize()
	if voc_size == 0 || total_docs == 0{
		t.Error("Initialization Failed")
	}
}

func TestTokenize(t *testing.T) {
	ts := TokenizeTerms(
		`allegations, or measures being taken against,
		corrupt public officials of any governmental 
		jurisdiction worldwide.`)
	if ts == nil {
		t.Error("TokenizeTerms Failed")
	} else {
		fmt.Println("[Log] Tokenized Terms:", ts)
	}
}*/
/*
func TestGetTFandDlen(t *testing.T) {
	dochit_chan := make(chan *DocHit, 1000)
	total_chan := make(chan int)

	Connect()

	go GetTFandDlen("alleg", dochit_chan, total_chan)

}*/
/*
func TestTrimQuery(t *testing.T) {
	fmt.Println(TrimQuery("85.   Document will discuss allegations, or measures being taken against, corrupt public officials of any governmental jurisdiction worldwide."))
	fmt.Println(TrimQuery("54.   Document will cite the signing of a contract or preliminary agreement, or the making of a tentative reservation, to launch a commercial satellite.   "))
	fmt.Println(TrimQuery("59.   Document will report a type of weather event which has directly caused at least one fatality in some location. "))
}*/
/*
func TestRankDocs(t *testing.T) {
	qs, _ := ReadQueries()
	
	sema := NewSema(15)
    ioctrl := InitIOCtrl(25)
	
	//go RecordTmpFiles(ioctrl.Files)

	go MergeTmpFiles(ioctrl.Files)

	for i, q := range qs {
		if q == "" {continue}
		
		<- sema
		ioctrl.Wg.Add(1)

		fmt.Println("Go query", i)
		go Query(q, sema, ioctrl)
	}

	ioctrl.Wg.Wait()

	close(ioctrl.Files)
}

func TestMerge(t *testing.T) {
	MergeTmp("tmplist.txt")
}*/