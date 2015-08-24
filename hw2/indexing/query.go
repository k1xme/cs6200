package hw2

import (
	"github.com/k1xme/CS6200/hw1/util"
	"fmt"
	"os"
	"sync"
	"strconv"
    "container/heap"
)

const (
	QUERIES_PATH = "/ap_data/query_desc.51-100.short.txt"
	tmp_name = "%s.%s.tmp"
)

var (
	catalog *Catalog
	indexFile *os.File
)

func Query(query string, sema chan bool, ioctrl *util.IOCtrl) {
    defer ioctrl.Wg.Done()

    // Store the computed score of docs for this query.
    okapi_score := make(map[string]float64)
    bm25_score := make(map[string]float64)
    laplace_score := make(map[string]float64)
    proximity_score := make(map[string]float64)

    score_map := map[string]map[string]float64{
        "okapitf": okapi_score,
        "bm25": bm25_score,
        "laplace": laplace_score,
        "proximity": proximity_score,
    }

    qno, qs := util.TrimQuery(query)
    tokens, tfq_map := TokenizeQuery(qs)

    var (
        term_rst []*[]*util.DocHit
        totals []float64
        tfq []float64
    )

    // Pull TF, Dlen from ES
    for _, token := range tokens {
           arr, t := GetTFandDlen(token)
           term_rst = append(term_rst, arr)
           totals = append(totals, t)
           tfq = append(tfq, float64(tfq_map[token]))
    }

    unique_docs := util.FilterDuplicate(&term_rst)
    termPosInDocs := JoinDocs(&term_rst)
    
    for docid, poss := range termPosInDocs {
        proximity_score[docid] = ComputeProximity(unique_docs[docid], VocSize, poss)
    }

    for i, t := range totals {
        dstats := term_rst[i]
        hitdoc_laplace_score := make(map[string]float64)

        for _, dstat := range *dstats {
            okapi := util.OkapiTF(dstat.Tf, dstat.Dlen, AvgDlen)
            bm25 := util.OkapiBM25(dstat.Tf, dstat.Dlen, tfq[i], t, DocNum, AvgDlen)
            laplace := util.UnigramLM_LaplaceSmoothing(dstat.Tf, dstat.Dlen, VocSize)

            util.AddTermScore(okapi_score, dstat.Id, okapi)
            util.AddTermScore(bm25_score, dstat.Id, bm25)
            // Store the score of docs that contains this term.
            util.AddTermScore(hitdoc_laplace_score, dstat.Id, laplace)
        }
        util.AccumulateLaplace(laplace_score, unique_docs, hitdoc_laplace_score, VocSize)
        
    }

    for model, score := range score_map {
        ranking := util.RankDocs(score)
    
        fname := fmt.Sprintf(tmp_name, model, qno)

        util.SaveRanking(fname, qno, ranking, ioctrl.Files)
    }

    // Tell main goroutine we are done.
    sema <- true
}
func ParseDocno(docid int) string {
	strid := strconv.Itoa(docid)

	if len(strid) == 7 {
		strid = "AP890" + strid[:3] + "-" + strid[3:]
		return strid
	}

	return "AP89" + strid[:4] + "-" + strid[4:]
}
// Can this function refactored to a Goroutine func?
// Get the TF of `term` in each of the matched docs in the corpus.
func GetTFandDlen(term string) (*[]*util.DocHit, float64){
    all_hits := LoadInvertedList(indexFile, catalog.TermRec[term][0], catalog.TermRec[term][1])
    total := len(*all_hits)

    var stats []*util.DocHit

    for docid, pos := range *all_hits {
    	docno := ParseDocno(docid)
        tmp := &util.DocHit{Id: docno, Tf:float64(len(pos)),
                        Dlen:float64(dlenMap[docid]), Pos: pos}

        stats = append(stats, tmp)
    }
    return &stats, float64(total)
}

func TokenizeQuery(query string) ([]string, map[string]int) {
    tfq := make(map[string]int)
    tokens := token_pattern.FindAllString(query, -1)
    var stemmed []string

    for _, token := range tokens {
        _, ok := stopWords[token]
        if ok {continue}

        tfq[Stem(token)] += 1
        stemmed = append(stemmed, Stem(token))
    }

    return stemmed, tfq
}

func JoinDocs(terms_rst *[]*[]*util.DocHit) map[string][][]int {
    // The mapping between doc and the positions of terms it contains.
    unique := make(map[string][][]int) 
    
    for _, ts := range *terms_rst{
        for _, d := range *ts {
            unique[d.Id] = append(unique[d.Id], d.Pos)
        } 
    }

    return unique
}

func ExecuteAllQueries(path string) {
	var err error
	catalog = LoadCatalog("ap_index.catalog")
    models   := []string{
        "okapitf", "bm25",
        "laplace", "proximity",
    }

	if catalog == nil {
		return
	} else {
		indexFile, err = os.Open(catalog.Fname)
		util.HandleError(err)
	}

	InitMeta()

	qs, _ := util.ReadQueries(path)

	sema := util.NewSema(15)
	ioctrl := util.InitIOCtrl(30)
	wait_merge := new(sync.WaitGroup)

	wait_merge.Add(1)
	
	fmt.Println("- Start...")
	
	go util.MergeTmpFiles(models, ioctrl.Files, wait_merge)

	for i, q := range qs {
		if q == "" {
			continue
		}

		<-sema
		ioctrl.Wg.Add(1)

		util.Print("-- Go query ", i+1)

		go Query(q, sema, ioctrl)
	}

	fmt.Println("\n- Executed all queries")

	ioctrl.Wg.Wait()
	close(ioctrl.Files)
	wait_merge.Wait()
	fmt.Println("- Finished merge!        ")
}

func ComputeProximity(dlen, vocsize float64, poss [][]int) float64{
    C := float64(1500)
    numTermsContain := float64(len(poss))
    window := ComputeWindow(poss)

    return (C - window) * numTermsContain / (dlen + vocsize)
}
func ComputeWindow(poss [][]int) float64 {
    var blurbs Blurbs

    for _, pos := range poss {
        bitem := BlurbItem{pos, 0}
        blurbs = append(blurbs, &bitem)
    }

    InitBlurbs(&blurbs)
    var minSpan = 1 << 31 - 1 // Initial value is MAXINT32
    var termNum = blurbs.Len()
    var finished []*BlurbItem

    for len(finished) < termNum {
        span := ComputeSpan(append(blurbs, finished...))
        if span < minSpan { minSpan = span }
        // Move the smallest position forward.
        minBlurb := heap.Pop(&blurbs).(*BlurbItem)
        // If it already reaches the end, then move it into finished.
        for minBlurb.Index == len(minBlurb.Pos) - 1 && blurbs.Len() > 0 {
            finished = append(finished, minBlurb)
            // Move the next smallest instead.
            minBlurb = heap.Pop(&blurbs).(*BlurbItem)
        }
        if minBlurb.Index == len(minBlurb.Pos) - 1 {
            finished = append(finished, minBlurb)
        } 
        
        minBlurb.Index += 1
        heap.Push(&blurbs, minBlurb)
    }
    return float64(minSpan)
}

func ComputeSpan(bs Blurbs) int {
    min := 1<<31 - 1
    max := 0

    for _, b := range bs {
        index := b.Index
        if index > len(b.Pos) - 1 {
            index = len(b.Pos) - 1
        }

        if b.Pos[index] > max {
            max = b.Pos[index]
        }

        if b.Pos[index] < min {
            min = b.Pos[index]
        }
    }
    return max - min
}