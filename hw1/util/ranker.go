package util

import (
    "math"
    "fmt"
    json "encoding/json"
    "errors"
    "strings"
    "sort"
)

const (
    index_name = "ap_dataset"
    _type = "document"

    tf_dlen_query = `
        {
          "query": {
            "function_score": {
              "query": {
                "term": {
                  "text": "%s"
                }
              },
              "functions": [
                {
                  "script_score": {
                    "lang": "groovy",
                    "script_file": "tf",
                    "params": {
                      "term": "%s",
                      "field": "text"
                    }
                  }
                }
              ],
              "boost_mode": "replace"
            }
          },
          "script_fields": {
            "dlen": {
              "script_file": "dlen"
            }
          }
        }`

    dstat_agg = `
    {
        "aggs": {
            "voc": {
                "cardinality": {
                    "field": "text"
            }
        },
            "adlen": {
                "avg": {
                    "script_file": "dlen"
                }
            }
        }
    }`

    tmp_name = "%s.%s.tmp"

)

//Global Vars.
var (
    voc_size, total_docs, avg_dlen float64

    search_args = map[string]interface{} {
        "_source" : "false",
        "size"    : "85000",
        //"search_type": "scan",
        //"scroll" : "1m",
    }
    agg_args = map[string]interface{}{
        "search_type" : "count",
    }

    models = []string{
        "okapitf", "tfidf", "bm25",
        "laplace",
        "jm",
    }
)

// The fields in customized response json struct should
// Always be exported. Otherwise the json package cannot
// decode them.
type Aggregations struct {
    Voc struct {Value float64}
    Adlen struct {Value float64}
}

type CustomFields struct { Dlen []float64 }

type DocHit struct {
   Id string
   Dlen float64
   Tf float64
}

// The final score computed by the retrival model and its DOCNO.
type DocScore struct {
    Id string
    Score float64
}

type DocSorter struct {
    DS []DocScore
    By func(s1, s2 *DocScore) bool
}

// Len is part of sort.Interface.
func (s *DocSorter) Len() int { return len(s.DS) }

// Swap is part of sort.Interface.
func (s *DocSorter) Swap(i, j int) {
    s.DS[i], s.DS[j] = s.DS[j], s.DS[i]
}

// Less is part of sort.Interface. It is implemented 
// by calling the "by" closure in the sorter.
func (s *DocSorter) Less(i, j int) bool {
    return s.By(&s.DS[i], &s.DS[j])
}


func OkapiTF(tf, dlen float64) float64 {
    // Is here the bottleneck of the program?
    return tf / (tf + 1.5 + 1.5 * (dlen / avg_dlen))
}

func TF_IDF(okapi_tf, df float64) float64 {
    return okapi_tf * math.Log(total_docs / df)
}

func OkapiBM25(tf, dlen, tfq, df float64) float64 {
    var (
        k1 = 1.2 
        k2 = 900.00
        b = 0.75
    )

    log := math.Log(total_docs + 0.5) - math.Log(df + 0.5)
    okapitf := (1 + k1)*tf  / (tf + k1 * ((1 - b) + b * (dlen / avg_dlen)))
    trail := (1 + k2)*tfq / (tfq + k2)
    return log*okapitf*trail
}

func UnigramLM_LaplaceSmoothing(tf, dlen float64) float64 {
    return math.Log(tf + 1) - math.Log(dlen + voc_size)
}

/*
* @ctf, corpus-wide term frequency.
* @all_ctf, corpus-wide tf of all query terms.
*/
func UnigramLM_JMSmoothing(tf, dlen, ctf, tdlen float64) float64 {
    jm_lam := 0.5
    return math.Log(jm_lam * (tf/dlen) + (1 - jm_lam) * (ctf - tf)/(tdlen - dlen))
}

// Set up `avg_dlen`, `voc_size` and `total_docs`.
// Then create a new Conn if `conn` is nil.
func Initialize() error {
    if conn == nil {
        conn = Connect()
    }

    rst, err := conn.Search(index_name,
        _type, agg_args, dstat_agg)

    total_docs = float64(rst.Hits.Total)
    
    avg_dlen, voc_size = ParseAgg(rst.Aggregations)

    if avg_dlen == -1 {
        return errors.New("Parsed Aggregations Failed")
    }

    return err
}

// Can this function refactored to a Goroutine func?
// Get the TF of `term` in each of the matched docs in the corpus.
func GetTFandDlen(term string) (*[]*DocHit, float64){
    //defer close(dochit_chan)

    qdata := fmt.Sprintf(tf_dlen_query, term, term)
    
    rst, err := conn.Search(
        index_name, _type, search_args, qdata)

    if err != nil { panic(err) }

    hits := rst.Hits
    all_hits := hits.Hits

    var stats []*DocHit

    for _, hit := range all_hits {
        var cfields CustomFields

        err := json.Unmarshal(*hit.Fields, &cfields)
        
        // Got Error. Push to Channel or return?
        if err != nil { panic(err) }

        tmp := &DocHit{Id: hit.Id, Tf:float64(hit.Score),
                        Dlen:cfields.Dlen[0]}

        stats = append(stats, tmp)
        //dochit_chan <- tmp
    }
    return &stats, float64(hits.Total)
}

/* 
Parse customized Aggregation result fields.
@data is also a json.RawMessage type.
*/
func ParseAgg(data json.RawMessage) (float64, float64) {
    
    var aggs Aggregations
    err := json.Unmarshal(data, &aggs)

    if err != nil {
        panic(err)
    }

    return aggs.Adlen.Value, aggs.Voc.Value
}

// Strip the meaningless words in query.
func TrimQuery(query string) (string, string) {
    stopwords := []string{
        "Document", "will", "discuss",
        "must", "report", "describe",
        "identify", "predict", "cite",
    }

    for _, st := range stopwords {
        query = strings.Replace(query, st, "", -1)
    }

    words := strings.Split(query, " ")
    
    qno := strings.TrimSuffix(words[0], ".")
    words = words[1:]
    query = strings.Join(words, " ")

    return qno, query
}

func TokenizeTerms(query string) ([]string, map[string]int) {
    var tokens []string

    tfq := make(map[string]int)
    analyze_args := map[string]interface{}{
        "text" : query,
        "analyzer": "my_english",
    }

    resp, err := conn.AnalyzeIndices(index_name, analyze_args)

    if err != nil {
        panic(err)
    }

    for _, token := range resp.Tokens {
        tokens = append(tokens, token.Name)
        tfq[token.Name] += 1
    }


    return tokens, tfq
}

/*
* Call @func Initialize() before calling this function.
*/
func Query(query string, sema chan bool, ioctrl *IOCtrl) {
    defer ioctrl.Wg.Done()

    // Store the computed score of docs for this query.
    okapi_score := make(map[string]float64)
    tfidf_score := make(map[string]float64)
    bm25_score := make(map[string]float64)
    laplace_score := make(map[string]float64)
    jm_score := make(map[string]float64)

    score_map := map[string]map[string]float64{
        "okapitf": okapi_score, "tfidf": tfidf_score,
        "bm25": bm25_score, "laplace": laplace_score,
        "jm": jm_score,
    }

    qno, qs := TrimQuery(query)
    tokens, tfq_map := TokenizeTerms(qs)
    var (
        term_rst []*[]*DocHit
        totals []float64
        tfq []float64
        ctf []float64
        tdlen float64
    )

    // Pull TF, Dlen from ES
    for _, token := range tokens {
           arr, t := GetTFandDlen(token)
           term_rst = append(term_rst, arr)
           totals = append(totals, t)
           tfq = append(tfq, float64(tfq_map[token]))
           ctf = append(ctf, SumTf(arr))
    }

    tdlen = SumDlen(&term_rst)

    unique_docs := FilterDuplicate(&term_rst)
    
    for i, t := range totals {
        dstats := term_rst[i]
        hitdoc_laplace_score := make(map[string]float64)
        hitdoc_jm_score := make(map[string]float64)

        for _, dstat := range *dstats {
            okapi := OkapiTF(dstat.Tf, dstat.Dlen)
            idf := TF_IDF(okapi, t)
            bm25 := OkapiBM25(dstat.Tf, dstat.Dlen, tfq[i], t)
            laplace := UnigramLM_LaplaceSmoothing(dstat.Tf, dstat.Dlen)
            jm := UnigramLM_JMSmoothing(dstat.Tf, dstat.Dlen, ctf[i], tdlen)

            AddTermScore(okapi_score, dstat.Id, okapi)
            AddTermScore(tfidf_score, dstat.Id, idf)
            AddTermScore(bm25_score, dstat.Id, bm25)

            // Store the score of docs that contains this term.
            AddTermScore(hitdoc_laplace_score, dstat.Id, laplace)
            AddTermScore(hitdoc_jm_score, dstat.Id, jm)
        }
        //CompensateSmoothing(jm_score, laplace_score, unique_docs, hitdoc_jm_score,
        //                    hitdoc_laplace_score, ctf[i], tdlen)
        AccumuJM(jm_score, unique_docs, hitdoc_jm_score, ctf[i], tdlen)
        AccumulateLaplace(laplace_score, unique_docs, hitdoc_laplace_score)
        
    }

    for model, score := range score_map {
        ranking := RankDocs(score)
    
        fname := fmt.Sprintf(tmp_name, model, qno)

        SaveRanking(fname, qno, ranking, ioctrl.Files)
    }

    // Tell main goroutine we are done.
    sema <- true
}

func RankDocs(docs_score map[string]float64) []DocScore{
    var scores []DocScore
    
    sort_func := func(s1, s2 *DocScore) bool{
        return s1.Score > s2.Score
    }
    for id, score := range docs_score {
        tmp := DocScore{Id: id, Score: score}
        scores = append(scores, tmp)
    }

    sorter := &DocSorter{DS: scores, By: sort_func}
    sort.Sort(sorter)
    
    return scores[:1000]
}

func AddTermScore(docs_score map[string]float64, id string, score float64) {
    docs_score[id] += score
}

func AccumulateLaplace(ls, unique_docs, hit_score map[string]float64) {
    for docno, dlen := range unique_docs {
        value, present := hit_score[docno]
        if present {
            ls[docno] += value
        } else {
            ls[docno] += UnigramLM_LaplaceSmoothing(0, dlen)
        }
    }
}

func AccumuJM(jm, unique_docs, hit_score map[string]float64, ctf, tdlen float64) {
    for docno, dlen := range unique_docs {
        value, present := hit_score[docno]

        if present {
            jm[docno] += value
        } else {
            jm[docno] += UnigramLM_JMSmoothing(0, dlen, ctf, tdlen)
        }
    }
}

func CompensateSmoothing(jm, ls, unique_docs, hitjm_score, hitlaplace_score map[string]float64, ctf, tdlen float64) {
    for docno, dlen := range unique_docs {
        jm_score, present := hitjm_score[docno]
        laplace_score, _ := hitlaplace_score[docno]

        if present {
            ls[docno] += laplace_score
            jm[docno] += jm_score
        } else {
            ls[docno] += UnigramLM_LaplaceSmoothing(0, dlen)
            jm[docno] += UnigramLM_JMSmoothing(0, dlen, ctf, tdlen)
        }
    }
}

func FilterDuplicate(terms_rst *[]*[]*DocHit) map[string]float64 {
    unique := make(map[string]float64)
    
    for _, ts := range *terms_rst{
        for _, d := range *ts {
            unique[d.Id] = d.Dlen
        } 
    }

    return unique
}

func SumTf(hits *[]*DocHit) float64 {
    var ctf float64

    for _, dh := range *hits {
        ctf += dh.Tf
    }

    return ctf
}

func SumDlen(all_hits *[]*[]*DocHit) float64 {
    var sum float64
    unique := make(map[string]float64)

    for _, t_hits := range *all_hits {
        for _, hit := range *t_hits {
            unique[hit.Id] = hit.Dlen
        }
    }

    for _, n := range unique {
        sum += n
    }

    return sum
}
/* SHOULD ADD MORE TYPES TO ELASTICGO SO THAT WE CAN FINISH THIS
func MakeTermQuery(temr string) {
    
    params := map[string]interface{}{
        "term": term,
        "field": "text"}

    script_score := map[string]interface{}{
        "lang": "groovy",
        "script_file": "tf",
        "params": params,
    }

    tf_function := map[string]interface{}{
        "script_score": script_score,
    }

    query := elastic.Query().FunctionScore("replace", ).Term("text", term)
}*/
