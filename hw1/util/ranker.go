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
                "match": {
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

    models = []string{"okapitf", "tfidf"}
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

func OkapiBM25(tf, dlen, tfq float64) {
    
}

func UnigramLM_LaplaceSmoothing() {
}

func UnigramLM_JMSmoothing() {   
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
        fmt.Println("error:", err)
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
        fmt.Println(err)
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

    score_map := map[string]map[string]float64{
        "okapitf": okapi_score, "tfidf": tfidf_score,
    }

    qno, qs := TrimQuery(query)
    tokens, tfq_map := TokenizeTerms(qs)
    var term_rst []*[]*DocHit
    var totals []float64
    var tfq []float64

    for _, token := range tokens {
           arr, t := GetTFandDlen(token)
           term_rst = append(term_rst, arr)
           totals = append(totals, t)
           tfq = append(tfq, float64(tfq_map[token]))
    }

    for i, t := range totals {
        dstats := term_rst[i]

        for _, dstat := range *dstats {
            okapi := OkapiTF(dstat.Tf, dstat.Dlen)
            idf := TF_IDF(okapi, t)
            //bm25 := OkapiBM25(dstat.Tf, dstat.Dlen, tfq[i])
            //Laplace := UnigramLM_LaplaceSmoothing()

            AddTermScore(okapi_score, dstat.Id, okapi)
            AddTermScore(tfidf_score, dstat.Id, idf)
            //AddTermScore(itf_score, dstat.Id, itf)
            //AddTermScore(itf_score, dstat.Id, itf)
            //AddTermScore(itf_score, dstat.Id, itf)
        }
    }

    // clear array.
    term_rst = nil

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

func NewSema(size int) chan bool {
    sema := make(chan bool, size)

    // init the sema by @size.
    for i := 0; i < size; i++ {
        sema <- true
    }

    return  sema   
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
