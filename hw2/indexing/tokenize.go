package hw2

import (
    "github.com/dchest/stemmer/porter2"
    "github.com/k1xme/CS6200/hw1/util"
    "regexp"
    "bufio"
    "os"
    "io"
    "fmt"
    "bytes"
    "encoding/gob"
)

const (
    term_regexp = `\w+(\.?\w+)*`
    dict_path = "tokens"
    STOPWORD_PATH = "/ap_data/stoplist.txt"
    COLLECTION_PATH = "/ap_data/ap89_collection/"
    META_PATH = "corpus.meta"
)

var (
    dict = make(map[string]int)
    dlenMap = make(map[int]int)
    token_pattern = regexp.MustCompile(term_regexp)
    stopWords = make(map[string]bool)
    stemmer = porter2.Stemmer
    AvgDlen float64
    DocNum float64
    VocSize float64
)

type CorpusMeta struct {
    Tokens map[string]int
    DLen map[int]int
}

/*
* Extract tokens from @param{string} text and put them into dict.
*/
func TokenizeText(text []byte) int {
    text = bytes.ToLower(text)
    tokens := token_pattern.FindAll(text, -1)
    // var stemmedTokens []string
    var dlen int

    for _, token := range tokens {
        strtoken := string(token)
        //_, ok := stopWords[strtoken]
        //if ok {continue}
        dict[strtoken] += 1
        // stemmedTokens = append(stemmedTokens, Stem(strtoken))
        dlen += 1
    }
    return dlen
}

func Stem(token string) string {
    stemmedToken := stemmer.Stem(token)
    return stemmedToken
    //return token
}

func SaveMeta() {
    save_file, e := os.Create(META_PATH)
    writer := bufio.NewWriter(save_file)  

    meta := CorpusMeta{Tokens: dict, DLen: dlenMap}
    encoder := gob.NewEncoder(writer)
    e = encoder.Encode(&meta)

    if e != nil{
        panic(e)
    }

    // Ending operations
    save_file.Close()
}

func LoadStopWords(path string) {
    swfile, e := os.Open(path)
    defer swfile.Close()

    util.HandleError(e)

    scanner := util.NewLineScanner(swfile)

    for scanner.Scan() {
        word := scanner.Text()
        stopWords[word] = true
    }
}

func LoadMeta(path string) {
    f, e := os.Open(path)
    defer f.Close()
    util.HandleError(e)
    
    reader := io.Reader(f)
    decoder := gob.NewDecoder(reader)
    meta := CorpusMeta{}
    e = decoder.Decode(&meta)
    
    util.HandleError(e)
    
    dlenMap = meta.DLen
    fmt.Println("Loaded Document Length Map")
    dict = meta.Tokens
    fmt.Println("Loaded Vocabulary and CTF")
}

func ComputeMeta() {
    LoadStopWords(STOPWORD_PATH)

    gob.Register(CorpusMeta{})

    docs := make(chan *util.Doc)

    go util.ParseDir(COLLECTION_PATH, docs)
    count := 0
    for doc := range docs {
        count ++
        fmt.Print("\rProgress:", count)

        id := GenDocID(doc.Docno)
        dlen := TokenizeText(doc.Text)
        dlenMap[id] = dlen
    }
    fmt.Println("Voc Size:", len(dict))
    fmt.Println("Docs Size:", len(dlenMap))
    SaveMeta()
}

func ComputeAvgLen() {
    sum := 0
    for _, dlen := range dlenMap {
        sum += dlen
    }
    AvgDlen = float64(sum)/float64(len(dlenMap))
}
func RemoveStopWords(tokens []string) []string {
    var new_tokens []string

    for _, token := range tokens {
        _, ok := stopWords[token]

        if ok { continue }
        new_tokens = append(new_tokens, token)
    }
    return new_tokens
}

func InitMeta() {
    LoadStopWords(STOPWORD_PATH)
    LoadMeta(META_PATH)
    ComputeAvgLen()
    DocNum = float64(len(dlenMap))
    VocSize = float64(len(dict))
}