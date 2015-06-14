package hw2

import (
    stemmer "github.com/blevesearch/go-porterstemmer"
    "github.com/k1xme/CS6200/hw1/util"
    "regexp"
    "bufio"
    "os"
    "fmt"
    "bytes"
    "encoding/gob"
    "sort"
)

const (
    term_regexp = `\w+(\.?\w+)*`
    dict_path = "tokens"
)

var (
    dict = make(map[string]bool)
    token_pattern = regexp.MustCompile(term_regexp)
    vocabulary []string
    stopWords = make(map[string]bool)
)

/*
* Extract tokens from @param{string} text and put them into dict.
*/
func TokenizeText(text []byte){
    text = bytes.ToLower(text)
    tokens := token_pattern.FindAll(text, -1)
    
    for _, token := range tokens {
        strtoken := string(token)
        _, ok := stopWords[strtoken]

        if ok {continue}
        dict[Stem(strtoken)] = true
    }
}

func Stem(token string) string {
    stemmedToken := stemmer.StemString(token)
    return stemmedToken
    //return token
}

func SaveTokens() {
    save_file, e := os.Create("tokens.bytes")
    writer := bufio.NewWriter(save_file)
    
    //token_id_map := make(map[string]int)

    var keys []string
    
    if e != nil {
        panic(e)
    }

    for k := range dict {
        keys = append(keys, k)
    }
    
    sort.Strings(keys)    

    encoder := gob.NewEncoder(writer)
    err := encoder.Encode(keys)

    if err != nil{
        panic(err)
    }

    // Ending operations
    save_file.Close()
}

func LoadUniqueTokens(path string) {
    save_file, _ := os.Open(path)
    defer save_file.Close()

    dec := gob.NewDecoder(save_file)

    err := dec.Decode(&vocabulary)

    util.HandleError(err)

    fmt.Println("Load dict from ", path, len(vocabulary))
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

func GetUniqueTokens() {
    docs := make(chan *util.Doc)

    go util.ParseDir("/ap_data/ap89_collection/", docs)

    for doc := range docs {
        TokenizeText(doc.Text)
    }

    SaveTokens()
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