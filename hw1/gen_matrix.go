package main

import (
    "bufio"
    "strings"
    "os"
)

var (
    dataset := make(map[string]int)
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func initializeLineScanner(fname string) bufio.Scanner {
    f, err := os.Open(fname)
    check(err)
    scanner := bufio.NewScanner(f)
    scanner.Split(bufio.ScanLines)
    return scanner
}
func initializeDataset(fname string) {
    // Read QREL file to get avaiable QID-DOCID
    scanner := initializeLineScanner(fname)
    for scanner.Scan() {
        qid, _, docid, rel := strings.SplitAfter(scanner.Text(), " ")
        
    }
}
func readRanking(fname string) {
    scanner := initializeLineScanner(fname)

    for scanner.Scan() {
        qid, _, docid, _, score, _ := strings.SplitAfter(scanner.Text(), " ")

    }
}