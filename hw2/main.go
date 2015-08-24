package main

import (
	indexing "github.com/k1xme/CS6200/hw2/indexing"	
)

func main() {
	// var logs []*indexing.Catalog
	// itemChan := make(chan *indexing.RecItem)
	
	indexing.GenInvertedIndex()
	// go indexing.PopOutLog(logs, itemChan)

	// indexing.MergeAllIndices(itemChan)
	// indexing.ExecuteAllQueries(indexing.QUERIES_PATH)
}