import os
import sys
import re
import cPickle as pickle

def readQREL(qrel_file):
	sparse_matrix = {}
	query_docs = {}
	count = 0
	with open(qrel_file, "r") as qrel:
		for line in qrel:
			qid, _, docid, label = line.split()
			count += 1
			sparse_matrix[(qid,docid)] = [label]
			if qid not in query_docs: query_docs[qid] = []
			query_docs[qid].append(docid)

	print "Got total", count
	store(query_docs, "query_docs.pkl")
	return sparse_matrix

def readFeature(feat_file, sparse_matrix):
	with open(feat_file, "r") as feat:
		for line in feat:
			qid, _, docid, _, value, _ = line.split()
			if (qid,docid) in sparse_matrix:
				sparse_matrix[(qid,docid)].append(value)

def storeMatrix(sparse_matrix):
	store(sparse_matrix, "feat_matrix.pkl")

def store(d, fname):
	f = open(fname, "w")
	pickle.dump(d, f)

def main():
	if len(sys.argv) < 3:
		print "Not enough arguments!"
		return
	feat_pattern = re.compile("_ranking.txt")
	qrel_file = sys.argv[1]
	feat_files = sys.argv[2:]
	feat_list = []
	sparse_matrix = readQREL(qrel_file)
	for feat_file in feat_files:
		print "Loading feature file", feat_file
		readFeature(feat_file, sparse_matrix)
		feat_list.append(feat_pattern.split(feat_file.split("/")[-1])[0])

	storeMatrix(sparse_matrix)

	with open("feat_list.txt", "w") as f:
		for i in range(len(feat_list)):
			f.write("%d\t%s\n" % (i, feat_list[i]))


if __name__ == '__main__':
	# [rel, bm25, jm, laplace, okapitf, tfidf]
	main()