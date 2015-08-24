import os
from sklearn import linear_model
from numpy import array
import sys
import cPickle as pickle
import assign_test_train
from gen_matrix import store

def readMatrix(matrix_file):
	f = open(matrix_file, "r")
	matrix = pickle.load(f)
	return matrix

def train(x, y):
	# Linear Regression model
	lr_model = linear_model.LinearRegression()
	lr_model.fit(x, y)
	print "Model params:", lr_model.get_params()
	# store(lr_model, "lr_trained_model.pkl")
	return lr_model

def predict(lr_model, x): return lr_model.predict(x)

def outputPrediction(fname, mapping, scores):
	with open(fname, "w") as f:
		for i in xrange(len(mapping)):
			qid, docid = mapping[i]
			score = scores[i]
			f.write("%s Q0 %s %d %f EXP\n" % (qid, docid, i, score))

def main():
	if len(sys.argv) < 3:
		print "Not enought arguments!"
		return
	train_set, test_set = assign_test_train.assign()

	matrix_file = sys.argv[1]
	qd_file = sys.argv[2]
	matrix = readMatrix(matrix_file)
	print "Matrix size:", len(matrix)
	query_docs = readMatrix(qd_file)
	print "query_docs size:", len(query_docs)
	samples = []
	labels =[]
	qd_map = []
	for qid in train_set:
		for docid in query_docs[str(qid)]:
			feats = matrix[(str(qid), docid)]
			if len(feats) < 6:  feats += ["0"] * (6-len(feats))
			labels.append(int(feats[0]))
			samples.append(map(lambda feat: float(feat), feats[1:]))
			qd_map.append((str(qid), docid))
	print "Size of training set:", len(samples)
	# Train the model
	lr_model = train(array(samples), array(labels))

	# Test on the training queries.
	scores = predict(lr_model, samples)
	outputPrediction("lr_training_scores.txt", qd_map, scores)

	# Test on the testing queries.
	tests = []
	qd_map = []
	for qid in test_set:
		for docid in query_docs[str(qid)]:
			feats = matrix[(str(qid), docid)]
			if len(feats) < 6:  feats += [0] * (6-len(feats))
			tests.append(map(lambda feat: float(feat), feats[1:]))
			qd_map.append((str(qid), docid))
	print "Size of testing set:", len(tests)
	scores = predict(lr_model, tests)
	outputPrediction("lr_testing_scores.txt", qd_map, scores)

if __name__ == '__main__':
	main()