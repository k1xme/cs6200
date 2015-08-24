import random

queries = [85, 59, 56, 71, 64, 62, 93, 99,
		   58, 77, 54, 87, 94, 100, 89, 61,
		   95, 68, 57, 97, 98, 60, 80, 63,91]
test, train = [], []

def assign():
	random.shuffle(queries)
	train = queries[:20]
	test = queries[20:]
	print "Train set:", train
	print "Test set:", test

	with open("trian_list.txt", "w") as f:
		for i in range(20): f.write("%d\t%d\n" % (i, train[i]))

	with open("test_list.txt", "w") as f:
		for i in range(5): f.write("%d\t%d\n" % (i, test[i]))

	return train, test

if __name__ == '__main__':
	assign()