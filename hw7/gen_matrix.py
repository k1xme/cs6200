import random
import os
import cPickle as pickle
import elasticsearch
from sklearn import tree
from numpy import array
from indexer import es, INDEX, DOC_TYPE
from collections import OrderedDict


features = ["free", "win", "porn", "click here", "pill", "naked", "viagra",
            "part-time job", "lose weight", "sex", "alert"]
SLOP = 3


def get_spams_hams(path):
    spams = []
    hams = []

    for line in open(path):
        mail_type, path = line.split()
        mail_id = path.split("/")[-1]
        if mail_type == "spam": spams.append(mail_id)
        else: hams.append(mail_id)

    return spams, hams


def assign_training_testing_sets(spams, hams):
    random.shuffle(spams)
    random.shuffle(hams)

    training_set = spams[:int(0.8*len(spams))] + hams[:int(0.8*len(hams))]
    testing_set = spams[int(0.8*len(spams)):] + hams[int(0.8*len(hams)):]

    return set(training_set), set(testing_set)

def store_sets(training_set, testing_set):
    store("training_set.pkl", training_set)
    store("testing_set.pkl", testing_set)


def store(fname, d):
    f = open(fname, "w")
    pickle.dump(d, f)


def scan(query):
    scanner = elasticsearch.helpers.scan(es,
                                         query,
                                         scroll="1m",
                                         index=INDEX,
                                         doc_type=DOC_TYPE,
                                         size=2000,
                                         _source=False,
                                         track_scores=True)
    return scanner


def gen_matrix(target_set, spams):
    # The last elem is the label of this example.
    matrix = OrderedDict()
    n = len(features)
    labels = []
    # Initialize Matrix by setting spam label for each example.
    for mail_id in target_set:
        matrix[mail_id] = [0]*n
        label = 0
        if mail_id in spams: label = 1
        labels.append(label)

    for i in range(len(features)):
        spam_term = features[i]
        query = {
                    "query": {
                        "match_phrase": {
                            "content": {
                                "query": spam_term,
                                "slop":  SLOP
                            }
                        }
                    }
                }

        scanner = scan(query)
        count = 0

        for batch_result in scanner:
            mail_id, score = batch_result["_id"], batch_result["_score"]
            
            if mail_id in matrix: matrix[mail_id][i] = score

            count += 1

        print "Spam_word:", spam_term, count

    return matrix, labels


def train(x, y):
    # Linear Regression model
    model = tree.DecisionTreeClassifier()
    model.fit(x, y)
    print "Model params:", model.get_params()
    return model

def predict(lr_model, x): return lr_model.predict(x)

def load_train_test_list():
    training_file = open("training_set.pkl")
    testing_file = open("testing_set.pkl")
    training_set = pickle.load(training_file)
    testing_set = pickle.load(testing_file)
    training_file.close()
    testing_file.close()

    return training_set, testing_set

def main():
    training_set, testing_set = set([]), set([])
    spams, hams = get_spams_hams("trec07p/full/index")

    if not os.path.isfile("testing_set.pkl") or \
            not os.path.isfile("training_set.pkl"):
        training_set, testing_set = assign_training_testing_sets(spams, hams)
        store_sets(training_set, testing_set)
    else:
        training_set, testing_set = load_train_test_list()
    
    spams, hams = set(spams), set(hams)

    print "Training size:", len(training_set)
    print "Testing size:", len(testing_set)    

    testing_set_matrix, testing_set_labels = gen_matrix(testing_set, spams)
    training_set_matrix, training_set_labels = gen_matrix(training_set, spams)

    model = train(array(training_set_matrix.values()),
                  array(training_set_labels))
    predict_result = predict(model, array(testing_set_matrix.values()))

    count = 0
    for i in range(len(predict_result)):
        if predict_result[i] == testing_set_labels[i]:
            count+=1

    print "Precision:", float(count)/len(testing_set_labels)

    for mail in testing_set_matrix.keys()[:10]:
        print "Potential spam mail:", mail

if __name__ == '__main__':
    main()