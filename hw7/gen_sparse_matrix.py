import sklearn
import elasticsearch
import os
import cPickle as pickle
from indexer import es, INDEX, DOC_TYPE
from gen_matrix import get_spams_hams, load_train_test_list
from collections import OrderedDict
from codecs import open


FEATURE_LIST_PATH = "feature_list.txt"


def load_training_testing_sets():
    training_file = open("training_set.pkl")
    testing_file = open("testing_set.pkl")
    training_set = pickle.load(training_file)
    testing_set = pickle.load(testing_file)
    training_file.close()
    testing_file.close()

    return training_set, testing_set


def gen_sparse_matrix(set_name,target_set, features, spams):
    sparse_matrix_file = open(set_name, "w")
    ids_list_file = open("%s_ids.txt" % set_name, "w")

    n = len(target_set)
    target_set = list(target_set)
    pre = 0
    i = 1000
    line_count = 0
    
    while i <= n:
        body = {
                  "ids": target_set[pre:i],
                  "parameters": {
                    "field_statistics": False,
                    "positions": False,
                    "offsets": False,
                    "term_statistics": False
                  }
                }
        docs = es.mtermvectors(INDEX, DOC_TYPE, body)
        for doc in docs["docs"]:
            if not doc["term_vectors"]: continue
            terms = doc["term_vectors"]["content"]["terms"]
            label = "0"
            mail_id = doc["_id"]
            if mail_id in spams: label = "1"
            sparse_matrix_file.write(label)
            cols = []
            
            print "Processing doc", line_count, mail_id
            
            for term, stat in terms.iteritems():
                if term not in features: continue
                cols.append((features[term], stat["term_freq"]))

            cols.sort()

            for col in cols:
                sparse_matrix_file.write(" %d:%d" % (col[0], col[1]))

            sparse_matrix_file.write("\n")
            ids_list_file.write("%d\t%s\n" % (line_count, mail_id))
            line_count += 1

        if i == n: break
        pre = i
        i = min(n, i+1000)

    sparse_matrix_file.close()
    ids_list_file.close()


def get_all_features():
    features = OrderedDict()

    query = {
              "aggs": {
                "features": {
                  "terms": {
                    "field": "content",
                    "size": 0,
                    "min_doc_count": 15
                  }
                }
              }
            }

    aggs = es.search(INDEX, DOC_TYPE, body=query, search_type="count")
    buckets = aggs["aggregations"]["features"]["buckets"]

    count = 1
    for bucket in buckets:
        features[bucket["key"]] = count
        count += 1

    return features

def load_feature_list():
    features = OrderedDict()
    for line in open("feature_list.txt", "r", "utf8"):
        key, index = line.split()
        features[key] = int(index)
    return features

def output_feature_list(features):
    with open("feature_list.txt", "w", "utf8") as f:
        for key, value in features.iteritems():
            f.write(u"%s\t%d\n" % (key, value))


def main():
    spams, _ = get_spams_hams("trec07p/full/index")

    if not os.path.isfile(FEATURE_LIST_PATH):
        features = get_all_features()
        output_feature_list(features)
    else:
        features = load_feature_list()

    load_features_log(features)
    # training_set, testing_set = load_train_test_list()
    # gen_sparse_matrix("train", training_set, features, spams)
    # gen_sparse_matrix("test", testing_set, features, spams)

def load_features_log(features):
    f = open("linear.model")

    lines = f.readlines()[6:]
    terms = features.keys()
    spam_words = []
    for i in xrange(len(lines)):
        spam_words.append((float(lines[i]), terms[i]))

    f.close()
    spam_words.sort(key=lambda x: x[0], reverse=True)

    for word in spam_words[:100]:
        print word
    



if __name__ == '__main__':
    main()

