from elasticsearch import Elasticsearch
from codecs import open

es = Elasticsearch()
queries = ["Brezhnev Doctrine", "Sino-Soviet split", "cuban missile crisis", "poland solidarity"]
qids = [150901, 150902, 150903, 150904]

def main():
    with open("hw4_trec.txt", "w", "utf-8") as trec:
        for i in range(len(queries)):
            body = {
                "size": 200,
                "query": {
                    "query_string": {
                        "query": queries[i]
                    }
                }}
            rst = es.search(index="homework3", doc_type="VerticalSearch", body=body,
                _source=False)
            rank = 0
            for hit in rst["hits"]["hits"]:
                rank += 1
                trec.write("%d\tQ0\t%s\t%d\t%.4f\tEXP\n" % (qids[i], hit["_id"], rank, hit["_score"]))
                print "%d %s %.4f" % (qids[i], hit["_id"], hit["_score"])

if __name__ == '__main__':
    main()


