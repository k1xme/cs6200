from elasticsearch import Elasticsearch
import sys

client = Elasticsearch()


if __name__ == '__main__':
	q = sys.argv[1]
	qid = sys.argv[2]
	query = {
		"_source": False,
		"size": 200,
		"query": {
					"query_string": {
							"query": q
					}
		}
	}
	with open("%s_urls.txt" % qid, "w") as f:
		for hit in client.search(index="homework3", doc_type="VerticalSearch", body=query)["hits"]["hits"]:
			print "Saving URL %s" % hit["_id"]
			f.write("%s\n" % unicode(hit["_id"]).encode("utf-8"))

	print "Finished"
