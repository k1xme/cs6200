from collections import defaultdict
from math import sqrt
import sys
import memory_profiler
import elasticsearch


es = elasticsearch.Elasticsearch()
root = set([])
base = set([])
baseOutlinks = defaultdict(list)
baseInlinks = defaultdict(list)
inlinks = defaultdict(list) # dict of URLs with all the URLs pointing to them.
outlinks = defaultdict(list) #dict of URLs and their outgoing urls.


def create_root():
    # result = es.search(index="homework3", doc_type="VerticalSearch", body=query, size=1000, _source_include=["out_links"])
    # for hit in result["hits"]["hits"]:
    #     base.add(hit["_id"])
    #     baseOutlinks[hit["_id"]] = hit["out_links"]
    #     baseInlinks[hit["_id"]] = inlinks[hit["_id"]]
    for line in open("toplinks.txt", "r"):
        url = line.split()[-1]
        root.add(url)
        baseOutlinks[url] = outlinks[url]
        baseInlinks[url] = inlinks[url]

    for r in root: base.add(r)


def expand_root():
    # res = es.mget(index="homework3", doc_type="VerticalSearch", body={"ids": list(expand)})
    # outlinks = {}
    # for hit in res["hits"]["hits"]: outlinks[hit["_id"]] = hit["out_links"]
    expand = set([])

    for url in base:
        for link in baseInlinks[url][:50]:
            if link not in base: expand.add(link)

        for link in baseOutlinks[url]:
            if link not in base: expand.add(link)

    for link in expand:
        base.add(link)
        baseOutlinks[link] = outlinks[link]
        baseInlinks[link] = inlinks[link]

    print "Base created", len(base)


def is_converge(new, old):
    stddiv = 0
    for url in new:
        stddiv += (new[url] - old[url])**2

    stddiv = sqrt(stddiv / len(new))

    return stddiv < 10**-5


def compute_HITS():
    auths = defaultdict(lambda:1.0)
    hubs = defaultdict(lambda:1.0)
    nextAuths = None
    nextHubs = None

    for url in base:
        auths[url] = 1.0
        hubs[url] = 1.0

    step = 1
    while True:
        print step
        norm = 0
        nextAuths = defaultdict(lambda:0)
        nextHubs = defaultdict(lambda:0)


        for url in root:
            for link in baseInlinks[url]:
                nextAuths[url] += hubs[link]
            norm += nextAuths[url]**2

        norm = sqrt(norm)

        for url, auth in nextAuths.iteritems():
            nextAuths[url] = auth/norm

        norm = 0

        for url in base:
            for link in baseOutlinks[url]:
                nextHubs[url] += nextAuths[link]
            norm += nextHubs[url]**2

        norm = sqrt(norm)
        for url, hub in nextHubs.iteritems():
            nextHubs[url] = hub/norm

        if is_converge(nextAuths, auths) and is_converge(nextHubs, hubs):
            return nextAuths, nextHubs
        step += 1
        auths, hubs = nextAuths, nextHubs

def readFile():
    global inlinks
    global outlinks
    with open(sys.argv[1], 'r') as lines:
        for line in lines:
            links = line.split(" http://")
            node = links.pop(0)
            links = map(lambda url: "http://"+url, links)
            inlinks[node] = links

            for link in links: outlinks[link].append(node)

    return True


def save_top500(ranking, score_name):
    with open("top500_%s.txt" % score_name, "w") as f:
        for key, value in sorted(ranking.iteritems(), key=lambda (k,v): (v,k), reverse=True)[:500]:
            f.write("%s %f\n" % (key, value))

def main():
    global inlinks
    global outlinks
    readFile()
    create_root()
    expand_root()
    del inlinks
    del outlinks
    print "starting computation"
    auths, hubs = compute_HITS()
    print "saving scores"
    save_top500(auths, "auths")
    save_top500(hubs, "hubs")
    print "Finished"

if __name__ == '__main__':
    main()

