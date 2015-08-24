import math, sys

t = .85

def calculatePagerank(urls, inlinks, outlinks):
    n = len(urls)
    sinks = []
    for url in urls:
        if url not in outlinks:
            sinks.append(url)
    print "SINK COUNT ", len(sinks)
    #initial pagerank is evenly split
    pageranks = {}
    for url in urls:
        pageranks[url] = 1.0/n

    prevPerplexity = 0
    currentPerplexity = perplex(pageranks)

    while not isconverged(prevPerplexity, currentPerplexity):
        print(currentPerplexity)
        newpagerank = {}
        sinkPR = 0

        for sinkUrl in sinks:
            sinkPR += pageranks[sinkUrl]

        for url in pageranks.keys():

            newpagerank[url] = (1 - t) / n
            newpagerank[url] += t * sinkPR / n
            if url not in inlinks: continue
            for inlink in inlinks[url]:
                outlinkCount = outlinks[inlink]
                PR = pageranks[inlink]
                newpagerank[url] += t * PR / outlinkCount

        pageranks = newpagerank
        prevPerplexity = currentPerplexity
        currentPerplexity = perplex(pageranks)

    return pageranks

def isconverged(prevPerplexity, currentPerplexity):
    r1 = round(prevPerplexity, 2)
    r2 = round(currentPerplexity, 2)
    return r1 == r2

def perplex(pagerank):
    return pow(2, shannonEntropy(pagerank))

def shannonEntropy(pagerank):
    s = 0
    for key in pagerank:
        p = pagerank[key]
        s += p * math.log(p, 2)
    return -1 * s

def readFile():
    count = 0 # total num URLs in collection
    inlinks = {} # dict of URLs with all the URLs pointing to them.
    outlinks = {} #dict of URLs and their outgoing urls.
    urls = set([]) #list of all URLs in this collection.

    for line in open(sys.argv[1], 'r'):
        line = line.strip() #remove newlines
        if "http://" in line:
            links = line.split(" http://") # Some urls may contain " " in it.
        else:
            links = line.split(" ")

        node = links.pop(0)

        if "http://" in line:
            links = map(lambda url: "http://"+url, links)

        inlinks[node] = links
        urls.add(node)
        for u in links: urls.add(u)

        for link in links:
            if link in outlinks:
                outlinks[link] += 1
            else:
                outlinks[link] = 1

    return urls, inlinks, outlinks


def save_top500(pageranks):
    with open("top500_PR_wt.txt", "w") as f:
        for key, value in sorted(pageranks.iteritems(), key=lambda (k,v): (v,k), reverse=True)[:500]:
            f.write("%s %f\n" %(key, value))

    print "Finished"

if __name__ == '__main__':
    urls, inlinks, outlinks = readFile()
    print "Total Urls:", len(urls)
    pagerank = calculatePagerank(urls, inlinks, outlinks)
    save_top500(pagerank)