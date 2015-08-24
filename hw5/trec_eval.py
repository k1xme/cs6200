import sys
import math
import matplotlib.pyplot as plt
from collections import defaultdict, OrderedDict


RECALLS = (0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
# CUTOFFS = (5, 10, 15, 20, 30, 100, 200, 500, 1000)
CUTOFFS = (5, 10, 20, 50, 100)

trec_retrive_stats = {}
output_every = False

def readQrel(qrel_file):
    qrel = defaultdict(lambda: defaultdict(lambda: 0))
    num_rel = defaultdict(lambda: 0)

    for line in open(qrel_file, "r"):
        qid, accessor, docid, rel = line.split()
        rel = int(rel)
        qrel[qid][docid] = rel
        num_rel[qid] += 1 if rel > 0 else rel

    return qrel, num_rel

def readTrec(trec_file):
    trec = defaultdict(lambda: {})
    for line in open(trec_file, "r"):
        qid, _, docid, _, score, _ = line.split()
        trec[qid][docid] = float(score)
    trec = OrderedDict(sorted(trec.items()))
    return trec

def computePrecision(qrel, trec, num_rel):
    num_queries = len(trec)
    total_num_ret = 0
    total_num_rel = 0
    total_num_rel_ret = 0
    sum_prec_at_recalls = [0] * len(RECALLS)
    sum_prec_at_cutoffs = [0] * len(CUTOFFS)
    sum_f1_at_cutoffs = [0] * len(CUTOFFS)
    sum_avg_prec = 0
    sum_r_prec = 0
    sum_ndcg = 0

    for qid, trec_map in trec.iteritems():
        if num_rel[qid] == 0: continue
        
        prec_list = [0]*1001
        rec_list = [0]*1001
        rels = [0] * 1000
        num_ret = 0
        num_rel_ret = 0
        sum_prec = 0
        num_all_rel = num_rel[qid]

        trec_map =  sorted(trec_map.iteritems(), key=lambda i: i[1], reverse=True)

        for docid, score in trec_map:
            num_ret += 1
            rel = qrel[qid][docid]
            
            if rel > 0:
                sum_prec += float((1+num_rel_ret))/num_ret
                num_rel_ret += 1
                rels[num_ret-1] = rel

            prec_list[num_ret] = float(num_rel_ret)/num_ret
            rec_list[num_ret] = float(num_rel_ret)/num_all_rel

        avg_prec = sum_prec/num_all_rel
        # is it neccessary to fill out the precision list?
        final_rec = float(num_rel_ret)/num_all_rel

        for i in xrange(num_ret+1, 1001):
            prec_list[i] = float(num_rel_ret)/i
            rec_list[i] = final_rec

        prec_at_cutoffs = []
        f1_at_cutoffs = []

        for cutoff in CUTOFFS:
            prec, rec = prec_list[cutoff], rec_list[cutoff]
            prec_at_cutoffs.append(prec)
            f1 = 2*prec*rec/(prec+rec) if rec > 0 and prec > 0 else 0
            f1_at_cutoffs.append(f1)
        
        # Have to compute R-PRECISION here. Otherwise the value will be greater.
        if num_all_rel > num_ret: rp = float(num_rel_ret)/num_all_rel
        else: rp = prec_list[num_all_rel]
        
        max_prec = 0

        for i in xrange(1000, 0, -1):
            max_prec = max(max_prec, prec_list[i])
            prec_list[i] = max_prec

        prec_at_recalls = []

        i = 1
        for recall in RECALLS:
            while i < 1001 and rec_list[i] < recall: i += 1
            rec = prec_list[i] if i < 1001 else 0
            prec_at_recalls.append(rec)


        total_num_ret += num_ret
        total_num_rel += num_all_rel
        total_num_rel_ret += num_rel_ret
        for i in range(len(CUTOFFS)):
            sum_prec_at_cutoffs[i] += prec_at_cutoffs[i]
            sum_f1_at_cutoffs[i] += f1_at_cutoffs[i]
        for i in range(len(RECALLS)): sum_prec_at_recalls[i] += prec_at_recalls[i]

        sum_avg_prec += avg_prec
        sum_r_prec += rp

        # Compute nDCG.
        ndcg = dcg(rels, num_ret)/dcg(sorted(rels, reverse=True), num_ret)
        sum_ndcg += ndcg

        save_prec_at_recalls(qid, prec_at_recalls)

        if output_every:
            eval_print(int(qid), num_ret, num_all_rel, num_rel_ret, prec_at_recalls,
                avg_prec, prec_at_cutoffs, rp, f1_at_cutoffs, ndcg)

    avg_prec_at_cutoffs = [sum_prec_cutoff/num_queries for sum_prec_cutoff in sum_prec_at_cutoffs]
    avg_prec_at_recalls = [sum_prec_recall/num_queries for sum_prec_recall in sum_prec_at_recalls]
    avg_f1_at_cutoffs = [sum_f1/num_queries for sum_f1 in sum_f1_at_cutoffs]

    mean_avg_prec = sum_avg_prec/num_queries
    avg_r_prec = sum_r_prec/num_queries
    avg_ndcg = sum_ndcg/num_queries

    eval_print(num_queries, total_num_ret, total_num_rel, total_num_rel_ret, avg_prec_at_recalls,
        mean_avg_prec, avg_prec_at_cutoffs, avg_r_prec, avg_f1_at_cutoffs, avg_ndcg)

def dcg(rels, k):
    rst = rels[0]
    for i in range(1, k):
        rst += float(rels[i])/math.log(i+1)
    return rst

def eval_print(num_queries, total_num_ret, total_num_rel, total_num_rel_ret, avg_prec_at_recalls,
        mean_avg_prec, avg_prec_at_cutoffs, avg_r_prec, f1_at_cutoffs=[], ndcg=0):
    print "***********************"
    print "Queryid (Num):    %5d" % num_queries
    print "Total number of documents over all queries"
    print "    Retrieved:    %5d" % total_num_ret
    print "    Relevant:     %5d" % total_num_rel
    print "    Rel_ret:      %5d" % total_num_rel_ret
    print "Interpolated Recall - Precision Averages:"
    for i in range(len(RECALLS)):
        print "    at %.2f       %.4f" % (RECALLS[i], avg_prec_at_recalls[i])
    print "Average precision (non-interpolated) for all rel docs(averaged over queries)"
    print "                  %.4f" % mean_avg_prec
    print "########################"
    print "Precision:"
    sum_r_prec = 0
    for i in range(len(CUTOFFS)):
        print "  At    %d docs:   %.4f" % (CUTOFFS[i], avg_prec_at_cutoffs[i])
    print "R-Precision (precision after R (= num_rel for a query) docs retrieved):"
    print "    Exact:        %.4f" % avg_r_prec
    if not f1_at_cutoffs: return
    print "########################"
    print "F-1@K:"
    for i in range(len(CUTOFFS)):
        print "    at %d       %.4f" % (CUTOFFS[i], f1_at_cutoffs[i])
    print "########################"
    print "nDCG:"
    print "                  %.4f" % ndcg

def save_prec_at_recalls(qid, prec_at_recalls):
    plt.plot(RECALLS, prec_at_recalls)
    plt.ylabel("Precisions")
    plt.xlabel("Recalls")
    plt.title(qid)
    plt.savefig("%s.png" % qid)
    plt.close()

if __name__ == '__main__':
    if len(sys.argv) < 4:
        qrelFile, trecFile = sys.argv[1], sys.argv[2]
    else:
        output_every, qrelFile, trecFile = True, sys.argv[2], sys.argv[3]
    qrel, num_rel = readQrel(qrelFile)
    trec = readTrec(trecFile)
    computePrecision(qrel, trec, num_rel)

