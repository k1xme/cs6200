import pymongo
from codecs import open
client = pymongo.MongoClient()
gradebook = client.hw5.gradebook

def main():
    grades = []
    with open("hw5_qrel.txt", "w", "utf-8") as qrel:
        for grade in gradebook.find():
            if (grade["qid"], grade["docid"]) in grades: continue
            qrel.write("%s\t%s\t%s\t%d\n" % (grade["qid"], grade["accessor"], grade["docid"], grade["grade"]))
            grades.append((grade["qid"], grade["docid"]))
            print grade["docid"]


if __name__ == '__main__':
    main()
