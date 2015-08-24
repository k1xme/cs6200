import os
import cPickle
import pyzmail
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

'''
ES index settings:

PUT hw7
{
  "settings": {
    "index" : {
      "number_of_shards" : 4,
      "number_of_replicas" : 0
    },
    "analysis": {
      "filter": {
        "custom_delimiter": {
          "type": "word_delimiter",
          "generate_number_parts": false,
          "split_on_numerics": false
        },
        "encoding_filter": {
          "type": "length",
          "min": 1,
          "max": 15
        }
      }, 
      "analyzer": {
        "index_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "char_filter": ["html_strip"],
          "filter": [
            "lowercase",
            "stop", 
            "custom_delimiter",
            "encoding_filter"
          ]
        }
      }
    }
  }
}

PUT hw7/_mapping/mail
{
  "mail": {
    "properties": {
      "content": {
        "type": "string",
        "index_analyzer": "index_analyzer",
        "search_analyzer": "index_analyzer",
        "store": true,
        "term_vector": "with_positions_offsets_payloads"
      },
      "is_spam": {
        "type": "boolean",
        "index": "not_analyzed"
      }
    }
  }
}
'''

es = Elasticsearch()
DOC_TYPE = "mail"
INDEX = "hw7"

def get_content(path):
    content = ""
    with open(path) as f:
        mail = pyzmail.PyzMessage.factory(f.read())

        if mail["subject"]: content += mail["subject"] + " "

        if not mail.is_multipart():
            content += mail.get_payload()
            return content

        for part in mail.mailparts:
            if part.type == "text/html" or part.type == "text/plain":
                content += part.get_payload()
    return content


def wrap_action(content, mail_id, is_spam):
    content = unicode(content, "latin1")
    action = {"_index": INDEX, '_type': DOC_TYPE, '_id': mail_id, '_source': {
        'content': content, "is_spam": is_spam}
    }

    return action


def gen_actions(index_path):
    os.chdir(os.path.dirname(index_path))
    index_file_path = os.path.basename(index_path)
    actions = []
    for line in open(index_file_path):
        mail_type, path = line.split()
        mail_id = path.split("/")[-1]
        print "processing", mail_id
        content = get_content(path)
        actions.append(wrap_action(content, mail_id, mail_type=="spam"))

    return actions


def pickle_actions(actions):
    with open("es_actions.pkl", "w") as f:
        pass


def main():
    actions = gen_actions("trec07p/full/index")
    success, _ = bulk(es, actions)
    print "Finished indexing"
    print "Statistics: ", success


if __name__ == '__main__':
    main()