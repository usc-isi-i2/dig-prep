"""
dig.indexreferenceurls
"""
__author__ = 'saggu'

import json
from elasticsearch import Elasticsearch
from sys import stderr
import sys
import re

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

#fileName = "build.json"


def readJsonfromFile(fileName):
    with open(fileName) as f:
        d = json.load(f)
        for fc in d:
            sha1 = re.findall('([a-f0-9]{40})',fc["uri"]) #find all sha from the uri
            urisha = sha1[0] #there should be only one sha1 hex in the url
            print "indexing id: " + urisha
            es.index(index="istrads",doc_type="istrad",id=urisha,body=fc)


def indexURL(fileName):
    try:

        with open(fileName) as f:
            lines = f.readlines()

        for line in lines:
            if line.strip() != "":
                jsonurlobj = json.loads(line.strip())
                objkey = jsonurlobj.keys()[0]
                body = jsonurlobj[objkey]
                #print body
                print "indexing id: " + objkey + "\n"
                es.index(index="pages",doc_type="page",id=objkey,body=body)
    except Exception, e:
        print >> stderr.write('ERROR: %s\n' % str(e))

if __name__ == '__main__':

    if len(sys.argv) > 1:
   #     indexURL(sys.argv[1])
        readJsonfromFile(sys.argv[1])
        #indexURL(f)
    else:
        print "Input file name"


    print "Done!"
