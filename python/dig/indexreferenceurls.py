"""
dig.indexreferenceurls
"""
__author__ = 'saggu'

import json
import codecs
from elasticsearch import Elasticsearch
from sys import stderr
import sys
import re

es = Elasticsearch([{'host': '10.202.20.250', 'port': 9200}])

#fileName = "build.json"


def readJsonfromFile(filename, index, doctype):
    try:
        with codecs.open(filename,encoding='utf-8') as f:
            d = json.load(f)
            for fc in d:
                sha1 = re.findall('([A-F0-9]{40})',fc["uri"]) #find all sha from the uri
                urisha = sha1[0].upper() #there should be only one sha1 hex in the url
                print "indexing id: " + urisha
                es.index(index=index,doc_type=doctype,id=urisha,body=fc)
    except Exception, e:
        print >> stderr.write('ERROR: %s\n' % str(e))

def indexURL(filename, index, doctype):
    try:

        with open(filename) as f:
            lines = f.readlines()

        for line in lines:
            if line.strip() != "":
                jsonurlobj = json.loads(line.strip())
                objkey = jsonurlobj['uri']
                #body = jsonurlobj[objkey]
                #print body
                #print "indexing id: " + line
                #u = unicode(line, "utf-8")
		print "indexing id: " + objkey
                es.index(index=index,doc_type=doctype,body=line)
    except Exception, e:
        print >> stderr.write('ERROR: %s\n' % str(e))

if __name__ == '__main__':

    if len(sys.argv) == 5:
        if sys.argv[4] == "0": #separate id included in json for indexing, Andrew's format
            indexURL(sys.argv[1], sys.argv[2], sys.argv[3])
        elif sys.argv[4] == "1": #separate id read from URI in json for indexing, Pedro's format
            readJsonfromFile(sys.argv[1], sys.argv[2], sys.argv[3])

    else:
        print "Usage: indexreferenceurls.py <fileName> <index> <doctype> <fileType>"


    print "Done!"
