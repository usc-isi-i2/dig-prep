"""
dig.indexdigdata
"""
__author__ = 'saggu'

import json
from elasticsearch import Elasticsearch
from sys import stderr

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

fileName = "bp-merged-elastic_ps.json"


def indexURL():
    try:
        with open(fileName) as f:
            lines = f.readlines()

        for line in lines:
            if line.strip() != "":
                jsonurlobj = json.loads(line.strip())
                objkey = jsonurlobj.keys()[0]
                body = jsonurlobj[objkey]
                #print body
		#print objkey
                print "indexing id: " + objkey + "\n"
                es.index(index="snapshots",doc_type="snapshot",id=objkey,body=body)
    except Exception, e:
        print >> stderr.write('ERROR: %s\n' % str(e))

if __name__ == '__main__':
    indexURL()
    print "Done!"
