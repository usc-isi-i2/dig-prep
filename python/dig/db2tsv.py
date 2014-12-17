import os
#import dig.pymod.util as util
#from dig.pymod.util import elapsed, genDatestamps
#from hadoop.io import Text
#from hadoop.io import SequenceFile
#import datetime
import simplejson as json
#import re
import time
#import socket
import mysql.connector

ads_southern_calif_title_query="""select concat("http://memex.zapto.org/data/page/",upper(sha1(url)),"/",unix_timestamp(modtime),"000/titletext") as cache_url, title as `text` from ads_southern_calif"""

ads_southern_calif_body_query="""select concat("http://memex.zapto.org/data/page/",upper(sha1(url)),"/",unix_timestamp(modtime),"000/bodytext") as cache_url, text as `text` from ads_southern_calif"""

ads_title_query="""select concat("http://memex.zapto.org/data/page/",upper(sha1(url)),"/",unix_timestamp(modtime),"000/titletext") as cache_url, title as `text` from ads"""

ads_body_query="""select concat("http://memex.zapto.org/data/page/",upper(sha1(url)),"/",unix_timestamp(modtime),"000/bodytext") as cache_url, text as `text` from ads"""


def db2tsv(host='karma-dig-db.cloudapp.net',
           database='memex_small',
           user='sqluser', 
           password='sqlpassword',
           query=ads_southern_calif_title_query,
           limit=5,
           outfile=None):
    if not outfile:
        outfile = '/tmp/db2tsv.tsv'
    if limit:
        query = query + " LIMIT %s" % limit
    cnx = mysql.connector.connect(user=user,
                                  password=password,
                                  host=host,
                                  database=database)
    cursor = cnx.cursor()
    cursor.execute(query)
    with open(outfile, 'w') as f:
        for (url, raw) in cursor:
            print >>f, url, "\t", json.dumps(raw)
    cnx.close()

