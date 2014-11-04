#!/usr/bin/env python

import sys
import hashlib
import time
import datetime
import requests
import simplejson as json
from collections import defaultdict
import csv

def load_inputs(limit=5, retries=5,pathname="/tmp/memex_large_elastic_search_inputs.tsv"):
    with open(pathname, 'r') as tsvfile:
        rdr = csv.reader(tsvfile, delimiter='\t')
        for (url,importtime,modtime,source) in rdr:
            yield (url,str(importtime),str(modtime),source)
            limit -= 1
            if limit < 0:
                break

source_dict = {
    "cityxguide.com": "cityxguide",
    "classifieds.myredbook.com": "myredbook",
    "eroticmugshots.com": "eroticmugshots",
    "escortads.xxx": "escortads",
    "escortsin.ca": "escortsinca",
    "escortsinthe.us": "escortsintheus",
    "liveescortreviews.com": "liveescortreviews",
    "massagetroll.com": "massagetroll",
    "myproviderguide.com": "myproviderguide",
    "sipsap.com": "sipsap",
    "www.cityvibe.com": "cityvibe",
    "www.myproviderguide.com": "myproviderguide",
    "www.naughtyreviews.com": "naughtyreviews",
    "www.rubads.com": "rubads"
}

def urlToSource(url):
    site = url.split('/')[2]
    if "backpage.com" in site:
        return "backpage"
    elif "craigslist" in site:
        return "craigslist"
    elif "classivox" in site:
        return "classivox"
    elif "escortphonelist" in site:
        return "escortphonelist"
    elif "escortsincollege" in site:
        return "escortsincollege"
    else:
        return source_dict.get(site, "unknown")

# def datestringToDatestamp(datestring, fmt="%Y-%m-%d %H:%M:%S"):
#     return time.strftime("%Y%m%d", time.strptime(datestring, fmt))

d = {}

def process_inputs(tuples=[], limit=sys.maxint):
    global d
    count = 0
    with open('/tmp/inputs.json','w') as f:
        for (native_url,cache_url,sha1,epoch) in tuples:
            try:
                # sans http://
                uid = hashlib.sha1(native_url[7:]).hexdigest().upper()

                # in general these might be distinct
                # cache_url = native_url
                # some doubt that this is complete?
                # datestamp = datestringToDatestamp(importtime)
                # cache_url = "https://karmadigstorage.blob.core.windows.net/arch/istr_memex_large/%s/%s" % (datestamp, native_url[7:])

                sig = sha1
                stage = "text"
                db_url = "http://karma-dig-service.cloudapp.net:55333/db/host/karma-dig-db.cloudapp.net/database/memex_large/table/ads/%s/%s/%s" % (sha1, epoch, stage)
                source = urlToSource(native_url)

                obj = {"native_url": native_url,
                       "cache_url": cache_url,
                       "db_url": db_url,
                       "sha1": sig,
                       "epoch": epoch,
                       "source": source,
                       "document_type": "page",
                       "process_stage": stage}
                count += 1
                print >> f, json.dumps({uid: obj}, sort_keys=True)
            except Exception as e:
                print >> sys.stderr, "Exception %r ignored, row %d skipped" % (e, count)
                raise
            limit -= 1
            if limit <= 0:
                break
        return count

def dump_inputs():
    global d
    with open('/tmp/inputs.json','w') as f:
        for k,v in d.iteritems():
            print >> f, json.dumps({k: v}, sort_keys=True)


def z(limit):
    process_inputs(load_inputs(limit=limit))
    dump_inputs()

process_inputs(load_inputs(limit=sys.maxint), limit=sys.maxint)
