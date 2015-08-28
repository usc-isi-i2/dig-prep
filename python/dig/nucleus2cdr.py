#!/usr/bin/python

import sys, os, codecs, glob
import os.path, time, calendar
import json

infolder = os.path.realpath(sys.argv[1])
# inpath = os.path.realpath(sys.argv[1])

## Desired URLs are something like
## ^http[s]?://[^/]*agorahooawayyfoe.onion/p\.*

## our paths look like /dnmarchives/agora/2015-07-07/cat/XpMZAakr4w
## out paths look like /dnmarchives/agora/2015-07-07/p/XpMZAakr4w
## we will transform this to https://agorahooawayyfoe.onion/p/XpMZAakr4w

def inpathToUrl(path):
    root = "https://agorahooawayyfoe.onion/p/"
    return os.path.join(root, os.path.basename(path))

def inpathToEpoch(path, fromPathname=True):
    if fromPathname:
        # Use filename
        # /dnmarchives/agora/2015-07-07/p/XpMZAakr4w
        # => 2015-07-07
        tm = time.strptime(os.path.dirname(path).split(os.sep)[-2], "%Y-%m-%d")
        return int(1000 * time.mktime(tm))
    else:
        # Use file write date
        print os.path.getmtime(path)
        return 1000*calendar.timegm(time.gmtime(os.path.getmtime(path)))

def inpathToBody(path):
    url = inpathToUrl(path)
    epoch = inpathToEpoch(path)
    with codecs.open(path, 'r', encoding='utf-8') as fi:
        return {"_source": {"raw_content" : fi.read(),
                            "url": url,
                            "timestamp" : epoch}}

def infolderToOutpath(path):
    return os.path.join(path, "contents.cdr")

if __name__ == '__main__':
    outpath = infolderToOutpath(infolder)
    with codecs.open(outpath, 'w', encoding='utf-8') as fo:
        limit = 25000
        for inpath in glob.glob(os.path.join(infolder, "[0-9a-zA-Z]*")):
            url = inpathToUrl(inpath)
            body = inpathToBody(inpath)
            print >> fo, url,
            print >> fo, "\t",
            print >> fo, json.dumps(body)
            limit += -1
            if limit <= 0:
                break

