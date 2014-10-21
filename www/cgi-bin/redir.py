#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import sys, os
try:
    import simplejson as json
except:
    import json
import requests

import cgi, cgitb

from exc import HttpStatus, NotFoundHttpStatus, InternalServerErrorHttpStatus, NotImplementedHttpStatus

# target = http://karma-dig-service.cloudapp.net:55333/dig/isi/images/4A07F3C789957DE95F7D4197A5048838FF589C22/1412717393/processed
#
# result looks like (keys may differ)
# {"results":[{"native_url":"http://www.myproviderguide.com/p/73f1a92647ef416faed7bb3708ae459a.jpg",
#              "content_url":"http://karma-dig-4.hdp.azure.karma.isi.edu/image/F78FEFE3405D7D0840D0DB4C340DF93B0765A08E-1412717393",
#              "cache_url":"https://s3.amazonaws.com/roxyimages/4a07f3c789957de95f7d4197a5048838ff589c22.jpg",
#              "memex_url":"http://karma-dig-4.hdp.azure.karma.isi.edu/crawl/4A07F3C789957DE95F7D4197A5048838FF589C22-1412717393",
#              "sha1":"4A07F3C789957DE95F7D4197A5048838FF589C22",
#              "source":"myproviderguide",
#              "content_sha1":"F78FEFE3405D7D0840D0DB4C340DF93B0765A08E",
#              "epoch":1412717393}]}

def fetch(sha1, epoch,
          host = "karma-dig-service.cloudapp.net",
          port = 55333,
          scope = "image",
          stage = "raw",
          mapping = {"image": "images",
                     "page": "pages"},
          payload = "cache_url",
          verbose = False):
    """Returns location, or throws an exception"""
    if verbose:
        print >> sys.stderr, "enter fetch"
    template = "http://%s:%s/dig/isi/%s/%s/%s/%s"
    if verbose:
        print >> sys.stderr, "template %s" % template
    try:
        url = template % (host, port, mapping[scope], sha1, epoch, stage)
        if verbose:
            print >> sys.stderr, "url %r" % url
        response = requests.get(url)
        if verbose:
            print >> sys.stderr, "response %r" % response
            print >> sys.stderr, "response text %r" % response.text
        datum = json.loads(response.text)
        if verbose:
            print >> sys.stderr, "datum %r" % datum
        results = datum and datum.get("results")
        if verbose:
            print >> sys.stderr, "results %r" % results
        if results:
            if isinstance(results, list):
                if len(results) == 1:
                    # single value
                    return results[0].get(payload)
                else:
                    # 501 Not Implemented (yet)
                    raise NotImplementedHttpStatus()
            else:
                # Not a list: 500 server error
                raise InternalServerErrorHttpStatus()
        else:
            raise NotFoundHttpStatus()
    except HttpStatus:
        raise
    except Exception as e:
        # Any error: 500 server error
        raise InternalServerErrorHttpStatus(e)

def debugRespond(sha1=None, epoch=None, scope=None, stage=None, location=None, err=None):
    # print a debug page
    cgitb.enable()
    print "Content-Type: text/html;charset=utf-8"
    print

    print """<html>
    <body>
    Handler for %s
    <br/>
    sha1=%s
    <br/>
    epoch=%s
    <br/>
    scope=%s
    <br/>
    stage=%s
    <br/>
    location=%s
    <br/>
    error=%r
    <br/>
    env=%r
    </body>
    </html>""" % (scope, sha1, epoch, scope, stage, location, err, os.environ)

def handleRedirect(scope="page", defaultStage="raw", debug=False, verbose=False):
    sha1 = None
    epoch = None
    location = None
    try:
        form = cgi.FieldStorage()
        sha1 = form.getvalue("sha1","missing").upper()
        epoch = form.getvalue("epoch","missing")
        stage = form.getvalue("stage",defaultStage).lower()
        location = fetch(sha1, epoch, scope=scope, stage=stage)
        # We got an answer, so emit the redirect
        if verbose:
            print >> sys.stderr, "location %s" % location
        # print "Status: 303 See Other"
        print "Status: 302 Found"
        print "Location: %s\n" % location
    except HttpStatus as status:
        if debug:
            debugRespond(sha1, epoch, scope, stage, location, status)
        else:
            status.report()
            status.explain()
    except Exception as err:
        if debug:
            debugRespond(sha1, epoch, scope, stage, location, err)
        else:
            # Presumably an error that was not forseen in fetch
            # Perform the redirect inline
            print "Status: 500 Internal Server Error\n"
            print "Server failed (%s) on resource %s" % (scope, os.environ["REQUEST_URI"])
