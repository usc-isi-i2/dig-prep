#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import sys, os
try:
    import simplejson as json
except:
    import json
import requests

import cgi, cgitb

from exc import HttpStatus, SeeOtherHttpStatus, NotFoundHttpStatus, InternalServerErrorHttpStatus, NotImplementedHttpStatus

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

# Drop None and "extracted" here: must be referred from redirect service which interprets those
LEGAL_STAGES = ["raw", "processed"]
LEGAL_SCOPES = ["page", "image"]
LEGAL_PARTS = ["featurecollection"]

PAYLOADS = { 
    # maps (<scope>, <stage>) into a function to drill down to the value to return from ES result
    ("page", "raw"): lambda(j): j.get("cache_url"),
    ("page", "processed"): lambda(j): j,
    ("page", "processed/featurecollection"): lambda(j): j.get("hasFeatureCollection"),

    ("image", "raw"): lambda(j): j.get("cache_url"),
    ("image", "processed"): lambda(j): j,
    ("image", "processed/featurecollection"): lambda(j): j.get("hasFeatureCollection"),
    }

MIME_TYPES = {
    # maps (<scope>, <stage>) into a function generate the type to return from ES result
    # None means we don't do the MIME stuff, we redirect and count on the end destination server to handle
    ("page", "raw"): lambda(j): None,
    ("page", "processed"): lambda(j): "application/json",
    ("page", "processed/featurecollection"): lambda(j): "application/json",

    ("image", "raw"): lambda(j): None,
    ("image", "processed"): lambda(j): "application/json",
    ("image", "processed/featurecollection"): lambda(j): "application/json",
    }

VERBOSE = False

def fetch(sha1, epoch,
          host = "karma-dig-service.cloudapp.net",
          port = 55333,
          scope = "image",
          stage = "raw",
          part = None,
          mapping = {"image": "images",
                     "page": "pages"},
          verbose = VERBOSE):
    """Returns location, or throws an exception"""
    template = "http://%s:%s/dig/isi/%s/%s/%s/%s"
    if verbose:
        print >> sys.stderr, "template %s" % template
    try:
        if not scope in LEGAL_SCOPES:
            # Unrecognized scope => 404
            raise NotFoundHttpStatus()
        if not stage in LEGAL_STAGES:
            # Unrecognized stage => 404
            raise NotFoundHttpStatus()
        # subkey is <stage> or <stage>/<part>
        subkey = stage
        if part:
            subkey = "%s/%s" % (stage, part)
        payload = PAYLOADS[(scope, subkey)]
        mime_type = MIME_TYPES[(scope, subkey)]
        url = template % (host, port, mapping[scope], sha1, epoch, stage)
        if verbose:
            print >> sys.stderr, "templated url %r" % url
        response = requests.get(url)
        if verbose:
            print >> sys.stderr, "scope %r" % (scope)
            print >> sys.stderr, "stage %r" % (stage)
            print >> sys.stderr, "sha1 %r" % (sha1)
            print >> sys.stderr, "epoch %r" % (epoch)
            print >> sys.stderr, "response %r" % response
            print >> sys.stderr, "response text %r" % response.text
            print >> sys.stderr, "response status %r" % response.status_code
            # this is the response content type
            print >> sys.stderr, "response content type %r" % response.headers['content-type']
        if response.status_code != 200:
            # If ES didn't recognize, treat as 404
            raise NotFoundHttpStatus()
        dct = json.loads(response.text)
        if verbose:
            print >> sys.stderr, json.dumps(dct, sort_keys=True, indent=4)
        # dct might be a set of one or more results, represented as {"results": [r1, r2]}
        # or it might be a single result {"a:" ...}
        results = dct and dct.get("results")
        single = dct and dct.get("a") and dct
        # results should be a list of json objects
        if verbose:
            print >> sys.stderr, "raw results %r" % (results or single)
        if single:
            if verbose:
                print >> sys.stderr, "single: return directly"
                print >> sys.stderr, "payload %r" % payload(single)
                print >> sys.stderr, "mime_type %r" % mime_type(single)
            return (payload(single), mime_type(single))
        if results:
            if isinstance(results, list):
                if len(results) == 1:
                    # single result:
                    # redirect to that URL
                    raise SeeOtherHttpStatus(payload(results[0]))
                else:
                    # multiple values:
                    # cannot redirect; we want to emit as JSON
                    # but this is not implemented yet
                    # return ([payload(r) for r in results], mime_type)
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

def debugRespond(sha1=None, epoch=None, scope=None, stage=None, location=None, mimeType=None, err=None):
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
    mimeType=%s
    <br/>
    error=%r
    <br/>
    env=%r
    </body>
    </html>""" % (scope, sha1, epoch, scope, stage, location, mimeType, err, os.environ)

def handleRedirect(scope="page", stage="raw", part=None, debug=False, verbose=VERBOSE):
    sha1 = None
    epoch = None
    location = None
    try:
        form = cgi.FieldStorage()
        sha1 = form.getvalue("sha1", "missing").upper()
        epoch = form.getvalue("epoch", "missing")
        stage = form.getvalue("stage", stage).lower()
        part = form.getvalue("part", part)
        (value, contentType) = fetch(sha1, epoch, scope=scope, stage=stage, part=part)
        # We got an answer, so we present that data
        # Should this be OKHttpStatus?
        if verbose:
            print >> sys.stderr, "fetch value %s" % value
            print >> sys.stderr, "fetch contentType %s" % contentType
        print "Content-type: %s" % (contentType)
        print
        # needs to be more sophisticated?
        print "%s" % json.dumps(value)
    except HttpStatus as status:
        # even redirects handled here now
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
            print "Internal error was (%r)" % (err)
