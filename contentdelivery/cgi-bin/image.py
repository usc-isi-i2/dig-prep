#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import sys
# enable debugging
import cgitb, cgi
cgitb.enable()

try:
    from redir import fetch
    print >> sys.stderr, "enter"
    form = cgi.FieldStorage()
    sha1 = form.getvalue("sha1","missing")
    epoch = form.getvalue("epoch","missing")
    location = fetch(sha1, epoch, scope="image")
    print >> sys.stderr, "%s, %s, %s" % (sha1, epoch, location)
    # print "Location: %s" % location
    print "Content-Type: text/plain;charset=utf-8"
    print
    print "sha1=%s" % sha1
    print "epoch=%s" % epoch
    print "location=%s" % location
except Exception as e:

    print "Content-Type: text/html;charset=utf-8"
    print

    print """<html>
    <body>
    This is image.py
    <br/>
    sha1=%s
    <br/>
    epoch=%s
    <br/>
    error=%r
    </body>
    </html>""" % (sha1, epoch, e)

