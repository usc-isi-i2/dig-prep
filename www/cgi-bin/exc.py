#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import os

class HttpStatus(BaseException):
    code = 200
    name = "OK"
    def report(self):
        print "Status: %d %s\n" % (self.code, self.name)

    def explain(self):
        print "OK %s" % os.environ["REQUEST_URI"] 

class OKHttpStatus(HttpStatus):
    code = 200
    name = "OK"
    def explain(self):
        print "OK %s" % os.environ["REQUEST_URI"]

class FoundHttpStatus(HttpStatus):
    """Not used anymore"""
    def __init__(self, location, mimeType="text/plain"):
        self.location = location
        self.mimeType = mimeType
    # Used to implement the redirect
    code = 302
    name = "Found"
    def report(self):
        print "Status: %d %s" % (self.code, self.name)
        print "Location: %s\n" % (self.location)

    def explain(self):
        print "Redirect %s to %s" % (os.environ["REQUEST_URI"], self.location)

class SeeOtherHttpStatus(HttpStatus):
    def __init__(self, location, mimeType="text/plain"):
        self.location = location
        self.mimeType = mimeType
    # Used to implement the redirect
    code = 303
    name = "See Other"
    def report(self):
        print "Status: %d %s" % (self.code, self.name)
        print "Location: %s\n" % (self.location)

    def explain(self):
        print "Redirect %s to %s" % (os.environ["REQUEST_URI"], self.location)

class NotFoundHttpStatus(HttpStatus):
    # No results: 404 does not exist
    code = 404
    
    def explain(self):
        print "Resource %s unknown" % os.environ["REQUEST_URI"] 
       
class InternalServerErrorHttpStatus(HttpStatus):
    def __init__(self, exception=None):
        self.exception = exception
    # Not a list: 500 server error
    code = 500
    name = "Internal Server Error"

    def explain(self):
        print "Server failed (e) on resource %s" % os.environ["REQUEST_URI"]
        # This is nice for debugging, but leaks the ES server identity
        # if self.exception:
        #     print "Internal error was (%r)" % (self.exception)

class NotImplementedHttpStatus(HttpStatus):
    code = 501
    name = "Not Implemented"
    def explain(self):
        print "Resource %s not implemented" % os.environ["REQUEST_URI"] 
