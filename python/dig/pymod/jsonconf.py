#!/usr/bin/python
# Filename: jsonconf.py

'''
jsonconf
@author: Andrew Philpot
@version 0.5

encode/decode objects in JSON format as configurations
Usage: python jsonconf.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
'''

import sys
import getopt
from objcode import ObjectDecoder
import os
import dig.pymod.util
from dig.pymod.util import echo

VERSION = '0.5'
__version__ = VERSION

# defaults
VERBOSE = False
JSONCONFROOT = """/nfs/isd3/philpot/project/wat/conf"""

def readConfigs(file=None):
    if not file:
        # possibly should run this through makeJsonconfFile to default the dir
        file = "conf/extract.json"
    # print >> sys.stderr, "read from file %s" % file
    try:
        r = util.slurp(open(file))
    except IOError as e:
        print >> sys.stderr, "JSON file %s not found" % file
        raise
    # print >> sys.stderr, "have read a string of len %s" % len(r)
    try:
        # print >> sys.stderr, "trying to decode"
        d = ObjectDecoder().decode(r)
        # print >> sys.stderr, "decoded to object of type %s" % type(d)
        return d
    except ValueError as e:
        print >> sys.stderr, "Bad JSON syntax in %s [%s]" % (file, e)
        raise

def makeJsonconfFile(type, root=None):
    if not root:
        root = JSONCONFROOT
    return os.path.join(root, "conf", "%s.json" % type)

import pprint

def retrieveJson(type, root=None, verbose=False):
    if not root:
        root = JSONCONFROOT
    p = makeJsonconfFile(type, root=root)
    # print >> sys.stderr, "rJ file is %s" % p
    val = readConfigs(file=p)
    if verbose:
        s = ("retrieveJson: file=%r\n" % p) + pprint.pformat(val)
        print >> sys.stderr, s
    return val

def lookupJson(key, jsondict, default=None, type=None):
    v = jsondict.get(key, None) or jsondict.get(unicode(key), None)
    # print """<pre>jd=%r</pre>""" % jsondict
    # print """<pre>key=%s, v=%s</pre>""" % (key, v)
    # print """<pre>key=%r, v=%r</pre>""" % (unicode(key), v)
    if v:
        return v
    elif default==None:
        return None
    elif default=="error":
        raise ValueError("No match for key %s type %s" % (key, type))
    elif default=="warn":
        print >> sys.stderr, "No key %s in %s" % (key, jsondict)
        return None
    else:
        return default

def chooseJson(key, type, root=None, default='error'):
    if not root:
        root = JSONCONFROOT
    choices = retrieveJson(type, root=root)
    return lookupJson(key, choices, default=default, type=type)

# 20 March 2013 by Philpot
# default root = None, then if necessary set inside the fn
# why? because definition binds to value, not to variable

def readConfig(conf='test', root=None, type='db', verbose=VERBOSE):
    if not root:
        root = JSONCONFROOT
    # print "entering RC, JCR = %s" % JSONCONFROOT
    # print "entering RC, root = %s" % root
    if verbose:
        print >> sys.stderr, "reading a key %s from %s/%s" % (conf, root, type)
    chosen = chooseJson(conf, type, root=root)
    if verbose:
        print >> sys.stderr, "%s cfg is %s" % (conf, chosen)
    return chosen

def readDbConfig(conf='test', root=None, verbose=VERBOSE):
    if not root:
        root = JSONCONFROOT
    return readConfig(conf=conf, type='db', root=root)

class Jsonconf(object):
    def __init__(self, filename, verbose=VERBOSE):
        '''create Jsonconf'''
        self.verbose = verbose
        self.filename = filename or 'conf/test.json'

    def readConfigs(self):
        return readConfigs(self.filename)

def main(argv=None):
    '''this is called if run from command line'''
    # process command line arguments
    if argv is None:
        argv = sys.argv
    try:
        opts, args = getopt.getopt(argv[1:], "hv", 
                                   ["echo=", "help",
                                    "verbose"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # default options
    my_verbose = VERBOSE
    # process options
    for o,a in opts:
        if o in ("-h","--help"):
            print __doc__
            sys.exit(0)
        if o in ("--echo", ):
            print a
        if o in ("-v", "--verbose", ):
            my_verbose = True
    filename = args[0] if args else 'conf/test.json'
    jc = Jsonconf(filename, verbose=my_verbose)
    print "From %s, read %s" % (filename, jc.readConfigs())
    c = chooseJson("trbaux", "db")
    print "Choose(trbaux,db) = %s" % c
    print "its port is %s" % c.get('port')


# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# End of jsonconf.py
