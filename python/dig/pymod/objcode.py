#!/usr/bin/python
# Filename: objcode.py

'''
objcode
@author: Andrew Philpot
@version 0.2

encode/decode objects
Usage: python objcode.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
'''

# import sys
# import inspect
# import types
# import getopt
# import fileinput
# import tempfile

import json

# 4 December 2012
# consider
# it is claimed that simplejson is equivalent
# it is certainly FASTER
# try:
#     import simplejson as json
# except ImportError:
#     import json

#####
# JSON encoding/decoding of configuration data
# Adapted from http://www.doughellmann.com/PyMOTW/json/
#####
class ObjectEncoder(json.JSONEncoder):
    
    def __init__(self):
        super(ObjectEncoder,self).__init__(indent=2,sort_keys=True)

    def default(self, obj):
        print 'default(', repr(obj), ')'
        # Convert objects to a dictionary of their representation
        d = { '__class__':obj.__class__.__name__, 
              '__module__':obj.__module__,
              }
        d.update(obj.__dict__)
        return d

# print ObjectEncoder().encode(obj)

#$ python json_encoder_default.py

# <MyObj(internal data)>
# default( <MyObj(internal data)> )
# {"s": "internal data", "__module__": "json_myobj", "__class__": "MyObj"}

# Decoding text, then converting the dictionary into an object takes a
# little more work to set up than our previous implementation, but not
# much.  

class ObjectDecoder(json.JSONDecoder):
    
    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object)

    def dict_to_object(self, d):
        if '__class__' in d:
            try:
                class_name = d.pop('__class__')
                module_name = d.pop('__module__')
                # module = __import__(module_name)
                # see http://docs.python.org/2/library/functions.html#__import__
                module = __import__(module_name, globals(), locals(), [class_name], -1)
                # print 'MODULE:', module

                class_ = getattr(module, class_name)
                # print 'CLASS:', class_

                args = dict( (key.encode('ascii'), value) for key, value in d.items())
                # print 'INSTANCE ARGS:', args

                inst = class_(**args)
            except AttributeError as e:
                # print "caught"
                # print "failing module was %s" % module
                raise
        else:
            inst = d
        return inst

# $ python json_decoder_object_hook.py

# MODULE: <module 'json_myobj' from '/Users/dhellmann/Documents/PyMOTW/src/PyMOTW/json/json_myobj.pyc'>
# CLASS: <class 'json_myobj.MyObj'>
# INSTANCE ARGS: {'s': 'instance value goes here'}
# [<MyObj(instance value goes here)>]


#####
# end JSON encoding/decoding of configuration data
#####

VERSION = '0.2'
__version__ = VERSION

# defaults
VERBOSE = True

class Objcode(object):
    def __init__(self, args, verbose=VERBOSE):
        '''create Objcode'''
        self.verbose = verbose
        self.encoder = ObjectEncoder()
        self.decoder = ObjectDecoder()

def main(argv=None):
    '''this is called if run from command line'''
    # process command line arguments
    if argv is None:
        argv = sys.argv
    try:
        opts, args = getopt.getopt(argv[1:], "hv", ["echo=", "help"])
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
    oc = Objcode(args, verbose=my_verbose)
    modname = globals()['__name__']
    print repr(modname), oc, ":"
    module = sys.modules[modname]
    print dir(module) 

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# End of objcode.py
