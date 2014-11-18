"""
dig.jpath
"""
__author__ = 'philpot'

import sys
import simplejson as json
import argparse
from jsonpath_rw import parse

# 18 November 2014
# intended as front end to inline_deobfuscator.py
# given input on stdin of the form
# URL\tJSON
# and a path argument on the command line
# return output of the form
# URL\tPAYLOAD
# where the URL is the same as input URL
# where the PAYLOAD is the (first) jsonpath application of the path argument to the input JSON

def main(argv=None):
    '''this is called if run from command line'''
    try:
        argv = argv or sys.argv
        parser = argparse.ArgumentParser()
        parser.add_argument('path', 
                            type=str, 
                            help='see python module jsonpath_rw')
        args = parser.parse_args()
        path = args.path
        expr = parse(path)
        for line in sys.stdin:
            try:
                (url, repn) = line.split('\t')
                obj = json.loads(repn)
                vals = expr.find(obj)
                val = vals[0].value
                print >> sys.stdout, "%s\t%s" % (url, json.dumps(val))
            except Exception as e:
                print >> sys.stderr, "inner %r" % e
                # Failure to parse one line: continue to next
                pass
    except Exception as e:
        print >> sys.stderr, "outer %r" % e
        # Any other failure: die with no output
        pass

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
