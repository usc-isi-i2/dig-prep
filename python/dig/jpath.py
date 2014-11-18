import simplejson as json
import sys
import argparse
from jsonpath_rw import parse

def main(argv=None):
    '''this is called if run from command line'''
    try:
        argv = argv or sys.argv
        parser = argparse.ArgumentParser()
        parser.add_argument('path', 
                            type=str, 
                            help='see python module jsonpath_rw')
        args = parser.parse_args()
        print >> sys.stderr, args
        path = args.path
        expr = parse(path)
        for line in sys.stdin:
            try:
                (url, repn) = line.split('\t')
                print >> sys.stderr, "repn %r" % repn
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
