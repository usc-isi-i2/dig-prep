import sys
import os
import argparse
import dig.pymod.util as util
from dig.pymod.util import elapsed, genDatestamps, interpretCmdLine
# from hadoop.io import Text
from hadoop.io import SequenceFile
import datetime

# adapted from https://github.com/matteobertozzi/Hadoop/blob/master/python-hadoop/examples/SequenceFileReader.py

def main(argv=None):
    '''this is called if run from command line'''
    (prog, args) = interpretCmdLine()
    parser = argparse.ArgumentParser(prog, description='seq2tsv')
    # parser.add_argument()
    parser.add_argument("pathname")
    args = parser.parse_args(args)
    outputPathname = args.pathname + ".tsv"
    count = 0
    start = datetime.datetime.now()
    with open(outputPathname, 'w') as f:
        reader = SequenceFile.Reader(args.pathname)

        key_class = reader.getKeyClass()
        value_class = reader.getValueClass()

        key = key_class()
        value = value_class()

        # reader.sync(4042)
        position = reader.getPosition()
        while reader.next(key, value):
            # print '*' if reader.syncSeen() else ' ',
            print >> f, '%s\t%s' % (key.toString(), value.toString())
            position = reader.getPosition()

        reader.close()
    end = datetime.datetime.now()
    delta = end - start
    print >> sys.stderr, "ELAPSED seq2tsv is %s" % elapsed(delta)
    return count

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
