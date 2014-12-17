import sys
# from azure import *
# from azure.storage import *
import os
# import urllib2
import argparse
import dig.pymod.util as util
from dig.pymod.util import elapsed, genDatestamps, interpretCmdLine
# from glob import iglob
# import subprocess
# import shutil
# from hadoop.io.SequenceFile import CompressionType
from hadoop.io import Text
from hadoop.io import SequenceFile
import datetime
import simplejson as json
# import re
# import time
# import socket


def main(argv=None):
    '''this is called if run from command line'''
    (prog, args) = interpretCmdLine()
    parser = argparse.ArgumentParser(prog, description='tsv2seq')
    # parser.add_argument()
    parser.add_argument("pathname")
    args = parser.parse_args(args)
    
    outputPathname = args.pathname + ".seq"
    writer = SequenceFile.createWriter(outputPathname, Text, Text)
    count = 0
    start = datetime.datetime.now()
    with open(args.pathname, 'r') as f:
        print f
        for line in f:
            try:
                (url, payload) = line.split('\t')
                key = Text()
                key.set(url)
                value = Text()
                # I'm not at all sure why we would want to decode, not encode here
                # this is the only thing that worked
                value.set(Text.decode(json.dumps(payload)))
                writer.append(key, value)
                count += 1
            except ValueError as e:
                pass
    writer.close()
    end = datetime.datetime.now()
    delta = end - start
    print >> sys.stderr, "ELAPSED tsv2seq is %s" % elapsed(delta)
    return count

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
