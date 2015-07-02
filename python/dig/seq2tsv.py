import sys
import os
import argparse
import io
# from hadoop.io import Text
from hadoop.io import SequenceFile

# adapted from https://github.com/matteobertozzi/Hadoop/blob/master/python-hadoop/examples/SequenceFileReader.py

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'usage: seq2tsv <filename>'
        exit(1)
    else:
        reader = SequenceFile.Reader(sys.argv[1])

        key_class = reader.getKeyClass()
        value_class = reader.getValueClass()

        key = key_class()
        value = value_class()

        #reader.sync(4042)
        position = reader.getPosition()
        while reader.next(key, value):
            # print '*' if reader.syncSeen() else ' ',
            print >> sys.stdout, '%s\t%s' % (key.toString(), value.toString())
            position = reader.getPosition()
        reader.close()
