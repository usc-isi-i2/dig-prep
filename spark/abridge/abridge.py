#!/usr/bin/env python

try:
    from pyspark import SparkContext, SparkFiles
except:
    print "### NO PYSPARK"
import sys
import os
import platform
import socket
import argparse
import json
from random import randint
import time
from datetime import timedelta

# Sniff for execution environment

location = "hdfs"
try:
    if "avatar" in platform.node():
        location = "local"
except:
    pass
try:
    if "avatar" in socket.gethostname():
        location = "local"
except:
    pass
print "### location %s" % location


configDir = os.getcwd() if location=="hdfs" else os.path.join(os.path.dirname(__file__), "data/config")
def configPath(n):
    return os.path.join(configDir, n)

binDir = os.getcwd() if location=="hdfs" else os.path.join(os.path.dirname(__file__), "bin")
def binPath(n):
    return os.path.join(binDir, n)

def abridge(sc, input, output, 
           limit=None,
           uriClass=None,
           debug=0, location='hdfs', outputFormat="sequence"):

    debugOutput = output + '_debug'
    def debugDump(rdd,keys=True,listElements=False):
        keys=False
        if debug >= 2:
            startTime = time.time()
            outdir = os.path.join(debugOutput, rdd.name() or "anonymous-%d" % randint(10000,99999))
            keyCount = None
            try:
                keyCount = rdd.keys().count() if keys else None
            except:
                pass
            rowCount = None
            try:
                rowCount = rdd.count()
            except:
                pass
            elementCount = None
            try:
                elementCount = rdd.mapValues(lambda x: len(x) if isinstance(x, (list, tuple)) else 0).values().sum() if listElements else None
            except:
                pass
            rdd.saveAsTextFile(outdir)
            endTime = time.time()
            elapsedTime = endTime - startTime
            print "wrote [%s] to outdir %r: [%s, %s, %s]" % (str(timedelta(seconds=elapsedTime)), outdir, keyCount, rowCount, elementCount)

    numPartitions = None
    # LOADING DATA
    if numPartitions:
        rdd_ingest = sc.sequenceFile(input, minSplits=numPartitions)
    else:
        rdd_ingest = sc.sequenceFile(input)
    rdd_ingest.setName('rdd_ingest_input')

    # LIMIT/SAMPLE (OPTIONAL)
    sampleSeed = 1234;
    if limit==0:
        limit = None
    if limit:
        # Because take/takeSample collects back to master, can create "task too large" condition
        # rdd_ingest = sc.parallelize(rdd_ingest.take(limit))
        # Instead, generate approximately 'limit' rows
        ratio = float(limit) / rdd_ingest.count()
        rdd_ingest = rdd_ingest.sample(False, ratio, seed=sampleSeed)
        
    # layout: pageUri -> content serialized JSON string
    rdd_ingest.setName('rdd_ingest_net')
    debugDump(rdd_ingest)

    # layout: pageUri -> dict (from json)
    rdd_json = rdd_ingest.mapValues(lambda x: json.loads(x))
    rdd_json.setName('rdd_json')
    debugDump(rdd_json)

    # RETAIN ONLY THOSE MATCHING URI CLASS
    if uriClass:
        rdd_relevant = rdd_json.filter(lambda (k,j): j.get("a", None)==uriClass)
    else:
        rdd_relevant = rdd_json
    rdd_relevant.setName('rdd_relevant')
    debugDump(rdd_relevant)

    rdd_final = rdd_relevant.mapValues(lambda v: json.dumps(v))
    rdd_final.setName('rdd_final')
    debugDump(rdd_final)

    if rdd_final.isEmpty():
        print "### NO DATA TO WRITE"
    else:
        if outputFormat == "sequence":
            rdd_final.saveAsSequenceFile(output)
        elif outputFormat == "text":
            rdd_final.saveAsTextFile(output)
        else:
            raise RuntimeError("Unrecognized output format: %s" % outputFormat)

def main(argv=None):
    '''this is called if run from command line'''
    # pprint.pprint(sorted(os.listdir(os.getcwd())))
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    parser.add_argument('-o','--output', required=True)
    parser.add_argument('-u','--uriClass', default='Offer')
    parser.add_argument('-l','--limit', required=False, default=None, type=int)
    parser.add_argument('-v','--verbose', required=False, help='verbose', action='store_true')
    parser.add_argument('-z','--debug', required=False, help='debug', type=int)
    args=parser.parse_args()

    sparkName = "abridge"

    sc = SparkContext(appName=sparkName)
    abridge(sc, args.input, args.output, 
            uriClass=args.uriClass,
            debug=args.debug,
            limit=args.limit,
            location=location,
            outputFormat="sequence")

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
