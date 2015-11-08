#!/usr/bin/env python

from pyspark import SparkContext
from optparse import OptionParser
import argparse


def get_hbase_as_rdd(host,tablename):

	sc = SparkContext(appName="hbase2rdd")
	conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": tablename}
	print "Connecting to host: " + conf["hbase.zookeeper.quorum"] + " table: " + conf["hbase.mapreduce.inputtable"]
	keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
	valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
	hbase_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat","org.apache.hadoop.hbase.io.ImmutableBytesWritable","org.apache.hadoop.hbase.client.Result",keyConverter=keyConv,valueConverter=valueConv,conf=conf)
	return hbase_rdd

if __name__ == "__main__":
	argp = argparse.ArgumentParser()
	argp.add_argument("-hostname", help="hbase hostname")
	argp.add_argument("-tablename", help="hbase tablename")
	arguments = argp.parse_args()

	if arguments.hostname is None or arguments.tablename is None:
		print "Please provide hbase host and tablename"
	else:
		rddh = get_hbase_as_rdd(arguments.hostname,arguments.tablename)
		print rddh.count()
    
    
