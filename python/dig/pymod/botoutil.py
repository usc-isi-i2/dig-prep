#!/usr/bin/python
# Filename: botoutil.py

'''
botoutil
@author: Andrew Philpot
@version 0.2

collection of boto utils
Usage: python botoutil.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
'''

import sys
import os
import dig.pymod.util
# import site

import boto
import govcloud
import uuid
import glob
import subprocess
from itertools import izip
import datetime as dt

VERSION = '0.2'
__version__ = VERSION
REVISION = '$Revision: 22117 $'.replace('$','')

# defaults
VERBOSE = True

def findBucket(conn, bucketName):
    """only because s3.createBucket doesn't seem to work in govcloud"""
    for cand in conn.get_all_buckets():
        if cand.name == bucketName:
            return cand
    return None

def copyObject(conn,
               src_bucket_name,
               src_key_name,
               dst_bucket_name,
               dst_key_name,
               metadata=None,
               preserve_acl=True):
    """
    adapted from http://stackoverflow.com/questions/2481685/amazon-s3-boto-how-to-rename-a-file-in-a-bucket
    Copy an existing object to another location.  Only works for object smaller than typical file limit, sigh.

    src_bucket_name   Bucket containing the existing object.
    src_key_name      Name of the existing object.
    dst_bucket_name   Bucket to which the object is being copied.
    dst_key_name      The name of the new object.
    metadata          A dict containing new metadata that you want
                      to associate with this object.  If this is None
                      the metadata of the original object will be
                      copied to the new object.
    preserve_acl      If True, the ACL from the original object
                      will be copied to the new object.  If False
                      the new object will have the default ACL.
    """
    conn = conn or govcloud.connect_s3()
    # bucket = conn.get_key(src_bucket_name)
    bucket = findBucket(conn, src_bucket_name)

    # Lookup the existing object in conn
    key = bucket.lookup(src_key_name)

    # Copy the key back on to itself, with new metadata
    return key.copy(dst_bucket_name, dst_key_name,
                    metadata=metadata, preserve_acl=preserve_acl)

def findBucket(conn, bucketName):
    """only because s3.createBucket doesn't seem to work in govcloud"""
    for cand in conn.get_all_buckets():
        if cand.name == bucketName:
            return cand
    return None

def splitFile(f, rootdir="/tmp", splitCmd="/usr/bin/split", chunkSize="100m"):
    """Return list of temp file chunks.  Delete after you're finished with them"""
    d = str(uuid.uuid4())
    path = os.path.join(rootdir, d)
    # I want it to fail hard here
    os.makedirs(path)
    prefix = os.path.join(path, "chunk-")
    subprocess.check_call([splitCmd, "-b", chunkSize, "-d", "-a", "5", f, prefix])
    chunks = glob.glob(os.path.join(path, "chunk-*"))
    chunks.sort()
    return chunks

def mpupload(f, bucketName, keyName, conn=None, verbose=False, acl='public-read', logger=None):
    n1 = dt.datetime.now()
    bkt = findBucket(conn, bucketName)
    if not bkt:
        raise("No such bucket %s" % bucketName)
    mp = bkt.initiate_multipart_upload(keyName)
    if verbose:
        print mp
    srcSize = os.path.getsize(f)
    partSumSize = 0
    objectSize = 0
    chunks = splitFile(f)
    if logger:
        logger.info("s3arch mpupload %d chunks in %s" % (len(chunks), os.path.dirname(chunks[0])))
    for chunk, idx in izip(chunks,range(1,len(chunks)+1)):
        if verbose:
            print chunk, idx
        fp = open(chunk, 'rb')
        part = mp.upload_part_from_file(fp, idx)
        if verbose:
            # part is always None
            print fp, part
        fp.close()
        os.remove(chunk)
    for part in mp:
        if verbose:
            print part, part.part_number, part.size
        partSumSize += part.size
    status = mp.complete_upload()
    n2 = dt.datetime.now()
    objectSize = bkt.get_key(keyName).size
    bkt.get_key(keyName).set_acl(acl)
    # if initial, sum(parts) and final all have same size, assume success
    sameSize = srcSize == partSumSize and partSumSize == objectSize
    os.rmdir(os.path.dirname(chunks[0]))
    d = {"file": f,
         "sameSize": sameSize,
         "conn": conn,
         "bucketName": bucketName,
         "bucket": bkt,
         "keyName": keyName,
         "mp": mp,
         "chunks": chunks,
         "uploadStatus": status,
         "elapsed": n2-n1,
         "transferred": objectSize}
    if verbose:
        print d
        print f
    return d
