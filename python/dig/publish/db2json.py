import os
import dig.pymod.util as util
import datetime
import simplejson as json
import re
import time
import mysql.connector
import argparse

HOST='karma-dig-db.cloudapp.net'
USER='sqluser'
PASSWORD='sqlpassword'
DATABASE='memex_small'
QUERY='select text from ads'
LIMIT=10
MAXATTEMPTS=3

OUTFILE='/tmp/db2json.json'

def db2json(outstream=sys.stdout,
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE,
            query=QUERY,
            limit=LIMIT,
            maxAttempts=MAXATTEMPTS):

    cnx = mysql.connector.connect(user=user,
                                  password=password,
                                  host=host,
                                  database=database)
    cursor = cnx.cursor()
    if limit:
        query = query + " LIMIT %s" % limit
    cursor.execute(query)

    count = 0
    for (row) in cursor:
        print >> outstream, json.dumps(row, sort_keys=True)
        count += 1
    return count

outfile=OUTFILE,


def main(argv=None):
    '''this is called if run from command line'''
    (prog, args) = interpretCmdLine()
    parser = argparse.ArgumentParser(prog, description='db2json')
    # parser.add_argument("-o")
    parser.add_argument('-o','--output', 
                        help='output file', 
                        default=None)
    parser.add_argument('-q','--query', 
                        help='query or query file', 
                        default=QUERY)
    args = parser.parse_args(args)
    try:
        if os.path.exists(args.query):
            args.query = args.query.open().read()
    except:
        print >> sys.stderr, "Failed to open %s, using as as query" % args.query
    if args.output:
        with open(args.output, "w") as f:
            db2json(outstream=f, query=args.query)
        else:
            db2json(sys.stdout, query=args.query)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
