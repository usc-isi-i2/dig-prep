import sys, os
import dig.pymod.util as util
import simplejson as json
import mysql.connector
import argparse
import urlparse
from itertools import count, izip
from dig.pymod.util import interpretCmdLine

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
            maxAttempts=MAXATTEMPTS,
            unwrap=False):

    def emit(item, stream):
        """JSON-encode all data except URLs"""
        try:
            u = urlparse(item)
            if u.scheme:
                stream.write(item)
            return
        except:
            pass
        stream.write(json.dumps(item, sort_keys=True))

    cnx = mysql.connector.connect(user=user,
                                  password=password,
                                  host=host,
                                  database=database)
    cursor = cnx.cursor()
    if limit:
        query = query + " LIMIT %s" % limit
    cursor.execute(query)

    tally = 0
    for (values) in cursor:
        if not unwrap:
            outstream.write("[")
        for (value, remaining) in izip(values, xrange(len(values)-1, -1, -1)):
            emit(value, outstream)
            if remaining>0:
                oustream.write(", ")
        if not unwrap:
            outstream.write("]")
        outstream.write('\n')
        tally += 1
    return tally

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
    parser.add_argument('-u','--unwrap', 
                        help='drop outer brackets', 
                        required=False, 
                        action='store_true')
    args = parser.parse_args(args)
    try:
        if os.path.exists(args.query):
            args.query = args.query.open().read()
    except:
        print >> sys.stderr, "Failed to open %s, using as as query" % args.query
    if args.output:
        with open(args.output, "w") as f:
            db2json(outstream=f, query=args.query, unwrap=args.unwrap)
    else:
        db2json(sys.stdout, query=args.query, unwrap=args.unwrap)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
