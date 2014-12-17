import sys, os
import dig.pymod.util as util
import simplejson as json
import mysql.connector
import argparse
import urlparse
from itertools import count, izip
from dig.pymod.util import interpretCmdLine, echo

HOST='karma-dig-db.cloudapp.net'
USER='sqluser'
PASSWORD='sqlpassword'
DATABASE='memex_large'
QUERY='select text from ads'
LIMIT=3
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
            tab=False,
            verbose=False):

    def emit(item, stream):
        """JSON-encode all data except URLs"""
        try:
            u = urlparse.urlparse(item)
            # Want to trigger if the payload starts with http
            # but not if merely starts with a URL
            if u.scheme and u.scheme in ['http', 'https'] and ' ' not in item and len(item)<150:
                stream.write(item)
                return
        except Exception as e:
            pass
        stream.write(json.dumps(item, sort_keys=True))

    cnx = mysql.connector.connect(user=user,
                                  password=password,
                                  host=host,
                                  database=database)
    cursor = cnx.cursor()
    if limit:
        query = query.rstrip() + " LIMIT %s" % limit
    if verbose:
        print >> sys.stderr, "Query: %r" % query
    # I don't see why we would need multi in this case
    cursor.execute(query, multi=False)

    tally = 0
    for (values) in cursor:
        if verbose:
            print "here is a row"
        if not tab:
            outstream.write("[")
        remaining = len(values)
        for value in values:
            emit(value, outstream)
            remaining -= 1
            if remaining>0:
                if tab:
                    outstream.write("\t")
                else:
                    outstream.write(", ")
        if not tab:
            outstream.write("]")
        outstream.write('\n')
        tally += 1
    return tally

def main(argv=None):
    '''this is called if run from command line'''
    (prog, args) = interpretCmdLine()
    parser = argparse.ArgumentParser(prog, description='db2json')
    # parser.add_argument("-o")
    parser.add_argument('-l','--limit', 
                        help='num rows to fetch', 
                        default=LIMIT)
    parser.add_argument('-o','--output', 
                        help='output file', 
                        default=None)
    parser.add_argument('-q','--query', 
                        help='query or query file', 
                        default=QUERY)
    parser.add_argument('-t','--tab', 
                        help='drop outer brackets, use tab separators', 
                        required=False, 
                        action='store_true')
    parser.add_argument('-v','--verbose', 
                        help='verbose output',
                        required=False, 
                        action='store_true')

    args = parser.parse_args(args)
    try:
        if os.path.exists(args.query):
            args.query = open(args.query, 'r').read()
    except Exception as e:
        print >> sys.stderr, "Failed to open %s [%r], using as as query" % (args.query, e)
    if args.verbose:
        print >> sys.stderr, "output %s" % args.output
        print >> sys.stderr, "query %s" % args.query
        print >> sys.stderr, "tab %s" % args.tab
    if args.output:
        with open(args.output, "w") as f:
            db2json(outstream=f, query=args.query, tab=args.tab, limit=args.limit, verbose=args.verbose)
    else:
        db2json(sys.stdout, query=args.query, tab=args.tab, limit=args.limit, verbose=args.verbose)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
