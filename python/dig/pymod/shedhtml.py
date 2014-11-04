#!/usr/bin/python
# Filename: shedhtml.py

'''
shedhtml nee edu.isi.nlp.lib.shedhtml
@author: Andrew Philpot
@version 1.1

Usage: python shedhtml.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
'''

import sys
from HTMLParser import HTMLParser, HTMLParseError
import StringIO
from dig.pymod.util import asStream
import dig.pymod.util
import codecs

VERSION = '1.1'
__version__ = VERSION
REVISION = "$Revision: 21617 $"
VERBOSE = True

blockLevelElements = ["p", "h1", "h2", "h3", "h4", "h5", "h6", "dl",
                      "dt", "dd", "ol", "ul", "li", "dir", "address",
                      "blockquote", "center", "del", "div", "ins",
                      "hr", "noscript", "pre", "script"]

# create a subclass and override the handler methods
class HTMLShedder(HTMLParser):
    def __init__(self):
        self.buffer = StringIO.StringIO()
        # this works only for new-style class
        # super(HTMLShedder,self).__init__()
        HTMLParser.__init__(self)

    # def handle_starttag(self, tag, attrs):
    #     print "starting %s" % tag

    def handle_data(self, data):
        # print "Encountered some data  :", data
        self.buffer.write(data)

    def handle_endtag(self,tag):
        if tag == "br" or tag in blockLevelElements:
            self.buffer.write(" ")

def shedHTML(input):
    stream = asStream(input, 'r')
    parser = HTMLShedder()
    output = ""
    try:
        data = stream.read()
        parser.feed(data)
        output = parser.buffer.getvalue()
        parser.close()
    except HTMLParseError, hpe:
        print >> sys.stderr, "HTML parse error %s, data skipped" % hpe
    return output

# call main() if this is run as standalone
if __name__ == "__main__":
    print shedHTML(sys.stdin)

# End of shedhtml.py
