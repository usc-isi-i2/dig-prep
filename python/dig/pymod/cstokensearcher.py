#!/usr/bin/python
# Filename: util.py

'''
cstokensearcher
@author: Andrew Philpot
@version 0.1

graft newer regular expression module (regex) allowing 
localized case sensitivity in regex into NLTK's TokenSearcher
Usage: python util.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
'''

## 31 July 2013
# Adapted from:

# Natural Language Toolkit: Texts
#
# Copyright (C) 2001-2011 NLTK Project
# Author: Steven Bird <sb@csse.unimelb.edu.au>
#         Edward Loper <edloper@gradient.cis.upenn.edu>
# URL: <http://www.nltk.org/>
# For license information, see LICENSE.TXT

"""
This module brings together a variety of NLTK functionality for
text analysis, and provides simple, interactive interfaces.
Functionality includes: concordancing, collocation discovery,
regular expression search over tokenized strings, and
distributional similarity.
"""

import regex as re
re.DEFAULT_VERSION = re.VERSION1

from nltk import TokenSearcher

class CaseSensitiveTokenSearcher(TokenSearcher):
    """
    A class that makes it easier to use regular expressions to search
    over tokenized strings.  The tokenized string is converted to a
    string where tokens are marked with angle brackets -- e.g.,
    C{'<the><window><is><still><open>'}.  The regular expression
    passed to the L{findall()} method is modified to treat angle
    brackets as nongrouping parentheses, in addition to matching the
    token boundaries; and to have C{'.'} not match the angle brackets.
    """
    # def __init__(self, tokens):
    #     self._raw = ''.join('<'+w+'>' for w in tokens) 

    def findall(self, regexp):
        """
        Find instances of the regular expression in the text.
        The text is a list of tokens, and a regexp pattern to match
        a single token must be surrounded by angle brackets.  E.g.
        
        >>> ts.findall("<.*><.*><bro>")
        ['you rule bro', ['telling you bro; u twizted bro
        >>> ts.findall("<a>(<.*>)<man>")
        monied; nervous; dangerous; white; white; white; pious; queer; good;
        mature; white; Cape; great; wise; wise; butterless; white; fiendish;
        pale; furious; better; certain; complete; dismasted; younger; brave;
        brave; brave; brave
        >>> text9.findall("<th.*>{3,}")
        thread through those; the thought that; that the thing; the thing
        that; that that thing; through these than through; them that the;
        through the thick; them that they; thought that the
        
        @param regexp: A regular expression
        @type regexp: C{str}
        """

        input = regexp
        # preprocess the regular expression
        regexp = re.sub(r'\s', '', regexp)
        regexp = re.sub(r'<', '(?:<(?:', regexp)
        regexp = re.sub(r'>', ')>)', regexp)
        regexp = re.sub(r'(?<!\\)\.', '[^>]', regexp)

        # print "regexp %r=>%r" % (input, regexp)

        # perform the searchxo
        hits = re.findall(regexp, self._raw)

        # Sanity check
        for h in hits:
            if not h.startswith('<') and h.endswith('>'):
                raise ValueError('Bad regexp for TokenSearcher.findall')
            
        # postprocess the output
        hits = [h[1:-1].split('><') for h in hits]
        return hits
