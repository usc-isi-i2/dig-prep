#!/usr/bin/python
# -*- coding: utf-8 -*-
# Filename: bphone.py

'''
bphone
@author: Andrew Philpot
@version 0.4

wat phone module
Usage: python bphone.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
\t-s, --source:\tsource default backpage
'''

import sys
import getopt
import watdb
from watdb import Watdb
import re
from dig.pymod.util import asStream
import dig.pymod.util

from watlog import watlog
logger = watlog("wat.bphone")
logger.info('wat.bphone initialized')

## todo
## consider /nfs/studio-data/wat/data/escort/20130124/neworleans.backpage.com/FemaleEscorts/sweet-southern-beautyariel-21/7544109
## apparently extracts part of URL as phone number
## we are supposed to be looking only at the text proper
## is shedhml doing its job?

VERSION = '0.4'
REVISION = "$Revision: 23000 $"

# defaults
VERBOSE = True

AREA_CODES = dict()

def read_area_codes():
    sql = "select a.areacode, a.state from areacode a"
    db = Watdb(conf='wataux', engine=None)
    db.connect()
    rows = db.cxn.query(sql)
    for row in rows:
        ac = row.areacode
        state = row.state
        AREA_CODES[str(ac)] = state
    db = db.disconnect()
    return AREA_CODES

def ensure_area_codes():
    if not AREA_CODES:
        read_area_codes()
    return AREA_CODES

ensure_area_codes()

def valid_area_code(ac):
    return ensure_area_codes().get(ac,False)

def valid_phone_number(ph, test_area_code=True):
    m = re.search(r"""^[2-9]\d{2}[2-9]\d{6}$""", ph)
    if m:
        if test_area_code:
            return valid_area_code(ph[0:3])
        else:
            return True
    else:
        return False

def clean_phone_text(text):
    text = text.lower()
    
    # simply remove numeric entities
    text = re.sub(r"""&#\d{1,3};""", "", text, flags=re.I)

    # misspelled numeral words 

    # re.sub(pattern,replacement,string, flags=re.I | re.G)
    
    text = re.sub(r"""th0usand""", "thousand", text, flags=re.I)
    text = re.sub(r"""th1rteen""", "thirteen", text, flags=re.I)
    text = re.sub(r"""f0urteen""", "fourteen", text, flags=re.I)
    text = re.sub(r"""e1ghteen""", "eighteen", text, flags=re.I)
    text = re.sub(r"""n1neteen""", "nineteen", text, flags=re.I)
    text = re.sub(r"""f1fteen""", "fifteen", text, flags=re.I)
    text = re.sub(r"""s1xteen""", "sixteen", text, flags=re.I)
    text = re.sub(r"""th1rty""", "thirty", text, flags=re.I)
    text = re.sub(r"""e1ghty""", "eighty", text, flags=re.I)
    text = re.sub(r"""n1nety""", "ninety", text, flags=re.I)
    text = re.sub(r"""fourty""", "forty", text, flags=re.I)
    text = re.sub(r"""f0urty""", "forty", text, flags=re.I)
    text = re.sub(r"""e1ght""", "eight", text, flags=re.I)
    text = re.sub(r"""f0rty""", "forty", text, flags=re.I)
    text = re.sub(r"""f1fty""", "fifty", text, flags=re.I)
    text = re.sub(r"""s1xty""", "sixty", text, flags=re.I)
    text = re.sub(r"""zer0""", "zero", text, flags=re.I)
    text = re.sub(r"""f0ur""", "four", text, flags=re.I)
    text = re.sub(r"""f1ve""", "five", text, flags=re.I)
    text = re.sub(r"""n1ne""", "nine", text, flags=re.I)
    text = re.sub(r"""0ne""", "one", text, flags=re.I)
    text = re.sub(r"""tw0""", "two", text, flags=re.I)
    text = re.sub(r"""s1x""", "six", text, flags=re.I)
    # mixed compound numeral words
    # consider 7teen, etc.
    text = re.sub(r"""twenty[\\W_]{0,3}1""", "twenty-one", text, flags=re.I)
    text = re.sub(r"""twenty[\\W_]{0,3}2""", "twenty-two", text, flags=re.I)
    text = re.sub(r"""twenty[\\W_]{0,3}3""", "twenty-three", text, flags=re.I)
    text = re.sub(r"""twenty[\\W_]{0,3}4""", "twenty-four", text, flags=re.I)
    text = re.sub(r"""twenty[\\W_]{0,3}5""", "twenty-five", text, flags=re.I)
    text = re.sub(r"""twenty[\\W_]{0,3}6""", "twenty-six", text, flags=re.I)
    text = re.sub(r"""twenty[\\W_]{0,3}7""", "twenty-seven", text, flags=re.I)
    text = re.sub(r"""twenty[\\W_]{0,3}8""", "twenty-eight", text, flags=re.I)
    text = re.sub(r"""twenty[\\W_]{0,3}9""", "twenty-nine", text, flags=re.I)
    text = re.sub(r"""thirty[\\W_]{0,3}1""", "thirty-one", text, flags=re.I)
    text = re.sub(r"""thirty[\\W_]{0,3}2""", "thirty-two", text, flags=re.I)
    text = re.sub(r"""thirty[\\W_]{0,3}3""", "thirty-three", text, flags=re.I)
    text = re.sub(r"""thirty[\\W_]{0,3}4""", "thirty-four", text, flags=re.I)
    text = re.sub(r"""thirty[\\W_]{0,3}5""", "thirty-five", text, flags=re.I)
    text = re.sub(r"""thirty[\\W_]{0,3}6""", "thirty-six", text, flags=re.I)
    text = re.sub(r"""thirty[\\W_]{0,3}7""", "thirty-seven", text, flags=re.I)
    text = re.sub(r"""thirty[\\W_]{0,3}8""", "thirty-eight", text, flags=re.I)
    text = re.sub(r"""thirty[\\W_]{0,3}9""", "thirty-nine", text, flags=re.I)
    text = re.sub(r"""forty[\\W_]{0,3}1""", "forty-one", text, flags=re.I)
    text = re.sub(r"""forty[\\W_]{0,3}2""", "forty-two", text, flags=re.I)
    text = re.sub(r"""forty[\\W_]{0,3}3""", "forty-three", text, flags=re.I)
    text = re.sub(r"""forty[\\W_]{0,3}4""", "forty-four", text, flags=re.I)
    text = re.sub(r"""forty[\\W_]{0,3}5""", "forty-five", text, flags=re.I)
    text = re.sub(r"""forty[\\W_]{0,3}6""", "forty-six", text, flags=re.I)
    text = re.sub(r"""forty[\\W_]{0,3}7""", "forty-seven", text, flags=re.I)
    text = re.sub(r"""forty[\\W_]{0,3}8""", "forty-eight", text, flags=re.I)
    text = re.sub(r"""forty[\\W_]{0,3}9""", "forty-nine", text, flags=re.I)
    text = re.sub(r"""fifty[\\W_]{0,3}1""", "fifty-one", text, flags=re.I)
    text = re.sub(r"""fifty[\\W_]{0,3}2""", "fifty-two", text, flags=re.I)
    text = re.sub(r"""fifty[\\W_]{0,3}3""", "fifty-three", text, flags=re.I)
    text = re.sub(r"""fifty[\\W_]{0,3}4""", "fifty-four", text, flags=re.I)
    text = re.sub(r"""fifty[\\W_]{0,3}5""", "fifty-five", text, flags=re.I)
    text = re.sub(r"""fifty[\\W_]{0,3}6""", "fifty-six", text, flags=re.I)
    text = re.sub(r"""fifty[\\W_]{0,3}7""", "fifty-seven", text, flags=re.I)
    text = re.sub(r"""fifty[\\W_]{0,3}8""", "fifty-eight", text, flags=re.I)
    text = re.sub(r"""fifty[\\W_]{0,3}9""", "fifty-nine", text, flags=re.I)
    text = re.sub(r"""sixty[\\W_]{0,3}1""", "sixty-one", text, flags=re.I)
    text = re.sub(r"""sixty[\\W_]{0,3}2""", "sixty-two", text, flags=re.I)
    text = re.sub(r"""sixty[\\W_]{0,3}3""", "sixty-three", text, flags=re.I)
    text = re.sub(r"""sixty[\\W_]{0,3}4""", "sixty-four", text, flags=re.I)
    text = re.sub(r"""sixty[\\W_]{0,3}5""", "sixty-five", text, flags=re.I)
    text = re.sub(r"""sixty[\\W_]{0,3}6""", "sixty-six", text, flags=re.I)
    text = re.sub(r"""sixty[\\W_]{0,3}7""", "sixty-seven", text, flags=re.I)
    text = re.sub(r"""sixty[\\W_]{0,3}8""", "sixty-eight", text, flags=re.I)
    text = re.sub(r"""sixty[\\W_]{0,3}9""", "sixty-nine", text, flags=re.I)
    text = re.sub(r"""seventy[\\W_]{0,3}1""", "seventy-one", text, flags=re.I)
    text = re.sub(r"""seventy[\\W_]{0,3}2""", "seventy-two", text, flags=re.I)
    text = re.sub(r"""seventy[\\W_]{0,3}3""", "seventy-three", text, flags=re.I)
    text = re.sub(r"""seventy[\\W_]{0,3}4""", "seventy-four", text, flags=re.I)
    text = re.sub(r"""seventy[\\W_]{0,3}5""", "seventy-five", text, flags=re.I)
    text = re.sub(r"""seventy[\\W_]{0,3}6""", "seventy-six", text, flags=re.I)
    text = re.sub(r"""seventy[\\W_]{0,3}7""", "seventy-seven", text, flags=re.I)
    text = re.sub(r"""seventy[\\W_]{0,3}8""", "seventy-eight", text, flags=re.I)
    text = re.sub(r"""seventy[\\W_]{0,3}9""", "seventy-nine", text, flags=re.I)
    text = re.sub(r"""eighty[\\W_]{0,3}1""", "eighty-one", text, flags=re.I)
    text = re.sub(r"""eighty[\\W_]{0,3}2""", "eighty-two", text, flags=re.I)
    text = re.sub(r"""eighty[\\W_]{0,3}3""", "eighty-three", text, flags=re.I)
    text = re.sub(r"""eighty[\\W_]{0,3}4""", "eighty-four", text, flags=re.I)
    text = re.sub(r"""eighty[\\W_]{0,3}5""", "eighty-five", text, flags=re.I)
    text = re.sub(r"""eighty[\\W_]{0,3}6""", "eighty-six", text, flags=re.I)
    text = re.sub(r"""eighty[\\W_]{0,3}7""", "eighty-seven", text, flags=re.I)
    text = re.sub(r"""eighty[\\W_]{0,3}8""", "eighty-eight", text, flags=re.I)
    text = re.sub(r"""eighty[\\W_]{0,3}9""", "eighty-nine", text, flags=re.I)
    text = re.sub(r"""ninety[\\W_]{0,3}1""", "ninety-one", text, flags=re.I)
    text = re.sub(r"""ninety[\\W_]{0,3}2""", "ninety-two", text, flags=re.I)
    text = re.sub(r"""ninety[\\W_]{0,3}3""", "ninety-three", text, flags=re.I)
    text = re.sub(r"""ninety[\\W_]{0,3}4""", "ninety-four", text, flags=re.I)
    text = re.sub(r"""ninety[\\W_]{0,3}5""", "ninety-five", text, flags=re.I)
    text = re.sub(r"""ninety[\\W_]{0,3}6""", "ninety-six", text, flags=re.I)
    text = re.sub(r"""ninety[\\W_]{0,3}7""", "ninety-seven", text, flags=re.I)
    text = re.sub(r"""ninety[\\W_]{0,3}8""", "ninety-eight", text, flags=re.I)
    text = re.sub(r"""ninety[\\W_]{0,3}9""", "ninety-nine", text, flags=re.I)
    # now resolve compound numeral words 
    text = re.sub(r"""twenty-one""", "21", text, flags=re.I)
    text = re.sub(r"""twenty-two""", "22", text, flags=re.I)
    text = re.sub(r"""twenty-three""", "23", text, flags=re.I)
    text = re.sub(r"""twenty-four""", "24", text, flags=re.I)
    text = re.sub(r"""twenty-five""", "25", text, flags=re.I)
    text = re.sub(r"""twenty-six""", "26", text, flags=re.I)
    text = re.sub(r"""twenty-seven""", "27", text, flags=re.I)
    text = re.sub(r"""twenty-eight""", "28", text, flags=re.I)
    text = re.sub(r"""twenty-nine""", "29", text, flags=re.I)
    text = re.sub(r"""thirty-one""", "31", text, flags=re.I)
    text = re.sub(r"""thirty-two""", "32", text, flags=re.I)
    text = re.sub(r"""thirty-three""", "33", text, flags=re.I)
    text = re.sub(r"""thirty-four""", "34", text, flags=re.I)
    text = re.sub(r"""thirty-five""", "35", text, flags=re.I)
    text = re.sub(r"""thirty-six""", "36", text, flags=re.I)
    text = re.sub(r"""thirty-seven""", "37", text, flags=re.I)
    text = re.sub(r"""thirty-eight""", "38", text, flags=re.I)
    text = re.sub(r"""thirty-nine""", "39", text, flags=re.I)
    text = re.sub(r"""forty-one""", "41", text, flags=re.I)
    text = re.sub(r"""forty-two""", "42", text, flags=re.I)
    text = re.sub(r"""forty-three""", "43", text, flags=re.I)
    text = re.sub(r"""forty-four""", "44", text, flags=re.I)
    text = re.sub(r"""forty-five""", "45", text, flags=re.I)
    text = re.sub(r"""forty-six""", "46", text, flags=re.I)
    text = re.sub(r"""forty-seven""", "47", text, flags=re.I)
    text = re.sub(r"""forty-eight""", "48", text, flags=re.I)
    text = re.sub(r"""forty-nine""", "49", text, flags=re.I)
    text = re.sub(r"""fifty-one""", "51", text, flags=re.I)
    text = re.sub(r"""fifty-two""", "52", text, flags=re.I)
    text = re.sub(r"""fifty-three""", "53", text, flags=re.I)
    text = re.sub(r"""fifty-four""", "54", text, flags=re.I)
    text = re.sub(r"""fifty-five""", "55", text, flags=re.I)
    text = re.sub(r"""fifty-six""", "56", text, flags=re.I)
    text = re.sub(r"""fifty-seven""", "57", text, flags=re.I)
    text = re.sub(r"""fifty-eight""", "58", text, flags=re.I)
    text = re.sub(r"""fifty-nine""", "59", text, flags=re.I)
    text = re.sub(r"""sixty-one""", "61", text, flags=re.I)
    text = re.sub(r"""sixty-two""", "62", text, flags=re.I)
    text = re.sub(r"""sixty-three""", "63", text, flags=re.I)
    text = re.sub(r"""sixty-four""", "64", text, flags=re.I)
    text = re.sub(r"""sixty-five""", "65", text, flags=re.I)
    text = re.sub(r"""sixty-six""", "66", text, flags=re.I)
    text = re.sub(r"""sixty-seven""", "67", text, flags=re.I)
    text = re.sub(r"""sixty-eight""", "68", text, flags=re.I)
    text = re.sub(r"""sixty-nine""", "69", text, flags=re.I)
    text = re.sub(r"""seventy-one""", "71", text, flags=re.I)
    text = re.sub(r"""seventy-two""", "72", text, flags=re.I)
    text = re.sub(r"""seventy-three""", "73", text, flags=re.I)
    text = re.sub(r"""seventy-four""", "74", text, flags=re.I)
    text = re.sub(r"""seventy-five""", "75", text, flags=re.I)
    text = re.sub(r"""seventy-six""", "76", text, flags=re.I)
    text = re.sub(r"""seventy-seven""", "77", text, flags=re.I)
    text = re.sub(r"""seventy-eight""", "78", text, flags=re.I)
    text = re.sub(r"""seventy-nine""", "79", text, flags=re.I)
    text = re.sub(r"""eighty-one""", "81", text, flags=re.I)
    text = re.sub(r"""eighty-two""", "82", text, flags=re.I)
    text = re.sub(r"""eighty-three""", "83", text, flags=re.I)
    text = re.sub(r"""eighty-four""", "84", text, flags=re.I)
    text = re.sub(r"""eighty-five""", "85", text, flags=re.I)
    text = re.sub(r"""eighty-six""", "86", text, flags=re.I)
    text = re.sub(r"""eighty-seven""", "87", text, flags=re.I)
    text = re.sub(r"""eighty-eight""", "88", text, flags=re.I)
    text = re.sub(r"""eighty-nine""", "89", text, flags=re.I)
    text = re.sub(r"""ninety-one""", "91", text, flags=re.I)
    text = re.sub(r"""ninety-two""", "92", text, flags=re.I)
    text = re.sub(r"""ninety-three""", "93", text, flags=re.I)
    text = re.sub(r"""ninety-four""", "94", text, flags=re.I)
    text = re.sub(r"""ninety-five""", "95", text, flags=re.I)
    text = re.sub(r"""ninety-six""", "96", text, flags=re.I)
    text = re.sub(r"""ninety-seven""", "97", text, flags=re.I)
    text = re.sub(r"""ninety-eight""", "98", text, flags=re.I)
    text = re.sub(r"""ninety-nine""", "99", text, flags=re.I)
    # larger units function as suffixes now
    # assume never have three hundred four, three hundred and four
    text = re.sub(r"""hundred""", "00", text, flags=re.I)
    text = re.sub(r"""thousand""", "000", text, flags=re.I)
    # single numeral words now
    # some would have been ambiguous
    text = re.sub(r"""seventeen""", "17", text, flags=re.I)
    text = re.sub(r"""thirteen""", "13", text, flags=re.I)
    text = re.sub(r"""fourteen""", "14", text, flags=re.I)
    text = re.sub(r"""eighteen""", "18", text, flags=re.I)
    text = re.sub(r"""nineteen""", "19", text, flags=re.I)
    text = re.sub(r"""fifteen""", "15", text, flags=re.I)
    text = re.sub(r"""sixteen""", "16", text, flags=re.I)
    text = re.sub(r"""seventy""", "70", text, flags=re.I)
    text = re.sub(r"""eleven""", "11", text, flags=re.I)
    text = re.sub(r"""twelve""", "12", text, flags=re.I)
    text = re.sub(r"""twenty""", "20", text, flags=re.I)
    text = re.sub(r"""thirty""", "30", text, flags=re.I)
    text = re.sub(r"""eighty""", "80", text, flags=re.I)
    text = re.sub(r"""ninety""", "90", text, flags=re.I)
    text = re.sub(r"""three""", "3", text, flags=re.I)
    text = re.sub(r"""seven""", "7", text, flags=re.I)
    text = re.sub(r"""eight""", "8", text, flags=re.I)
    text = re.sub(r"""forty""", "40", text, flags=re.I)
    text = re.sub(r"""fifty""", "50", text, flags=re.I)
    text = re.sub(r"""sixty""", "60", text, flags=re.I)
    text = re.sub(r"""zero""", "0", text, flags=re.I)
    text = re.sub(r"""four""", "4", text, flags=re.I)
    text = re.sub(r"""five""", "5", text, flags=re.I)
    text = re.sub(r"""nine""", "9", text, flags=re.I)
    text = re.sub(r"""one""", "1", text, flags=re.I)
    text = re.sub(r"""two""", "2", text, flags=re.I)
    text = re.sub(r"""six""", "6", text, flags=re.I)
    text = re.sub(r"""ten""", "10", text, flags=re.I)
    # now do letter for digit substitutions
    text = re.sub(r"""oh""", "0", text, flags=re.I)
    text = re.sub(r"""o""", "0", text, flags=re.I)
    text = re.sub(r"""i""", "1", text, flags=re.I)
    text = re.sub(r"""l""", "1", text, flags=re.I)
    return text

def make_phone_regexp():
    return re.compile(r"""([[{(<]{0,3}[2-9][\W_]{0,3}\d[\W_]{0,3}\d[\W_]{0,6}[2-9][\W_]{0,3}\d[\W_]{0,3}\d[\W_]{0,6}\d[\W_]{0,3}\d[\W_]{0,3}\d[\W_]{0,3}\d)""")

### 8 June 2012
### here's my idea
### stick indices into the string
### update the sub and match REs to recognize (maintain) the indices
### possibly, record the rules used
### when you extract a phone number, also extract the bounding indices
### highlight these back into the original text
### get annotators (turk?) to double check the extraction
### consider removing or reordering rules highly correlated with failures
### consider adding rules for apparently missed cases
### I think this will be a good long day of development
### The lisp version serves as partial 

# (defparameter good-string "f0ur15234567eight")
# (defparameter good-embedded "call me at f0ur15234567eight right now")
# (defparameter bad-string "abcû123")

# (defparameter digits (manifest-ht 'eql
# 				  #\0 (code-char 251)
# 				  #\1 (code-char 252)
# 				  #\2 (code-char 253)
# 				  #\3 (code-char 254)))

# (defparameter class (format nil "[~{~A~}]{4}" (loop for v being the hash-values of digits collect v)))

# (defun rgethash (value ht)
#   (maphash #'(lambda (k v)
# 	       (when (eql v value)
# 		 (return-from rgethash k)))
# 	   ht))

# (defun encode-idx (idx &key (digits digits) (width 4))
#   (let ((base (hash-table-count digits)))
#     (map 'string #'(lambda (char)
# 		     (gethash char digits))
# 	 (format nil "~V,V,'0R" base width idx))))

# (defun decode-idx (code &key (digits digits))
#   (let ((base (hash-table-count digits)))
#     (loop with sum = 0
#        for char across code
#        do (setq sum (+ (* sum base) (digit-char-p (rgethash char digits))))
#        finally (return sum))))
    
# (defun index-string (string)
#   (with-output-to-string (s)
#     (loop for char across string
#        as idx from 0 by 1
#        do (write-string (encode-idx idx) s)
#        do (write-char char s))
#     (write-string (encode-idx (length string)) s)))
	 
# (defun unindex-string (string &key (width 4))
#   (let ((start 0)
# 	(len (length string)))
#     (append (loop 
# 	       collect (decode-idx (subseq string start (+ start width)))
# 	       do (incf start width)
# 	       collect (char string start)
# 	       do (incf start)
# 	       until (>= start (- len width)))
# 	    (list (decode-idx (subseq string (- len width) len))))))

# (defun extend-rewrite (from to)
#   ())

# ;;; KNOWS only three rules

# (defun transform-string (s)
#   (let ((i (index-string s)))
#     ;; old
#     ;; (setq i (regex-replace-all "f0ur" "four" i))
#     ;; new
#     (setq i (regex-replace-all 
# 	     (format nil 
# 		     "(~A)f(~A)0(~A)u(~A)r(~A)"
# 		     class class class class class)
# 	     i
# 	     "\\1f\\2o\\3u\\4r\\5"))

#     ;; old
#     ;; (setq i (regex-replace-all "four" "4" i))
#     ;; new
#     (setq i (regex-replace-all 
# 	     (format nil 
# 		     "(~A)f(~A)o(~A)u(~A)r(~A)"
# 		     class class class class class)
# 	     i
# 	     "\\{1}4\\{5}"))
  
#     ;; old
#     ;; (setq i (regex-replace-all "eight" "8" i))
#     ;; new
#     (setq i (regex-replace-all 
# 	     (format nil 
# 		     "(~A)e(~A)i(~A)g(~A)h(~A)t(~A)"
# 		     class class class class class class)
# 	     i
# 	     "\\{1}8\\{6}"))
#     (let ((u (unindex-string i)))
#       (values s i u))))


PHONE_REGEXP = make_phone_regexp()

# 3 May 2012
# new strategy: skip finditer, do the indexing ourselves

def genPhones(text):
    text = clean_phone_text(text)
    regex = PHONE_REGEXP
    idx = 0
    m = regex.search(text, idx)
    while m:
        g = m.group(1)
        start = m.start(1)
        end = m.end(1)
        digits = re.sub(r"""\D+""", "", g)
        prefix = text[start-1:start] if start>0 else None
        if digits[0:2] == '82' and prefix == '*':
            # this number overlaps with a *82 sequence
            idx += 2
        elif not valid_area_code(digits[0:3]):
            # probably a price, height, etc.
            idx += 1
        else:
            # seems good
            yield digits
            idx = end
        m = regex.search(text, idx)
            
    
def extractPhoneNumbers(text):
    return [ph for ph in genPhones(text)]

class Phone(object):
    def __init__(self, verbose=VERBOSE):
        '''create Phone'''
        self.verbose = verbose
        
TEST_TEXTS = ['*219~*851~33**77',
              '1*574*302*8442* ',
              '2ONE6-SIX2TWO-5NINE5FOUR',
              'TwO/6/zERo-FouR/0/NiNe-4/eIgHt/1/One',
              '[9][0][4]  [4][6][8]  [7][0][8][1]',
              '4-1-9-3-4-0-4-6-4-5',
              '317**407*1740**',
              '317- eight.3.three.62.three.3',
              '3.1.7.5.9.9.6.5.3.4.',
              '{2one9}..{4three3}..{732eight}',
              '((2-1-9)) {{4.3.3.}} [[7328]]',
              '**Rachel567*868*3300**',
              '5@7@4@4@4@0@1@6@0@5',
              '5Seven4Three2Two7Zero7Six',
              'THREE.1.7.FIVE.99.SIX5.3.4',
              '**2**1**9\_ 8***5**1-33**77',
              '~317~371~2144~',
              '2SIX0--3ONE0--000EIGHT.',
              '3!13!2!21!70!95',
              '26o*435*o72o ',
              '**2**1**9\_ 8***5**1-33**77** ',
              '2 six 9 eight 3 o 5 five 6 four',
              '310 fourfoureight 2th0usand',
              "I'm 5'6\" 140 lbs. with a nice plump booty =) Available 24/7 *** 214 784 2976 *** **INCALL SPECIALS ** NO PIMPS ** NO TEXTING",

              '213 2343456 323 4456789 626 987 6543',
              'Drop Dead Gorgeous with outgoing personality.Sure to leave you at ease\
                 specializing in Swedish Deep Tissue Massages.Long legs, thick thighs,\
                 SEXY ALL OVER and very flexible. Want to take an exotic adventure\
                 I can be reached at 404-492-2346.\
                 Incalls and outcalls to all Atlanta areas.\
                 Outcalls are extra for travel.\
                 NO DIRTY TALK!!\
                 100% REAL PICS\
                 ALL NATURAL!!!!!!!',
              # normal
              "8185551212",
              # double
              "2135551212 plus 3104567890",
              # bad ac
              "\$200tel3365551212",
              # difficult, preceding price or dimension
              "\$276 3235551212",
              "5'10\" 323 555 1212",
              # *82 cases
              '*8284323547295836',
              'call me at *8284323547295836',
              '*8234555547295836',
              '***8234555547295836',
              # long
              "*8227354265155512129673",
"Hello I'M BAMBI In im ready to give u THE satisfaction U NEED AND WANT.WHen u leave me u will have NO WORRIES What so ever.IM EvERY Mans DREAM,Very compassionate in love to listen and take my TIME so u can ENJOY My Ride,U will want TO Come back for more TRUST ME.....No Black men Accepted,NO BLOCKED CALLS ACCEPTED 4 MORE Details in PICS Call BAMBI @310-617-3206 DONT Hesitate to call i'll B Waitin 4 YOU....;-);-);-);-)"

              ]

def main(argv=None):
    '''this is called if run from command line'''
    # process command line arguments
    if argv is None:
        argv = sys.argv
    try:
        opts, args = getopt.getopt(argv[1:], "hvs:", 
				   ["echo=", "help"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # default options
    my_verbose = VERBOSE
    # process options
    for o,a in opts:
        if o in ("-h","--help"):
            print __doc__
            sys.exit(0)
        if o in ("--echo", ):
            print a
        if o in ("-v", "--verbose", ):
            my_verbose = True
    ph = Phone(verbose=my_verbose)
    if args:
        for arg in args:
            phones = extractPhoneNumbers(asStream(arg, 'r').read())
            print "|".join(phones)
    else:
        for text in TEST_TEXTS:
            phones = extractPhoneNumbers(text)
            print text, "=>\n\t", "|".join(phones)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# End of phone.py
