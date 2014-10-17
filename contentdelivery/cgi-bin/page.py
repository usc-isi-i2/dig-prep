#!/usr/bin/env python
# -*- coding: utf-8 -*-

# enable debugging
import cgitb, cgi
cgitb.enable()

print "Content-Type: text/html;charset=utf-8"
print

form = cgi.FieldStorage()

print """<html>
<body>
This is page.py
<br/>
sha1=%s
<br/>
epoch=%s
</body>
</html>""" % (form.getvalue("sha1","missing"), form.getvalue("epoch","missing"))
