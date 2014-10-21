#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import os

print "Status: 404 Not Found\n"
print "Resource %s unknown" % os.environ["REDIRECT_URL"] 
