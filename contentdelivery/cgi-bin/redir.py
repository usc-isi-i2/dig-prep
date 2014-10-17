#!/usr/local/bin/python
# -*- coding: utf-8 -*-

# activate_this = '/opt/dig/venv/dig/bin/activate_this.py'
# execfile(activate_this, dict(__file__=activate_this))

import sys
try:
    import simplejson as json
except:
    import json
import requests

# target = http://karma-dig-service.cloudapp.net:55333/dig/isi/images/4A07F3C789957DE95F7D4197A5048838FF589C22/1412717393/processed
#
# result looks like (keys may differ)
# {"results":[{"native_url":"http://www.myproviderguide.com/p/73f1a92647ef416faed7bb3708ae459a.jpg",
#              "content_url":"http://karma-dig-4.hdp.azure.karma.isi.edu/image/F78FEFE3405D7D0840D0DB4C340DF93B0765A08E-1412717393",
#              "cache_url":"https://s3.amazonaws.com/roxyimages/4a07f3c789957de95f7d4197a5048838ff589c22.jpg",
#              "memex_url":"http://karma-dig-4.hdp.azure.karma.isi.edu/crawl/4A07F3C789957DE95F7D4197A5048838FF589C22-1412717393",
#              "sha1":"4A07F3C789957DE95F7D4197A5048838FF589C22",
#              "source":"myproviderguide",
#              "content_sha1":"F78FEFE3405D7D0840D0DB4C340DF93B0765A08E",
#              "epoch":1412717393}]}

def fetch(sha1, epoch,
          host = "karma-dig-service.cloudapp.net",
          port = 55333,
          scope = "image",
          format = "processed",
          mapping = {"image": "images",
                     "page": "pages"},
          payload = "cache_url"):
    print >> sys.stderr, "enter fetch"
    template = "http://%s:%s/dig/isi/%s/%s/%s/%s"
    try:
        url = template % (host, port, mapping[scope], sha1, epoch, format)
        print >> sys.stderr, url
        data = json.loads(requests.get(url)).get("results")
        if data:
            return data.get(payload, "no_%s_for_%s" % (url, scope))
        else:
            return "no_json_for_%s" % url
    except Exception as e:
        print >> sys.stderr, "error [%s]" % e
        return None
