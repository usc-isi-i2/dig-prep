import sys, os, time, calendar, socket

import mysql.connector
from azure import WindowsAzureError
import hashlib
from azure.storage import BlobService

try:
    import simplejson as json
except:
    import json

### stolen from build.py

def datestampToEpoch(datestamp):
    # make up the times, just pick 12:00:01.000
    epoch = calendar.timegm(time.strptime(datestamp + " 12:00:01 am", "%Y%m%d %I:%M:%S %p")*1000)
    return epoch

def datestampToDatestring(datestamp):
    return time.strftime("%Y-%m-%d", time.strptime(datestamp, "%Y%m%d")) + " 12:00:01 am"

def datestringToEpoch(datestring, fmt="%Y-%m-%d %H:%M:%S"):
    epoch = calendar.timegm(time.strptime(datestring, fmt)*1000)
    return epoch

def datestringToDatestamp(datestring, fmt="%Y-%m-%d %H:%M:%S"):
    return time.strftime("%Y%m%d", time.strptime(datestring, fmt))
    
### end theft

myaccount="karmadigstorage"
mykey="TJbdTjRymbBHXLsDtF/Nx3+6WXWN0uwh3RG/8GPJQRQyqg+rkOzioczm5czPtr+auGFhNeBx8GTAfuCufRyw8A=="
mycontainer='arch'
mycontainer='istr-memex-small-ads'
bs = BlobService(account_name=myaccount, account_key=mykey)

def publishAzureBlob(tbl='backpage_incoming',
                     source="backpage",
                     limit=2,
                     user='sqluser', 
                     password='sqlpassword',
                     # host='karma-dig-db.cloudapp.net',
                     dbhost='karma-dig-db.cloudapp.net',
                     database='memex_small',
                     maxAttempts = 3):
    cnx = mysql.connector.connect(user=user,
                                  password=password,
                                  host=dbhost,
                                  database=database)
    cursor = cnx.cursor()

    query = (("""SELECT t.url, t.body, a.importtime FROM %s t join ads a on a.url=t.url """ % tbl) + 
             (""" LIMIT %s"""))

    query = query % (limit)

    cursor.execute(query)

    urls = []
    with open('/tmp/azureblobpublish_ads_%s.json' % source, 'w') as f:

        for (url, body, importtime) in cursor:
            datestamp = importtime.strftime('%Y%m%d')
            # emulate https://karmadigstorage.blob.core.windows.net/arch/churl/20140101/olympia.backpage.com/FemaleEscorts/100-asian-hi-im-honey-n-im-super-sweet-25/13538952
            # http://karmadigstorage.blob.core.windows.net/istr-memex-small/istr_memex_small/20140101/olym...
            crawlAgent = "istr_%s" % database
            destination = os.path.join(crawlAgent, str(datestamp), url[7:])
            blobUrl = "http://karmadigstorage.blob.core.windows.net/%s/%s" % (mycontainer, destination)
            try:
                success = False
                remainingAttempts = maxAttempts
                while not success and remainingAttempts>0:
                    try:
                        size = len(body)
                        status = bs.put_block_blob_from_text(mycontainer, destination, body,
                                                             x_ms_blob_content_type='text/html')
                        print >> sys.stderr, "repub %s as %s / %s: size=%d, status=%s" % (url, mycontainer, destination, size, status)
                        success = True

                        native_url = url
                        cache_url = blobUrl
                        sha1 = hashlib.sha1(native_url).hexdigest().upper()
                        epoch = datestringToEpoch(str(importtime))
                        # uid is Amadeep's key to the elastic search data
                        uid = hashlib.sha1(native_url[7:]).hexdigest().upper()
                        j = {"native_url": native_url,
                             "cache_url": cache_url,
                             "sha1": sha1,
                             "epoch": epoch,
                             "source": source,
                             "document_type": "page",
                             "process_stage": "raw"}
                        print >> f, json.dumps({uid: j}, sort_keys=True)
                        break
                    except socket.error as se:
                        remainingAttempts -= 1
                        print >> sys.stderr, "repub %s failed, sleep 5 sec, %d more tries" % (url, remainingAttempts)
                        time.sleep(5)
                    except WindowsAzureError as e:
                        print >> sys.stderr, "Azure failure [%r], skipping"
            except Exception as e:
                print >> sys.stderr, "Total failure per [%s]" % e

    cnx.close()
    return urls

def pab():
    publishAzureBlob()
