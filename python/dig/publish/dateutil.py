import time
import calendar

### stolen from build.py

def datestampToEpoch(datestamp):
    # make up the times, just pick 12:00:01.000
    epoch = 1000*calendar.timegm(time.strptime(datestamp + " 12:00:01 am", "%Y%m%d %I:%M:%S %p"))
    return epoch

def datestampToDatestring(datestamp):
    return time.strftime("%Y-%m-%d", time.strptime(datestamp, "%Y%m%d")) + " 12:00:01 am"

def datestringToEpoch(datestring, fmt="%Y-%m-%d %H:%M:%S"):
    epoch = 1000*calendar.timegm(time.strptime(datestring, fmt))
    return epoch

def datestringToDatestamp(datestring, fmt="%Y-%m-%d %H:%M:%S"):
    return time.strftime("%Y%m%d", time.strptime(datestring, fmt))
    
### end theft
