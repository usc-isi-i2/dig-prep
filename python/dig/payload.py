import simplejson as json
import sys

for line in sys.stdin:
    try:
        (url, repn) = line.split('\t')
        d = json.loads(repn)
        json.dump(d, sys.stdout, indent=4, sort_keys=True)
    except ValueError as e:
        pass
