#!/usr/bin/python

import sys
import socket
import uuid
import requests
from pymod.util import info
from pymod.util import interpretCmdLine
import argparse
import datetime

KEY="AGTYEWA282GKJ8UZ"

PS=socket.gethostname()
UUID=str(uuid.uuid4())
CMD="user-list_users_no_pw"
 
# if [ $# -lt 1 ]; then
#     echo "usage: `basename $0` [hostname]" 
#     exit 1
# fi

# if [ "$PS" = "" ]; then
#     PS=`hostname | cut -f1 -d.`
# fi 


# # ARGS='other_http_get_arguments_for_the_DreamHost_cmd_that_you_are_using=4&foo=123'
# ARGS=""
# LINK="https://api.dreamhost.com/?key=%s&unique_id=%s&cmd=%s&ps=%s&%s" % (KEY, UUID, CMD, PS, ARGS)
# print LINK

# # RESPONSE=`wget -O- -q "$LINK"`

# RESPONSE=requests.get(LINK)

# # echo "$RESPONSE"
# # if ! (echo $RESPONSE | grep -q 'success'); then
# #     exit 1
# # fi

# print RESPONSE
# print RESPONSE.text

# Now use the content of $RESPONSE do do whatever you wish.
#
# Rinse, lather, repeat.

# ami_launch_index=`curl http://169.254.169.254/latest/meta-data/ami-launch-index`

from time import sleep
from timeout_decorator import timeout, TimeoutError
@timeout(5)
def my_external_ip():
    # first use SOCKET info
    r = socket.gethostbyname(socket.gethostname())
    # but on AWS, need to query this metadata instead
    success = False
    while not success:
        try:
            r=requests.get("http://169.254.169.254/latest/meta-data/network-ipv4")
            return r
        except requests.exceptions.ConnectionError as ce:
            continue
        except TimeoutError as e:
            break
    return r

# print "My external IP is %s" % my_external_ip()

def main():
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser(sys.argv, description='update_dns')
    parser.add_argument("-t", "--test", help="test mode",
                        action="store_true")
    parser.add_argument("-v", "--verbose", help="verbose",
                        action="store_true")
    parser.add_argument("hostname")
    args = parser.parse_args()
    
    ip = my_external_ip()
    if args.test:
        print "# Assign %s = %s" % (args.hostname , ip)
    else:
        PS=socket.gethostname()
        UUID=str(uuid.uuid4())
        # CMD="user-list_users_no_pw"
        CMD="dns-add_record"
        
        RECORD=args.hostname
        TYPE='A'
        VALUE=ip
        COMMENT="update_dns.py %s" % datetime.datetime.utcnow().isoformat()

        # ARGS='other_http_get_arguments_for_the_DreamHost_cmd_that_you_are_using=4&foo=123'
        ARGS=""
        LINK="https://api.dreamhost.com/?key={key}&unique_id={uuid}&cmd={cmd}&ps={ps}&record={record}&type={type}&value={value}&comment={comment}"
        LINK = LINK.format(key=KEY, uuid=UUID, cmd=CMD, ps=PS, record=RECORD, type=TYPE, value=VALUE, comment=COMMENT)
        if args.verbose:
            print LINK

        # RESPONSE=`wget -O- -q "$LINK"`

        RESPONSE=requests.get(LINK)

        # echo "$RESPONSE"
        # if ! (echo $RESPONSE | grep -q 'success'); then
        #     exit 1
        # fi

        if args.verbose:
            print RESPONSE
            print RESPONSE.text


# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
