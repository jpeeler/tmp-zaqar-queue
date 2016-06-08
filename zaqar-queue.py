#!/usr/bin/env python
from __future__ import print_function
import argparse
import os
import sys

from zaqarclient.queues.v2 import client as zq_client
from keystoneclient.v2_0 import client as ks_client

MESSAGE = {
    "body": {
        "data": "some data",
    }
}

def get_client(args):
    keystone = ks_client.Client(auth_url=os.environ.get('OS_AUTH_URL'),
                                username=os.environ.get('OS_USERNAME'),
                                password=os.environ.get('OS_PASSWORD'),
                                tenant_name=os.environ.get('OS_TENANT_NAME'))

    # NOTE: wouldn't have to do this if zaqar didn't require project id
    # (was hoping to just use tenant name and not use keystone client)
    os_opts = {
        'os_auth_token': keystone.auth_ref['token']['id'],
    }
    auth_opts = {'backend': 'keystone',
                 'options': os_opts}
    conf = {'auth_opts': auth_opts}
    zaqar = zq_client.Client(args.server, version=1.1, conf=conf)

    return zaqar

def send_message(client, args):
    queue = client.queue(args.queue)
    res = queue.post(MESSAGE)
    print('Sent message {} {}'.format(res, MESSAGE))

def receive_messages(client, args):
#TODO: would like to use subscription, but 2.0 requirement?
#res = zaqar.subscription(QUEUE)
    queue = client.queue(args.queue)
    try:
        while True:
            for msg in queue.messages(echo=True):
                print('Received message: {} {}'.format(msg, msg.body))
                msg.delete()
            if not args.receive_forever:
                break
    except KeyboardInterrupt as e:
        pass

def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(
        description='Fill in')

    parser.add_argument('--queue', default='myqueue',
                        help='Queue to listen for messages')
    parser.add_argument('--server', default='http://192.0.2.1:8888',
                        help='Zaqar server to listen on')
    #parser.add_argument('--ttl', type=int, default=600,
    #                    help='TTL value for messages')
    parser.add_argument('--receive', action='store_true',
                        help='Listen for messages one time')
    parser.add_argument('--receive-forever', action='store_true',
                        help='Listen for messages forever')
    parser.add_argument('--send', action='store_true',
                        help='Send one message')

    args = parser.parse_args()
    return args

def main():
    args = parse_arguments(sys.argv)

    zaqar = get_client(args)

    if args.send:
	send_message(zaqar, args)
    if args.receive or args.receive_forever:
        receive_messages(zaqar, args)

#claim = queue.claim(ttl=600, grace=600)
#for msg in claim:
#    print('Received message: {} {}'.format(msg, msg.body))

if __name__ == '__main__':
    main()
