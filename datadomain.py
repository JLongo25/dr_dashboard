#!/usr/bin/env python3
import socket
from ssh2.session import Session
import ssh2.exceptions
from datetime import datetime
from es_connect import connect_elasticsearch
from elasticsearch import helpers
import sys
import os
from datadomain_helper import logs, u_logs


if len(sys.argv) >= 2:
    datadomain = sys.argv[1]
else:
    sys.exit("Missing DataDomain Host Information.  Exiting program.")

# datadomain = 'IP1'
# datadomain = 'IP2'

ssh_command = 'replication status detailed'

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# set socket timeout to 5 seconds to prevent blocking and hanging
sock.settimeout(5)

try:
    sock.connect((datadomain, 22))
except socket.timeout as e:
    print(e)


session = Session()
# set session timeout to prevent blocking and hanging
session.set_timeout(5000)
session.handshake(sock)

try:
    session.userauth_password(u_logs(), logs())
except ssh2.exceptions.AuthenticationError as e:
    # print error to log file : TODO
    exit(-1)


channel = session.open_session()
channel.execute(ssh_command)
size, data = channel.read()
# put data to string format
result = ""
while size > 0:
    result = result + data.decode("UTF-8")
    size, data = channel.read()
channel.close()
sock.close()

if channel.get_exit_status() != 0:
    # print error and code to log file :TODO
    print("Exit status: %s" % channel.get_exit_status())

# put results in a list
result_list = result.splitlines()
grab = ['Destination', 'State', 'Error', 'ed-as-of', 'Pre-compressed bytes remaining']

to_elk = {}
data = []
n = 0
today = datetime.today().strftime('%s')
year = datetime.today().strftime('%Y')
collection_date = datetime.now().strftime('%s')
os.environ['TZ'] = 'PST'

for r in range(len(result_list)):
    for g in grab:
        if g in result_list[r]:
            n += 1
            result_dict = (result_list[r].split(sep=':', maxsplit=1))
            to_elk[result_dict[0]] = result_dict[1].strip()
            if n == 5:
                date = to_elk["Sync'ed-as-of time"]
                string_to_datetime = datetime.strptime(year + ' ' + date, '%Y %a %b %d %H:%M')
                os.environ["TZ"] = 'America/Los_Angeles'
                seconds = string_to_datetime.strftime('%s')
                print(seconds)
                to_elk["Sync'ed-as-of time"] = seconds
                to_elk['timestamp'] = today
                to_elk['Pre-compressed bytes remaining'] = to_elk['Pre-compressed bytes remaining'].replace(',', '')
                if to_elk['Error'] == 'no error':
                    to_elk['Error'] = False
                else:
                    to_elk['Error'] = True
                if datadomain == 'IP':
                    to_elk['Source'] = 'cluster2'
                else:
                    to_elk['Source'] = 'cluster1'
                data.append(to_elk)
                to_elk = {}
                n = 0

es = connect_elasticsearch()
resp = helpers.bulk(es, data, index='datadomain')
