#!/usr/bin/python3
import requests
import socket
import sys
import datetime
if len(sys.argv) < 3:
	print("uporaba: " + sys.argv[0] + " endpoint bindport [bindhost6=any] [seconds/output=0]\nprimer: " + sys.argv[0] + " http://splet.4a.si/pub/1 6969 :: 1", file=sys.stderr)
	exit(1)
rs = requests.Session()
ss = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
ss.bind((sys.argv[3] if len(sys.argv) > 3 else '', int(sys.argv[2])))
last = datetime.datetime(2024, 12, 8, 15, 55, 6, 298861)
while True:
	message, address = ss.recvfrom(65536)
	resp = rs.post(sys.argv[1], data=message)
	if datetime.datetime.now() - last > datetime.timedelta(seconds=int(sys.argv[4]) if len(sys.argv) > 4 else 0):
		last = datetime.datetime.now()
		print(resp.text)
